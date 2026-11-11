"""Replay producer (Part 2, E1): sends lake moves to Kafka topic lichess.moves with their original timing.

Run inside a Slurm job that has a broker (cluster/kafka_job.sh exports KAFKA_BOOTSTRAP), e.g.
    spark-submit --driver-memory 6g src/streaming/replay_producer.py --config config/cluster.yaml \
        --month 2026-10 --max-games 1000 --speed 100

Game selection: the first --max-games games of the month by started_at (from --start if given),
excluding BOT games, games with 0 moves and, by default, correspondence (moves days apart).
A contiguous window keeps the original gaps both between game starts and between moves.

Pacing: every move is scheduled at
    (started_at - window start + cumulative move_time_s of the game up to this ply) / speed
seconds after the replay starts (null or negative move times count as 0). --speed 0 sends
everything as fast as possible, to measure the maximum throughput.

One message per half-move, key = game_id (so a game's moves stay in order on one partition),
value = JSON with the fields in src/streaming/common.py:MOVE_FIELDS. `timestamp` is the send time
(UTC epoch ms) and is also the Kafka record timestamp, so consumers can measure end-to-end latency.
Only hashed player_id values are sent, never usernames.
"""

import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from confluent_kafka import KafkaException, Producer
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import build_arg_parser, load_config
from src.common.logging_setup import log_duration, setup_logging
from src.common.spark import get_spark
from src.streaming.common import default_bootstrap, default_topic

STEP = "replay_producer"
DEFAULT_TIME_CLASSES = ["ultrabullet", "bullet", "blitz", "rapid", "classical"]
PROGRESS_EVERY_S = 10.0

log = logging.getLogger(STEP)


def select_games(spark: SparkSession, lake_dir: str, month: str, max_games: int,
                 time_classes: list[str], start: datetime | None) -> DataFrame:
    """The first max_games human games with moves of the month, by started_at."""
    games = (spark.read.parquet(f"{lake_dir}/games")
             .where((F.col("source_month") == month) & F.col("time_class").isin(time_classes))
             .where(~F.col("has_bot") & (F.col("n_plies") > 0)))
    if start is not None:
        games = games.where(F.col("started_at") >= F.lit(start))
    return (games.select("game_id", "time_class", "started_at", "white_elo", "black_elo")
            .orderBy("started_at", "game_id")
            .limit(max_games))


def load_moves(spark: SparkSession, lake_dir: str, month: str, games: DataFrame,
               time_classes: list[str]) -> pd.DataFrame:
    """Moves of the selected games with the game's start time and the mover's rating (small: pandas)."""
    moves = (spark.read.parquet(f"{lake_dir}/moves")
             .where((F.col("source_month") == month) & F.col("time_class").isin(time_classes)))
    joined = (moves.join(F.broadcast(games), ["game_id", "time_class"])
              .select("game_id", "ply", "player_id", "color", "time_class",
                      F.when(F.col("color") == "w", F.col("white_elo")).otherwise(F.col("black_elo")).alias("elo"),
                      F.col("clock_after_s").alias("clock"), F.col("move_time_s").alias("move_time"),
                      F.col("eval_cp").alias("eval"), "eval_mate", "started_at"))
    return joined.toPandas()


def cache_file(cache_dir: str, month: str, max_games: int, start: datetime | None,
               time_classes: list[str]) -> Path:
    """Cache file name for one selection (month, number of games, start, time classes)."""
    start_tag = start.strftime("%Y%m%dT%H%M%S") if start else "monthstart"
    return Path(cache_dir) / f"replay_{month}_{max_games}g_{start_tag}_{'-'.join(sorted(time_classes))}.parquet"


def build_schedule(pdf: pd.DataFrame, speed: float) -> pd.DataFrame:
    """Add send_at_s (seconds after the replay start) and sort by it, keeping ply order inside a game."""
    pdf = pdf.sort_values(["game_id", "ply"], kind="stable").reset_index(drop=True)
    step = pdf["move_time"].fillna(0.0).clip(lower=0.0)
    game_start = (pdf["started_at"] - pdf["started_at"].min()).dt.total_seconds()
    original_s = game_start + step.groupby(pdf["game_id"]).cumsum()
    pdf["send_at_s"] = original_s / speed if speed > 0 else 0.0
    return pdf.sort_values(["send_at_s", "game_id", "ply"], kind="stable").reset_index(drop=True)


def _nullable(series: pd.Series, cast: type) -> list[Any]:
    """Column as a Python list with None for nulls (Arrow turns nullable ints into float + NaN)."""
    return [None if pd.isna(v) else cast(v) for v in series.tolist()]


def encode_messages(pdf: pd.DataFrame) -> tuple[list[bytes], list[str]]:
    """Keys and JSON value prefixes without the closing timestamp, built before the send loop
    so that encoding doesn't count against throughput. The send loop appends `"timestamp": <ms>}`."""
    cols = {
        "game_id": pdf["game_id"].tolist(),
        "ply": pdf["ply"].astype(int).tolist(),
        "player_id": pdf["player_id"].tolist(),
        "color": pdf["color"].tolist(),
        "time_class": pdf["time_class"].tolist(),
        "elo": _nullable(pdf["elo"], int),
        "clock": _nullable(pdf["clock"], float),
        "move_time": _nullable(pdf["move_time"], float),
        "eval": _nullable(pdf["eval"], int),
        "eval_mate": _nullable(pdf["eval_mate"], int),
    }
    names = list(cols)
    prefixes = []
    for row in zip(*cols.values()):
        body = json.dumps(dict(zip(names, row)), separators=(",", ":"))
        prefixes.append(body[:-1] + ',"timestamp":')
    keys = [g.encode() for g in cols["game_id"]]
    return keys, prefixes


def make_producer(bootstrap: str, linger_ms: int, compression: str) -> Producer:
    """Idempotent producer (per-partition order kept on retries); murmur2 = Java clients' partitioner."""
    return Producer({
        "bootstrap.servers": bootstrap,
        "client.id": STEP,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": linger_ms,
        "compression.type": compression,
        "partitioner": "murmur2_random",
        "queue.buffering.max.messages": 1_000_000,
    })


def send_all(producer: Producer, topic: str, keys: list[bytes], prefixes: list[str],
             send_at: list[float]) -> dict[str, Any]:
    """Send every message at its scheduled time (all at once if send_at is all 0); return stats."""
    state = {"delivered": 0, "failed": 0}

    def on_delivery(err: Any, _msg: Any) -> None:
        if err is None:
            state["delivered"] += 1
        else:
            state["failed"] += 1
            if state["failed"] <= 5:
                log.error("delivery failed: %s", err)

    n = len(keys)
    max_lag = sum_lag = 0.0
    t0 = time.monotonic()
    next_progress = t0 + PROGRESS_EVERY_S
    for i in range(n):
        target = t0 + send_at[i]
        while (wait := target - time.monotonic()) > 0:
            producer.poll(wait)  # sleeps until the send time, serving delivery callbacks meanwhile
        now = time.monotonic()
        lag = now - target
        sum_lag += lag
        max_lag = max(max_lag, lag)
        ts_ms = time.time_ns() // 1_000_000
        value = f"{prefixes[i]}{ts_ms}}}".encode()
        while True:
            try:
                producer.produce(topic, key=keys[i], value=value, timestamp=ts_ms, on_delivery=on_delivery)
                break
            except BufferError:  # local queue full: let librdkafka send, then retry
                producer.poll(0.05)
        producer.poll(0)
        if now >= next_progress:
            log.info("progress: sent %d / %d (%.0f msg/s so far), behind schedule by %.3f s",
                     i + 1, n, (i + 1) / (now - t0), lag)
            next_progress = now + PROGRESS_EVERY_S
    send_s = time.monotonic() - t0
    remaining = producer.flush(120)
    total_s = time.monotonic() - t0
    return {"messages": n, "delivered": state["delivered"], "failed": state["failed"],
            "undelivered_after_flush": remaining, "send_s": round(send_s, 3), "total_s": round(total_s, 3),
            "send_rate_msg_s": round(n / send_s, 1) if send_s > 0 else None,
            "delivered_rate_msg_s": round(state["delivered"] / total_s, 1) if total_s > 0 else None,
            "schedule_lag_mean_s": round(sum_lag / n, 4) if n else None,
            "schedule_lag_max_s": round(max_lag, 4)}


def main() -> None:
    parser = build_arg_parser("Replay lake moves to Kafka with their original timing")
    parser.add_argument("--month", required=True, help="source month to replay, e.g. 2026-10")
    parser.add_argument("--max-games", type=int, default=1000, help="number of games (default 1000)")
    parser.add_argument("--speed", type=float, default=1.0,
                        help="speed-up factor for the original timing; 0 = no pacing (default 1)")
    parser.add_argument("--start", type=datetime.fromisoformat,
                        help="UTC start of the game window, e.g. 2026-10-15T18:00 (default: start of the month)")
    parser.add_argument("--time-classes", type=lambda s: [t.strip() for t in s.split(",") if t.strip()],
                        default=DEFAULT_TIME_CLASSES,
                        help="comma-separated time classes (default: all except correspondence)")
    parser.add_argument("--bootstrap", default=default_bootstrap(),
                        help="Kafka bootstrap servers (default: $KAFKA_BOOTSTRAP)")
    parser.add_argument("--topic", default=default_topic(), help="topic (default: $KAFKA_TOPIC or lichess.moves)")
    parser.add_argument("--cache-dir",
                        help="keep the selected moves here (Parquet, hashed ids only) and reuse them in later "
                             "runs with the same month, --max-games, --start and --time-classes, skipping Spark")
    parser.add_argument("--summary-out", help="write the send summary to this JSON file")
    parser.add_argument("--linger-ms", type=int, default=5, help="producer linger.ms (default 5)")
    parser.add_argument("--compression", default="lz4", help="producer compression.type (default lz4)")
    args = parser.parse_args()
    if not args.bootstrap:
        parser.error("no broker: pass --bootstrap or run inside cluster/kafka_job.sh (sets KAFKA_BOOTSTRAP)")
    if args.speed < 0 or args.max_games <= 0:
        parser.error("--speed must be >= 0 and --max-games > 0")

    cfg = load_config(args.config)
    setup_logging(STEP, cfg["paths"]["log_dir"])
    log.info("month=%s max_games=%d speed=%s start=%s time_classes=%s topic=%s bootstrap=%s",
             args.month, args.max_games, args.speed or "no pacing", args.start, args.time_classes,
             args.topic, args.bootstrap)

    # Created first, so it gets its idempotent producer id from the broker while Spark loads the
    # moves; otherwise the first messages wait for it and their latency is inflated
    producer = make_producer(args.bootstrap, args.linger_ms, args.compression)
    try:
        producer.list_topics(args.topic, timeout=30)
    except KafkaException as exc:
        log.error("cannot reach the broker at %s: %s", args.bootstrap, exc)
        sys.exit(1)

    cached = (cache_file(args.cache_dir, args.month, args.max_games, args.start, args.time_classes)
              if args.cache_dir else None)
    if cached is not None and cached.exists():
        with log_duration(log, f"read cached moves {cached}"):
            pdf = pd.read_parquet(cached)
    else:
        with log_duration(log, "load moves from the lake"):
            spark = get_spark(cfg, STEP)
            lake_dir = cfg["paths"]["lake_dir"]
            games = select_games(spark, lake_dir, args.month, args.max_games, args.time_classes, args.start)
            pdf = load_moves(spark, lake_dir, args.month, games, args.time_classes)
            spark.stop()
        if cached is not None and not pdf.empty:
            cached.parent.mkdir(parents=True, exist_ok=True)
            pdf.to_parquet(cached, index=False)
            log.info("cached the selected moves in %s", cached)
    if pdf.empty:
        log.error("no moves selected for month %s", args.month)
        sys.exit(1)
    n_games = pdf["game_id"].nunique()
    log.info("selected %d games, %d moves; games start between %s and %s UTC; by time class: %s",
             n_games, len(pdf), pdf["started_at"].min(), pdf["started_at"].max(),
             pdf.groupby("time_class")["game_id"].nunique().to_dict())

    with log_duration(log, "build schedule and encode messages"):
        pdf = build_schedule(pdf, args.speed)
        send_at = pdf["send_at_s"].tolist()
        keys, prefixes = encode_messages(pdf)
        del pdf
    log.info("replay length %.1f s at speed %s; mean value size %.0f bytes",
             send_at[-1], args.speed or "no pacing", sum(map(len, prefixes)) / len(prefixes) + 14)  # 13-digit ms + "}"

    with log_duration(log, f"send {len(keys)} messages"):
        stats = send_all(producer, args.topic, keys, prefixes, send_at)
    stats.update(games=n_games, speed=args.speed, month=args.month)
    log.info("summary: %s", json.dumps(stats))
    if args.summary_out:
        Path(args.summary_out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.summary_out).write_text(json.dumps(stats, indent=2), encoding="utf-8")
    if stats["failed"] or stats["undelivered_after_flush"]:
        log.error("%d messages failed, %d undelivered", stats["failed"], stats["undelivered_after_flush"])
        sys.exit(1)


if __name__ == "__main__":
    main()
