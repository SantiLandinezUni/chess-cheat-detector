"""Scorer group test (Part 2, E1): N streaming scorers in one consumer group, one result row.

Used as CONSUMER_CMD of cluster/kafka_job.sh (see cluster/scorer_scaling.sh):
    python src/streaming/scorer_group.py --config config/cluster.yaml --consumers 4 --run-id <id> \
        --csv logs/streaming/scorer_scaling.csv --producer-summary <replay_producer --summary-out file>

Starts N src/streaming/scorer.py processes with the same --group, so they share the partitions of
lichess.moves (each game is scored by one member). Once every member has partitions it touches
$KAFKA_CONSUMER_READY, which lets kafka_job.sh start the producer. When all members have stopped
(idle timeout), it appends one row to the CSV:
    throughput    = moves consumed by all members / (last move received by any member - first one)
    score latency = emit time of a score - send time of the move that triggered it, over every
                    periodic and end-of-game score of every member (p50/p95/p99/max)
    ack latency   = the same up to the broker's acknowledgement of the score message
    scores_in_topic = messages in lichess.scores at the end (must equal scores_emitted)
The run's final scores go to paths.live_scores_dir/run_id=<run id>/ (cleared first, so a rerun with
the same id replaces them).
"""

import json
import logging
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
from confluent_kafka import Consumer, TopicPartition

from src.common.config import build_arg_parser, load_config
from src.common.logging_setup import setup_logging
from src.streaming.common import TOPIC_SCORES, default_bootstrap
from src.streaming.consumer_group import SETTLE_S, append_row, wait_ready
from src.streaming.count_consumer import latency_stats

STEP = "scorer_group"
log = logging.getLogger(STEP)

CSV_COLUMNS = ["run_at_utc", "job_id", "node", "cpus", "partitions", "speed", "consumers", "games",
               "messages", "producer_msg_s", "duration_s", "scorer_msg_s", "emit_every",
               "scores_emitted", "scores_in_topic", "final_scores", "games_ended_last_move",
               "games_ended_idle", "games_ended_shutdown",
               "score_latency_p50_ms", "score_latency_p95_ms", "score_latency_p99_ms", "score_latency_max_ms",
               "ack_latency_p50_ms", "ack_latency_p99_ms", "member_msg_min", "member_msg_max", "baselines"]


def start_members(args: Any, group: str, work: Path) -> list[subprocess.Popen]:
    """Start the N scorer processes of the group."""
    procs = []
    for i in range(args.consumers):
        cmd = [sys.executable, "src/streaming/scorer.py", "--config", args.config,
               "--group", group, "--name", f"{args.run_id}-{i}", "--run-id", args.run_id,
               "--emit-every", str(args.emit_every), "--idle-timeout", str(args.idle_timeout),
               "--ready-file", str(work / f"ready.{i}"),
               "--summary-out", str(work / f"summary.{i}.json"),
               "--latency-out", str(work / f"latency.{i}.npz")]
        if args.baselines:
            cmd += ["--baselines", args.baselines]
        procs.append(subprocess.Popen(cmd))
    return procs


def topic_messages(bootstrap: str, topic: str) -> int | None:
    """Messages in a topic: sum over its partitions of (high - low) watermark."""
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"{STEP}-offsets-{time.time_ns()}"})
    try:
        parts = c.list_topics(topic, timeout=30).topics[topic].partitions
        return sum(hi - lo for lo, hi in (c.get_watermark_offsets(TopicPartition(topic, p), timeout=30)
                                          for p in parts))
    except Exception as exc:  # reporting only: never fail the run for this
        log.error("could not read the offsets of %s: %s", topic, exc)
        return None
    finally:
        c.close()


def aggregate(n: int, work: Path) -> dict[str, Any]:
    """Combine the members' summaries and latencies into one group result."""
    summaries = [json.loads((work / f"summary.{i}.json").read_text()) for i in range(n)
                 if (work / f"summary.{i}.json").exists()]
    lat = [np.load(work / f"latency.{i}.npz") for i in range(n) if (work / f"latency.{i}.npz").exists()]
    score = latency_stats(np.concatenate([z["score"] for z in lat] or [np.empty(0, dtype=np.int64)]))
    ack = latency_stats(np.concatenate([z["ack"] for z in lat] or [np.empty(0, dtype=np.int64)]))
    firsts = [s["first_msg_ms"] for s in summaries if s["first_msg_ms"] is not None]
    lasts = [s["last_msg_ms"] for s in summaries if s["last_msg_ms"] is not None]
    total = sum(s["messages"] for s in summaries)
    duration_s = (max(lasts) - min(firsts)) / 1000 if firsts and lasts and max(lasts) > min(firsts) else None
    per_member = [s["messages"] for s in summaries]
    ended = {r: sum(s["final_games"][r] for s in summaries) for r in ("last_move", "idle", "shutdown")}
    return {"messages": total, "duration_s": round(duration_s, 3) if duration_s else None,
            "scorer_msg_s": round(total / duration_s, 1) if duration_s else None,
            "scores_emitted": sum(s["scores_emitted"] for s in summaries),
            "final_scores": sum(s["final_scores"] for s in summaries),
            "games_ended_last_move": ended["last_move"], "games_ended_idle": ended["idle"],
            "games_ended_shutdown": ended["shutdown"],
            "score_latency_p50_ms": score["p50_ms"], "score_latency_p95_ms": score["p95_ms"],
            "score_latency_p99_ms": score["p99_ms"], "score_latency_max_ms": score["max_ms"],
            "ack_latency_p50_ms": ack["p50_ms"], "ack_latency_p99_ms": ack["p99_ms"],
            "member_msg_min": min(per_member, default=None), "member_msg_max": max(per_member, default=None),
            "members_reported": len(summaries),
            "baselines": Path(summaries[0]["baselines"]).name if summaries else None}


def main() -> None:
    parser = build_arg_parser("Run N streaming scorers in one consumer group and record the group's results")
    parser.add_argument("--consumers", type=int, required=True, help="number of group members")
    parser.add_argument("--csv", required=True, help="CSV to append the result row to")
    parser.add_argument("--run-id", required=True, help="run id: folder of the final scores (cleared first)")
    parser.add_argument("--producer-summary", help="replay_producer --summary-out file (games, speed, rate)")
    parser.add_argument("--baselines", help="baselines folder (default: paths.scorer_baselines_dir)")
    parser.add_argument("--emit-every", type=int, default=10, help="members' --emit-every (default 10)")
    parser.add_argument("--idle-timeout", type=float, default=30.0, help="members' idle timeout (default 30)")
    parser.add_argument("--bootstrap", default=default_bootstrap(), help="for counting lichess.scores at the end")
    args = parser.parse_args()
    if args.consumers <= 0:
        parser.error("--consumers must be > 0")

    cfg = load_config(args.config)
    setup_logging(STEP, cfg["paths"]["log_dir"])
    out_dir = Path(cfg["paths"]["live_scores_dir"]) / f"run_id={args.run_id}"
    if out_dir.exists():
        shutil.rmtree(out_dir)
        log.info("removed the previous final scores of run %s", args.run_id)
    group = f"scorer-g{args.consumers}-{args.run_id}"
    work = Path(tempfile.mkdtemp(prefix=f"{group}-", dir=os.getenv("KAFKA_WORK_DIR")))
    log.info("group=%s consumers=%d emit_every=%d work=%s", group, args.consumers, args.emit_every, work)

    procs = start_members(args, group, work)
    if wait_ready(procs, work):
        time.sleep(SETTLE_S)
        log.info("all %d members have partitions", args.consumers)
    else:
        log.error("not every member got partitions in time")
    ready_file = os.getenv("KAFKA_CONSUMER_READY")
    if ready_file:
        Path(ready_file).touch()  # lets kafka_job.sh start the producer (also after an error, to finish the run)
    codes = [p.wait() for p in procs]

    result = aggregate(args.consumers, work)
    producer = json.loads(Path(args.producer_summary).read_text()) if (
        args.producer_summary and Path(args.producer_summary).exists()) else {}
    row = {"run_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "job_id": os.getenv("SLURM_JOB_ID"), "node": socket.gethostname().split(".")[0],
           "cpus": os.getenv("SLURM_CPUS_PER_TASK"), "partitions": os.getenv("KAFKA_PARTITIONS"),
           "speed": producer.get("speed"), "consumers": args.consumers, "games": producer.get("games"),
           "producer_msg_s": producer.get("delivered_rate_msg_s"), "emit_every": args.emit_every,
           "scores_in_topic": topic_messages(args.bootstrap, TOPIC_SCORES) if args.bootstrap else None} | result
    log.info("result: %s", json.dumps(row))
    append_row(Path(args.csv), row, CSV_COLUMNS)
    log.info("row appended to %s", args.csv)

    ok = (not any(codes) and result["members_reported"] == args.consumers
          and (row["scores_in_topic"] is None or row["scores_in_topic"] == result["scores_emitted"]))
    if not ok:
        log.error("member exit codes %s, %d of %d summaries, scores emitted %s vs in topic %s", codes,
                  result["members_reported"], args.consumers, result["scores_emitted"], row["scores_in_topic"])
        sys.exit(1)


if __name__ == "__main__":
    main()
