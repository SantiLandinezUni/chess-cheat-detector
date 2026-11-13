"""Count consumer (Part 2, E1): reads lichess.moves and logs throughput and end-to-end latency.

Run inside a Slurm job that has a broker (cluster/kafka_job.sh exports KAFKA_BOOTSTRAP), e.g.
    python src/streaming/count_consumer.py --config config/cluster.yaml --summary-out logs/consumer.json

Reads every partition of the topic from the beginning (assigned directly, no consumer group
rebalance). Every --interval seconds it logs messages per second and the latency percentiles of
that window; at the end, totals for the whole run. Latency = time the consumer received the
message - its Kafka record timestamp, which the producer sets to its send time (same node, same
clock). It stops after --idle-timeout seconds without messages (once some have arrived), or
after --max-messages.
"""

import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaException, TopicPartition

from src.common.config import build_arg_parser, load_config
from src.common.logging_setup import setup_logging
from src.streaming.common import MOVE_FIELDS, default_bootstrap, default_topic

STEP = "count_consumer"
BATCH = 10_000
PERCENTILES = (50, 95, 99)

log = logging.getLogger(STEP)


def latency_stats(lat_ms: np.ndarray) -> dict[str, float | None]:
    """p50 / p95 / p99 / max latency in ms (None if empty)."""
    if lat_ms.size == 0:
        return {f"p{p}_ms": None for p in PERCENTILES} | {"max_ms": None}
    pct = np.percentile(lat_ms, PERCENTILES)
    return {f"p{p}_ms": round(float(v), 1) for p, v in zip(PERCENTILES, pct)} | {"max_ms": float(lat_ms.max())}


def make_consumer(bootstrap: str, topic: str, group: str | None, ready_file: str | None,
                  client_id: str = STEP) -> Consumer:
    """Without a group: assigned to every partition of the topic, from the first offset.
    With a group: subscribed as one member of it, so the group's members share the partitions.
    The ready file is touched once the consumer has partitions (for a group member: its first
    assignment, which comes after the rebalance that its joining caused)."""
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        # without --group the id is unique and unused for offsets (assign() + no commits)
        "group.id": group or f"{STEP}-{time.time_ns()}",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "client.id": client_id,
    })
    meta = consumer.list_topics(topic, timeout=30)
    if topic not in meta.topics or meta.topics[topic].error is not None:
        raise KafkaException(f"topic {topic} not found")

    def mark_ready(n_partitions: int) -> None:
        if n_partitions and ready_file:
            Path(ready_file).touch()

    if group:
        def on_assign(_consumer: Consumer, parts: list[TopicPartition]) -> None:
            log.info("group %s: assigned partitions %s", group, sorted(p.partition for p in parts))
            mark_ready(len(parts))

        consumer.subscribe([topic], on_assign=on_assign)
        log.info("subscribed to %s as a member of group %s", topic, group)
    else:
        partitions = sorted(meta.topics[topic].partitions)
        consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in partitions])
        log.info("assigned %d partitions of %s", len(partitions), topic)
        mark_ready(len(partitions))
    return consumer


def check_value(value: bytes) -> bool:
    """The value is JSON with exactly the lichess.moves fields."""
    try:
        return tuple(json.loads(value)) == MOVE_FIELDS
    except (ValueError, TypeError):
        return False


def consume(consumer: Consumer, interval: float, idle_timeout: float, first_timeout: float,
            max_messages: int | None, check_values: bool) -> dict[str, Any]:
    """Poll until idle; log one line per interval; return the run summary."""
    all_lat: list[np.ndarray] = []
    windows: list[dict[str, Any]] = []
    win_lat: list[int] = []
    total = bad_values = errors = 0
    t_start = time.monotonic()
    t_first = t_last = None
    first_ms = last_ms = None  # wall clock (epoch ms), to combine the members of a group
    win_start = t_start

    while True:
        # poll() returns as soon as one message is there; consume(timeout=0) then takes what else has
        # arrived. (consume() with a timeout waits until the batch is full or the timeout ends,
        # which would add up to that timeout to the measured latency.)
        first = consumer.poll(0.2)
        msgs = [first] + consumer.consume(num_messages=BATCH, timeout=0) if first is not None else []
        now_ms = time.time_ns() // 1_000_000
        now = time.monotonic()
        for m in msgs:
            if m.error():
                errors += 1
                log.error("consumer error: %s", m.error())
                continue
            total += 1
            win_lat.append(now_ms - m.timestamp()[1])
            if check_values and not check_value(m.value()):
                bad_values += 1
        if msgs:
            t_first = t_first or now
            first_ms = first_ms or now_ms
            t_last, last_ms = now, now_ms

        if now - win_start >= interval:
            lat = np.asarray(win_lat, dtype=np.int64)
            row = {"t_s": round(now - t_start, 1), "messages": int(lat.size),
                   "msg_s": round(lat.size / (now - win_start), 1)} | latency_stats(lat)
            if t_first is not None:
                windows.append(row)
            if lat.size:
                log.info("t=%6.1fs  %8.0f msg/s  latency ms p50=%s p95=%s p99=%s max=%s  (total %d)",
                         row["t_s"], row["msg_s"], row["p50_ms"], row["p95_ms"], row["p99_ms"], row["max_ms"], total)
            all_lat.append(lat)
            win_lat = []
            win_start = now

        if max_messages and total >= max_messages:
            log.info("reached --max-messages %d", max_messages)
            break
        if t_last is not None and now - t_last >= idle_timeout:
            log.info("no messages for %.0f s: stopping", idle_timeout)
            break
        if t_first is None and now - t_start >= first_timeout:
            log.error("no message within %.0f s", first_timeout)
            break

    all_lat.append(np.asarray(win_lat, dtype=np.int64))
    lat = np.concatenate(all_lat)
    active_s = (t_last - t_first) if t_first is not None and t_last > t_first else None
    return {"messages": total, "errors": errors, "bad_values": bad_values if check_values else None,
            "active_s": round(active_s, 3) if active_s else None,
            "msg_s": round(total / active_s, 1) if active_s else None,
            "first_msg_ms": first_ms, "last_msg_ms": last_ms,
            "latency": latency_stats(lat), "windows": windows, "latencies_ms": lat}


def main() -> None:
    parser = build_arg_parser("Consume lichess.moves and report throughput and end-to-end latency")
    parser.add_argument("--bootstrap", default=default_bootstrap(),
                        help="Kafka bootstrap servers (default: $KAFKA_BOOTSTRAP)")
    parser.add_argument("--topic", default=default_topic(), help="topic (default: $KAFKA_TOPIC or lichess.moves)")
    parser.add_argument("--interval", type=float, default=5.0, help="seconds between report lines (default 5)")
    parser.add_argument("--idle-timeout", type=float, default=30.0,
                        help="stop after this many seconds without messages (default 30)")
    parser.add_argument("--first-timeout", type=float, default=900.0,
                        help="give up if no message arrives within this many seconds (default 900)")
    parser.add_argument("--max-messages", type=int, help="stop after this many messages")
    parser.add_argument("--check-values", action="store_true",
                        help="decode every value and check it has exactly the lichess.moves fields")
    parser.add_argument("--summary-out", help="write the run summary (with per-interval rows) to this JSON file")
    parser.add_argument("--group",
                        help="join this consumer group (partitions shared with its other members) "
                             "instead of reading every partition")
    parser.add_argument("--name", help="suffix for the log file name, e.g. the member number in a group")
    parser.add_argument("--latency-out", help="save every message's latency in ms to this .npy file")
    parser.add_argument("--ready-file",
                        help="touch this file once the partitions are assigned "
                             "(default: $KAFKA_CONSUMER_READY, which cluster/kafka_job.sh sets and waits for)")
    args = parser.parse_args()
    if not args.bootstrap:
        parser.error("no broker: pass --bootstrap or run inside cluster/kafka_job.sh (sets KAFKA_BOOTSTRAP)")

    cfg = load_config(args.config)
    setup_logging(f"{STEP}-{args.name}" if args.name else STEP, cfg["paths"]["log_dir"])
    log.info("topic=%s bootstrap=%s group=%s interval=%ss idle_timeout=%ss check_values=%s",
             args.topic, args.bootstrap, args.group, args.interval, args.idle_timeout, args.check_values)

    ready_file = args.ready_file or os.getenv("KAFKA_CONSUMER_READY")
    consumer = make_consumer(args.bootstrap, args.topic, args.group, ready_file)
    try:
        summary = consume(consumer, args.interval, args.idle_timeout, args.first_timeout,
                          args.max_messages, args.check_values)
    finally:
        consumer.close()

    windows = summary.pop("windows")
    latencies = summary.pop("latencies_ms")
    if args.latency_out:
        np.save(args.latency_out, latencies)
    log.info("summary: %s", json.dumps(summary))
    if args.summary_out:
        Path(args.summary_out).parent.mkdir(parents=True, exist_ok=True)
        with open(args.summary_out, "w", encoding="utf-8") as f:
            json.dump(summary | {"windows": windows}, f, indent=2)
        log.info("summary saved to %s", args.summary_out)
    if summary["messages"] == 0 or summary["errors"] or (summary["bad_values"] or 0) > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
