"""Consumer group test (Part 2, E1): N count consumers in one consumer group, one result row.

Used as CONSUMER_CMD of cluster/kafka_job.sh (see cluster/consumer_scaling.sh):
    python src/streaming/consumer_group.py --config config/cluster.yaml --consumers 4 \
        --csv logs/streaming/consumer_scaling.csv --producer-summary <replay_producer --summary-out file>

Starts N src/streaming/count_consumer.py processes with the same --group, so they share the topic's
partitions. Once every member has partitions (no rebalance left) it touches $KAFKA_CONSUMER_READY,
which lets kafka_job.sh start the producer. When all members have stopped (idle timeout), it
appends one row to the CSV:
    group throughput = all messages / (last message received by any member - first one)
    latency percentiles over every message of every member (end-to-end: producer send -> receive)
plus the producer's own send rate from its summary file, if given.
"""

import csv
import json
import logging
import os
import socket
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from src.common.config import build_arg_parser, load_config
from src.common.logging_setup import setup_logging
from src.streaming.count_consumer import latency_stats

STEP = "consumer_group"
READY_TIMEOUT_S = 120
SETTLE_S = 2.0  # after the last member got its partitions

log = logging.getLogger(STEP)

CSV_COLUMNS = ["run_at_utc", "job_id", "node", "cpus", "partitions", "consumers", "decode_values",
               "messages", "games", "producer_msg_s", "duration_s", "consumer_msg_s",
               "latency_p50_ms", "latency_p95_ms", "latency_p99_ms", "latency_max_ms",
               "member_msg_min", "member_msg_max"]


def start_members(args: Any, group: str, work: Path) -> list[subprocess.Popen]:
    """Start the N count_consumer processes of the group."""
    procs = []
    for i in range(args.consumers):
        cmd = [sys.executable, "src/streaming/count_consumer.py", "--config", args.config,
               "--group", group, "--name", f"{group}-{i}",
               "--interval", str(args.interval), "--idle-timeout", str(args.idle_timeout),
               "--ready-file", str(work / f"ready.{i}"),
               "--summary-out", str(work / f"summary.{i}.json"),
               "--latency-out", str(work / f"latency.{i}.npy")]
        if args.check_values:
            cmd.append("--check-values")
        procs.append(subprocess.Popen(cmd))
    return procs


def wait_ready(procs: list[subprocess.Popen], work: Path) -> bool:
    """True once every member has touched its ready file; False on timeout or if one exited."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < READY_TIMEOUT_S:
        if all((work / f"ready.{i}").exists() for i in range(len(procs))):
            return True
        if any(p.poll() is not None for p in procs):
            return False
        time.sleep(0.2)
    return False


def aggregate(n: int, work: Path) -> dict[str, Any]:
    """Combine the members' summaries and latencies into one group result."""
    summaries = [json.loads((work / f"summary.{i}.json").read_text()) for i in range(n)
                 if (work / f"summary.{i}.json").exists()]
    lat = np.concatenate([np.load(work / f"latency.{i}.npy") for i in range(n)
                          if (work / f"latency.{i}.npy").exists()] or [np.empty(0, dtype=np.int64)])
    firsts = [s["first_msg_ms"] for s in summaries if s["first_msg_ms"] is not None]
    lasts = [s["last_msg_ms"] for s in summaries if s["last_msg_ms"] is not None]
    total = sum(s["messages"] for s in summaries)
    duration_s = (max(lasts) - min(firsts)) / 1000 if firsts and lasts and max(lasts) > min(firsts) else None
    stats = latency_stats(lat)
    per_member = [s["messages"] for s in summaries]
    return {"messages": total, "duration_s": round(duration_s, 3) if duration_s else None,
            "consumer_msg_s": round(total / duration_s, 1) if duration_s else None,
            "latency_p50_ms": stats["p50_ms"], "latency_p95_ms": stats["p95_ms"],
            "latency_p99_ms": stats["p99_ms"], "latency_max_ms": stats["max_ms"],
            "member_msg_min": min(per_member, default=None), "member_msg_max": max(per_member, default=None),
            "members_reported": len(summaries)}


def append_row(path: Path, row: dict[str, Any], columns: list[str] = CSV_COLUMNS) -> None:
    """Append one row to the CSV, writing the header if the file is new."""
    path.parent.mkdir(parents=True, exist_ok=True)
    new = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        if new:
            writer.writeheader()
        writer.writerow(row)


def main() -> None:
    parser = build_arg_parser("Run N count consumers in one consumer group and record the group's results")
    parser.add_argument("--consumers", type=int, required=True, help="number of group members")
    parser.add_argument("--check-values", action="store_true", help="members decode and check every value")
    parser.add_argument("--csv", required=True, help="CSV to append the result row to")
    parser.add_argument("--producer-summary", help="replay_producer --summary-out file, for the producer's rate")
    parser.add_argument("--interval", type=float, default=5.0, help="members' report interval (default 5)")
    parser.add_argument("--idle-timeout", type=float, default=20.0, help="members' idle timeout (default 20)")
    args = parser.parse_args()
    if args.consumers <= 0:
        parser.error("--consumers must be > 0")

    cfg = load_config(args.config)
    setup_logging(STEP, cfg["paths"]["log_dir"])
    group = f"count-g{args.consumers}-{os.getenv('SLURM_JOB_ID', 'nojob')}-{time.time_ns() % 10**6}"
    work = Path(tempfile.mkdtemp(prefix=f"{group}-", dir=os.getenv("KAFKA_WORK_DIR")))
    log.info("group=%s consumers=%d check_values=%s work=%s", group, args.consumers, args.check_values, work)

    procs = start_members(args, group, work)
    if wait_ready(procs, work):
        time.sleep(SETTLE_S)
        log.info("all %d members have partitions", args.consumers)
    else:
        log.error("not every member got partitions within %d s", READY_TIMEOUT_S)
    ready_file = os.getenv("KAFKA_CONSUMER_READY")
    if ready_file:
        Path(ready_file).touch()  # lets kafka_job.sh start the producer (also after an error, to finish the run)
    codes = [p.wait() for p in procs]

    result = aggregate(args.consumers, work)
    producer = {}
    if args.producer_summary and Path(args.producer_summary).exists():
        producer = json.loads(Path(args.producer_summary).read_text())
    row = {"run_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "job_id": os.getenv("SLURM_JOB_ID"), "node": socket.gethostname().split(".")[0],
           "cpus": os.getenv("SLURM_CPUS_PER_TASK"), "partitions": os.getenv("KAFKA_PARTITIONS"),
           "consumers": args.consumers, "decode_values": args.check_values,
           "games": producer.get("games"), "producer_msg_s": producer.get("delivered_rate_msg_s")} | result
    log.info("result: %s", json.dumps(row))
    append_row(Path(args.csv), row)
    log.info("row appended to %s", args.csv)

    if any(codes) or result["members_reported"] != args.consumers:
        log.error("member exit codes %s, %d of %d summaries", codes, result["members_reported"], args.consumers)
        sys.exit(1)


if __name__ == "__main__":
    main()
