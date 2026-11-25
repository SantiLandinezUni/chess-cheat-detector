"""Scorer group test (Part 2, E1): N streaming scorers in one consumer group, one result row.

Used as CONSUMER_CMD of cluster/kafka_job.sh (see cluster/scorer_scaling.sh):
    python src/streaming/scorer_group.py --config config/cluster.yaml --consumers 4 --run-id <id> \
        --csv logs/streaming/scorer_scaling.csv --producer-summary <replay_producer --summary-out file>

Starts N src/streaming/scorer.py processes with the same --group, so they share the partitions of
lichess.moves (each game is scored by one member). Once every member has partitions it touches
$KAFKA_CONSUMER_READY, which lets kafka_job.sh start the producer.

When the group stops (fix for job 32798, where members stopped one by one on their own idle timeout and
the last one re-read the whole topic): members never stop by themselves (no idle timeout). The group stops
them all together, with SIGTERM, once (1) the producer has finished (kafka_job.sh touches
$KAFKA_PRODUCER_DONE) and (2) the group's lag is 0: for every partition, the position the members committed
(scorer.py commits after every batch) equals the end of the partition. Each
member then commits, writes its Parquet part and exits; this process waits for all of them and exits last,
so kafka_job.sh stops the broker only after every scorer has exited.
If this process gets SIGTERM/SIGINT first (kafka_job.sh at the job's time limit), it stops the members the
same way, reports how much was scored, and exits with STOPPED_EXIT (3): the run is incomplete.

Checks on the output (paths.live_scores_dir/run_id=<run id>/, cleared first):
    games_scored    = distinct game_id in the final scores; must equal games_sent (producer summary)
    duplicate_rows  = rows beyond one per (game_id, color); must be 0
    reread_messages = moves consumed by all members - moves in the topic; must be 0 (also with a rebalance:
                      the open games' state moves with the partition, see scorer.py)
    games_joined_late = games a member first saw in the middle (no handoff snapshot); must be 0
    scores_in_topic = messages in lichess.scores; must equal scores_emitted
One CSV row per run: throughput (moves consumed / (last move received - first)), score latency (emit time -
send time of the triggering move: p50/p95/p99/max), ack latency (-> broker ack), and the checks above.
--leave-one-after N (rebalance test): member 0 leaves the group after N messages; its partitions move to
the others, which restore its open games from lichess.scorer_state and continue from its committed offsets.
"""

import json
import logging
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow.dataset as ds
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from src.common.config import build_arg_parser, load_config
from src.common.logging_setup import setup_logging
from src.streaming.common import TOPIC_SCORER_STATE, TOPIC_SCORES, default_bootstrap, default_topic
from src.streaming.consumer_group import SETTLE_S, append_row, wait_ready
from src.streaming.count_consumer import latency_stats

STEP = "scorer_group"
CHECK_EVERY_S = 1.0
REPORT_EVERY_S = 30.0
STOPPED_EXIT = 3
log = logging.getLogger(STEP)

CSV_COLUMNS = ["run_at_utc", "job_id", "node", "cpus", "partitions", "speed", "consumers", "games",
               "messages", "producer_msg_s", "duration_s", "scorer_msg_s", "emit_every",
               "scores_emitted", "scores_in_topic", "final_scores", "games_ended_last_move",
               "games_ended_idle", "games_ended_shutdown",
               "score_latency_p50_ms", "score_latency_p95_ms", "score_latency_p99_ms", "score_latency_max_ms",
               "ack_latency_p50_ms", "ack_latency_p99_ms", "member_msg_min", "member_msg_max", "baselines",
               "status", "games_scored", "duplicate_rows", "messages_in_topic", "reread_messages",
               "lag_at_stop", "left_early", "handed_over_open_games", "games_restored", "games_joined_late",
               "stop_wait_s",
               "run_id", "leave_one_after"]


def start_members(args: Any, group: str, work: Path) -> list[subprocess.Popen]:
    """Start the N scorer processes of the group (no idle or first-message timeout: the group stops them)."""
    procs = []
    for i in range(args.consumers):
        cmd = [sys.executable, "src/streaming/scorer.py", "--config", args.config,
               "--group", group, "--name", f"{args.run_id}-{i}", "--run-id", args.run_id,
               "--emit-every", str(args.emit_every), "--idle-timeout", "0", "--first-timeout", "0",
               "--ready-file", str(work / f"ready.{i}"),
               "--summary-out", str(work / f"summary.{i}.json"),
               "--latency-out", str(work / f"latency.{i}.npz")]
        if args.baselines:
            cmd += ["--baselines", args.baselines]
        if args.leave_one_after and i == 0:
            cmd += ["--leave-after", str(args.leave_one_after)]
        procs.append(subprocess.Popen(cmd))
    return procs


class Offsets:
    """End offsets of the topics and the group's committed positions (a plain client, not a group member)."""

    def __init__(self, bootstrap: str, group: str, topic: str) -> None:
        self.topic = topic
        self.c = Consumer({"bootstrap.servers": bootstrap, "group.id": group, "enable.auto.commit": False})
        self.parts = sorted(self.c.list_topics(topic, timeout=30).topics[topic].partitions)

    def messages(self, topic: str) -> int:
        """Messages in a topic: sum over its partitions of (high - low) watermark."""
        parts = self.c.list_topics(topic, timeout=30).topics[topic].partitions
        return sum(hi - lo for lo, hi in (self.c.get_watermark_offsets(TopicPartition(topic, p), timeout=30)
                                          for p in parts))

    def lag(self) -> tuple[int, int]:
        """(lag, moves in the topic): lag = sum over partitions of end offset - committed position."""
        committed = self.c.committed([TopicPartition(self.topic, p) for p in self.parts], timeout=30)
        lag = total = 0
        for tp in committed:
            lo, hi = self.c.get_watermark_offsets(TopicPartition(self.topic, tp.partition), timeout=30)
            lag += hi - (tp.offset if tp.offset >= 0 else lo)
            total += hi - lo
        return lag, total

    def close(self) -> None:
        self.c.close()


def ensure_state_topic(bootstrap: str, topic: str, state_topic: str) -> None:
    """Create the handoff topic with the input topic's partition count if it is missing
    (kafka_job.sh creates only EXTRA_TOPICS; older job scripts list just lichess.scores)."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    topics = admin.list_topics(timeout=30).topics
    if state_topic in topics:
        return
    n = len(topics[topic].partitions)
    try:
        admin.create_topics([NewTopic(state_topic, num_partitions=n, replication_factor=1)])[state_topic].result(30)
        log.info("created topic %s (%d partitions)", state_topic, n)
    except KafkaException as exc:
        if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise


def wait_end(procs: list[subprocess.Popen], offsets: Offsets | None, producer_done: Path | None,
             stop: dict[str, Any]) -> str:
    """Wait until the stream is over: producer done and lag 0 ("complete"), a stop signal ("stopped"),
    or no member left ("members_exited")."""
    t0 = time.monotonic()
    next_report = t0 + REPORT_EVERY_S
    while True:
        if stop["signal"]:
            return "stopped"
        alive = [p for p in procs if p.poll() is None]
        if not alive:
            return "members_exited"
        done = producer_done is not None and producer_done.exists()
        lag = total = None
        if offsets is not None:
            try:
                lag, total = offsets.lag()
            except KafkaException as exc:
                log.warning("could not read the group's lag: %s", exc)
        if done and lag == 0:
            log.info("producer finished and lag 0 (%d moves in the topic, all processed): stopping the group",
                     total)
            return "complete"
        if time.monotonic() >= next_report:
            log.info("waiting: producer %s, lag %s of %s moves, %d of %d members running",
                     "finished" if done else "running", lag, total, len(alive), len(procs))
            next_report = time.monotonic() + REPORT_EVERY_S
        time.sleep(CHECK_EVERY_S)


def stop_members(procs: list[subprocess.Popen]) -> float:
    """SIGTERM to every running member at once ("final" stop), then wait for all of them (no timeout:
    kafka_job.sh is the one that enforces the job's time limit). Returns the seconds waited."""
    t0 = time.monotonic()
    for p in procs:
        if p.poll() is None:
            p.send_signal(signal.SIGTERM)
    for p in procs:
        p.wait()
    return time.monotonic() - t0


def check_output(out_dir: Path) -> dict[str, int]:
    """Rows, distinct games and duplicate (game_id, color) rows in the run's final scores."""
    if not out_dir.exists() or not any(out_dir.glob("*.parquet")):
        return {"final_rows": 0, "games_scored": 0, "duplicate_rows": 0}
    t = ds.dataset(out_dir, format="parquet").to_table(columns=["game_id", "color"])
    keys = t.group_by(["game_id", "color"]).aggregate([])
    return {"final_rows": t.num_rows, "games_scored": len(t.column("game_id").unique()),
            "duplicate_rows": t.num_rows - keys.num_rows}


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
            "left_early": sum(s.get("stop_mode") == "handoff" for s in summaries),
            "handed_over_open_games": sum(s.get("handed_over_open_games", 0) for s in summaries),
            "games_restored": sum(s.get("games_restored", 0) for s in summaries),
            "games_joined_late": sum(s.get("games_joined_late", 0) for s in summaries),
            "commit_errors": sum(s.get("commit_errors", 0) for s in summaries),
            "baselines": Path(summaries[0]["baselines"]).name if summaries else None}


def main() -> None:
    parser = build_arg_parser("Run N streaming scorers in one consumer group and record the group's results")
    parser.add_argument("--consumers", type=int, required=True, help="number of group members")
    parser.add_argument("--csv", required=True, help="CSV to append the result row to")
    parser.add_argument("--run-id", required=True, help="run id: folder of the final scores (cleared first)")
    parser.add_argument("--producer-summary", help="replay_producer --summary-out file (games, speed, rate)")
    parser.add_argument("--producer-done", default=os.getenv("KAFKA_PRODUCER_DONE"),
                        help="file that appears when the producer has finished (default: $KAFKA_PRODUCER_DONE, "
                             "set by cluster/kafka_job.sh)")
    parser.add_argument("--baselines", help="baselines folder (default: paths.scorer_baselines_dir)")
    parser.add_argument("--emit-every", type=int, default=10, help="members' --emit-every (default 10)")
    parser.add_argument("--leave-one-after", type=int,
                        help="rebalance test: member 0 leaves the group after this many messages")
    parser.add_argument("--idle-timeout", type=float,
                        help="ignored (kept for old job scripts): the group now stops when the producer has "
                             "finished and the lag is 0")
    parser.add_argument("--bootstrap", default=default_bootstrap(), help="Kafka bootstrap servers")
    parser.add_argument("--topic", default=default_topic(), help="input topic (default lichess.moves)")
    args = parser.parse_args()
    if args.consumers <= 0:
        parser.error("--consumers must be > 0")
    if not args.bootstrap or not args.producer_done:
        parser.error("needs a broker and a producer-done file: run inside cluster/kafka_job.sh "
                     "(or pass --bootstrap and --producer-done)")
    if args.leave_one_after and args.consumers < 2:
        parser.error("--leave-one-after needs at least 2 consumers")

    cfg = load_config(args.config)
    setup_logging(STEP, cfg["paths"]["log_dir"])
    if args.idle_timeout is not None:
        log.warning("--idle-timeout %s ignored: the group stops when the producer has finished and the lag is 0",
                    args.idle_timeout)
    stop = {"signal": None}

    def on_signal(signum: int, _frame: Any) -> None:
        log.warning("received signal %d: stopping the group before the end of the stream", signum)
        stop["signal"] = signum

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    out_dir = Path(cfg["paths"]["live_scores_dir"]) / f"run_id={args.run_id}"
    if out_dir.exists():
        shutil.rmtree(out_dir)
        log.info("removed the previous final scores of run %s", args.run_id)
    group = f"scorer-g{args.consumers}-{args.run_id}"
    work = Path(tempfile.mkdtemp(prefix=f"{group}-", dir=os.getenv("KAFKA_WORK_DIR")))
    log.info("group=%s consumers=%d emit_every=%d leave_one_after=%s work=%s", group, args.consumers,
             args.emit_every, args.leave_one_after, work)

    ensure_state_topic(args.bootstrap, args.topic, TOPIC_SCORER_STATE)
    procs = start_members(args, group, work)
    if wait_ready(procs, work):
        time.sleep(SETTLE_S)
        log.info("all %d members have partitions", args.consumers)
    else:
        log.error("not every member got partitions in time")
    ready_file = os.getenv("KAFKA_CONSUMER_READY")
    if ready_file:
        Path(ready_file).touch()  # lets kafka_job.sh start the producer (also after an error, to finish the run)

    offsets = None
    try:
        offsets = Offsets(args.bootstrap, group, args.topic)
    except KafkaException as exc:
        log.error("cannot read offsets from %s: %s", args.bootstrap, exc)
    reason = wait_end(procs, offsets, Path(args.producer_done), stop)
    stop_wait_s = stop_members(procs)
    codes = [p.returncode for p in procs]
    log.info("all members exited after %.1f s (exit codes %s)", stop_wait_s, codes)

    lag = in_topic = scores_in_topic = None
    if offsets is not None:
        try:
            lag, in_topic = offsets.lag()
            scores_in_topic = offsets.messages(TOPIC_SCORES)
        except KafkaException as exc:
            log.error("could not read the final offsets: %s", exc)
        offsets.close()
    result = aggregate(args.consumers, work)
    out = check_output(out_dir)
    producer = json.loads(Path(args.producer_summary).read_text()) if (
        args.producer_summary and Path(args.producer_summary).exists()) else {}
    games_sent = producer.get("games")
    reread = result["messages"] - in_topic if in_topic is not None else None
    status = "complete" if reason == "complete" else ("stopped" if reason == "stopped" else "failed")
    row = {"run_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "job_id": os.getenv("SLURM_JOB_ID"), "node": socket.gethostname().split(".")[0],
           "cpus": os.getenv("SLURM_CPUS_PER_TASK"), "partitions": os.getenv("KAFKA_PARTITIONS"),
           "speed": producer.get("speed"), "consumers": args.consumers, "games": games_sent,
           "producer_msg_s": producer.get("delivered_rate_msg_s"), "emit_every": args.emit_every,
           "scores_in_topic": scores_in_topic, "status": status, "games_scored": out["games_scored"],
           "duplicate_rows": out["duplicate_rows"], "messages_in_topic": in_topic, "reread_messages": reread,
           "lag_at_stop": lag, "stop_wait_s": round(stop_wait_s, 1), "run_id": args.run_id,
           "leave_one_after": args.leave_one_after} | result
    log.info("result: %s", json.dumps(row))
    append_row(Path(args.csv), row, CSV_COLUMNS)
    log.info("row appended to %s", args.csv)

    scored = (f"{out['games_scored']} games scored ({out['final_rows']} final rows in {out_dir}; "
              f"{result['games_ended_shutdown']} of them cut short at the stop, end_reason shutdown)")
    if status != "complete":
        log.error("INCOMPLETE (%s): processed %s of %s moves (lag %s); %s; games sent by the producer: %s",
                  "stopped before the end of the stream, e.g. at the job's time limit" if status == "stopped"
                  else "every member exited before the end of the stream",
                  None if in_topic is None or lag is None else in_topic - lag, in_topic, lag, scored,
                  games_sent if games_sent is not None else "unknown (producer did not finish)")
        sys.exit(STOPPED_EXIT if status == "stopped" else 1)

    problems = []
    if any(codes) or result["members_reported"] != args.consumers:
        problems.append(f"member exit codes {codes}, {result['members_reported']} of {args.consumers} summaries")
    if scores_in_topic != result["scores_emitted"]:
        problems.append(f"scores emitted {result['scores_emitted']} vs in topic {scores_in_topic}")
    if games_sent is None or out["games_scored"] != games_sent:
        problems.append(f"games scored {out['games_scored']} vs sent {games_sent}")
    if out["duplicate_rows"]:
        problems.append(f"{out['duplicate_rows']} duplicate (game_id, color) rows")
    if reread != 0:
        problems.append(f"{reread} moves read twice")
    if result["games_joined_late"] or result["games_restored"] != result["handed_over_open_games"]:
        problems.append(f"open games handed over {result['handed_over_open_games']}, restored "
                        f"{result['games_restored']}, first seen mid-game {result['games_joined_late']}")
    if args.leave_one_after and result["left_early"] != 1:
        problems.append(f"{result['left_early']} members left early, expected 1")
    if problems:
        log.error("run FAILED: %s", "; ".join(problems))
        sys.exit(1)
    log.info("run OK: %s = games sent; 0 duplicates; re-read moves %s; lag 0", scored, reread)


if __name__ == "__main__":
    main()
