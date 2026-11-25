"""Rebalance test check (Part 2, E1): a scorer run with one member stopped early must give the same final
scores as the same replay without a rebalance (fix for job 32798).

Run inside a Slurm job after cluster/scorer_rebalance.sh's two runs of one case (reference and forced
rebalance), from the repo root:
    python src/streaming/check_rebalance.py --config config/cluster.yaml --case nopacing-c4 \
        --ref-run <job>-ref-nopacing-c4 --test-run <job>-leave-nopacing-c4 \
        --group-csv logs/streaming/scorer_rebalance-<job>.csv --out logs/streaming/scorer_rebalance-<job>-check.csv

Compares the two runs' final scores (L13) on the key (game_id, color): same keys, and every column equal
except scored_at (floats exactly, nulls equal). Adds the group checks of the forced run from the group CSV
(scorer_group.py): games scored vs sent, duplicate rows, moves read twice (must be 0), open games handed
over and restored. One CSV row per case;
only counts, no ids. Exit 1 unless the forced run matches the reference exactly and passed its checks.
"""

import csv
import logging
import sys
from pathlib import Path

import pandas as pd

from src.common.config import build_arg_parser, load_config
from src.common.logging_setup import setup_logging
from src.streaming.consumer_group import append_row

STEP = "check_rebalance"
KEY = ["game_id", "color"]
IGNORED = ["scored_at"]
log = logging.getLogger(STEP)

COLUMNS = ["case", "job_id", "speed", "consumers", "leave_one_after", "games_sent", "games_scored_ref",
           "games_scored_test", "rows_ref", "rows_test", "duplicate_rows_test", "rows_only_ref",
           "rows_only_test", "rows_different", "identical", "messages_in_topic", "messages_consumed_test",
           "reread_messages", "handed_over_open_games", "games_restored", "games_joined_late",
           "status_ref", "status_test", "passed"]


def read_run(live_dir: str, run_id: str) -> pd.DataFrame:
    path = Path(live_dir) / f"run_id={run_id}"
    return pd.read_parquet(path).drop(columns=IGNORED + ["run_id"], errors="ignore")


def group_row(csv_path: str, run_id: str) -> dict[str, str]:
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = [r for r in csv.DictReader(f) if r.get("run_id") == run_id]
    if not rows:
        raise SystemExit(f"no row for run {run_id} in {csv_path}")
    return rows[-1]


def compare(ref: pd.DataFrame, test: pd.DataFrame) -> dict[str, int]:
    """Rows only in one run, and rows with a different value in any column."""
    m = ref.merge(test, on=KEY, how="outer", suffixes=("_ref", "_test"), indicator=True)
    both = m[m["_merge"] == "both"]
    diff = pd.Series(False, index=both.index)
    for c in (c for c in ref.columns if c not in KEY):
        a, b = both[f"{c}_ref"], both[f"{c}_test"]
        diff |= ~((a == b) | (a.isna() & b.isna()))
    return {"rows_only_ref": int((m["_merge"] == "left_only").sum()),
            "rows_only_test": int((m["_merge"] == "right_only").sum()), "rows_different": int(diff.sum())}


def main() -> None:
    parser = build_arg_parser("Compare a forced-rebalance scorer run with its reference run")
    parser.add_argument("--case", required=True, help="case name for the CSV, e.g. nopacing-c4")
    parser.add_argument("--ref-run", required=True, help="run id of the reference run (no rebalance)")
    parser.add_argument("--test-run", required=True, help="run id of the run with one member stopped early")
    parser.add_argument("--group-csv", required=True, help="scorer_group.py CSV with both runs' rows")
    parser.add_argument("--out", required=True, help="CSV to append the result row to")
    args = parser.parse_args()
    cfg = load_config(args.config)
    setup_logging(STEP, cfg["paths"]["log_dir"])

    live = cfg["paths"]["live_scores_dir"]
    ref, test = read_run(live, args.ref_run), read_run(live, args.test_run)
    g_ref, g_test = group_row(args.group_csv, args.ref_run), group_row(args.group_csv, args.test_run)
    cmp = compare(ref, test)
    identical = cmp == {"rows_only_ref": 0, "rows_only_test": 0, "rows_different": 0} and len(ref) == len(test)
    in_topic = int(g_test["messages_in_topic"])
    reread = int(g_test["reread_messages"])
    games_sent = int(g_test["games"])
    row = {"case": args.case, "job_id": g_test["job_id"], "speed": g_test["speed"],
           "consumers": g_test["consumers"], "leave_one_after": g_test["leave_one_after"],
           "games_sent": games_sent, "games_scored_ref": ref["game_id"].nunique(),
           "games_scored_test": test["game_id"].nunique(), "rows_ref": len(ref), "rows_test": len(test),
           "duplicate_rows_test": int(test.duplicated(KEY).sum()), **cmp, "identical": identical,
           "messages_in_topic": in_topic, "messages_consumed_test": int(g_test["messages"]),
           "reread_messages": reread, "handed_over_open_games": int(g_test["handed_over_open_games"]),
           "games_restored": int(g_test["games_restored"]), "games_joined_late": int(g_test["games_joined_late"]),
           "status_ref": g_ref["status"], "status_test": g_test["status"]}
    row["passed"] = bool(identical and row["games_scored_test"] == games_sent and row["duplicate_rows_test"] == 0
                         and g_ref["status"] == g_test["status"] == "complete" and int(g_test["left_early"]) == 1
                         and reread == 0 and row["games_joined_late"] == 0
                         and row["games_restored"] == row["handed_over_open_games"] > 0)
    log.info("result: %s", row)
    append_row(Path(args.out), row, COLUMNS)
    if not row["passed"]:
        log.error("rebalance check FAILED for %s", args.case)
        sys.exit(1)
    log.info("rebalance check OK for %s: %d games scored once each, identical to the reference; "
             "%d open games handed over and restored; 0 moves read twice", args.case, games_sent,
             row["handed_over_open_games"])


if __name__ == "__main__":
    main()
