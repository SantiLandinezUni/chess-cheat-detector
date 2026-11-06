"""Build step 4: data quality checks (data dictionary §7) against the Parquet lake.

Run from the repo root with the venv active:
    PYTHONPATH=. spark-submit --driver-memory 3g src/quality/checks.py --config config/local.yaml

Hard checks fail the step (exit code 1), soft checks only log warnings. Results are saved as
JSON for the report (default docs/validation_results/dq_parquet.json). The file holds counts
and game_ids only, never usernames.
"""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import REPO_ROOT, build_arg_parser, load_config
from src.common.logging_setup import log_duration, setup_logging
from src.common.spark import get_spark

STEP = "dq_checks"
DEFAULT_OUTPUT = REPO_ROOT / "docs" / "validation_results" / "dq_parquet.json"
RESULTS = ["1-0", "0-1", "1/2-1/2", "*"]
N_EXAMPLES = 5

# NN columns from data dictionary §4.1 and §4.2
GAMES_REQUIRED = ["game_id", "source_month", "event", "is_tournament", "time_control", "time_class",
                  "started_at", "white_id", "black_id", "white_elo", "black_elo", "has_bot", "result",
                  "termination", "n_plies", "has_clock", "has_eval", "ingested_at"]
MOVES_REQUIRED = ["game_id", "ply", "color", "player_id", "san", "uci", "source_month", "time_class"]

log = logging.getLogger(STEP)


def _examples(df: DataFrame, col: str = "game_id") -> list[str]:
    """A few example ids of violating rows, for debugging."""
    return [r[0] for r in df.select(col).distinct().limit(N_EXAMPLES).collect()]


def _result(check_id: str, description: str, level: str, applies_to: str, violations: int | None,
            examples: list[str] | None = None, value: float | None = None, threshold: Any = None,
            status: str | None = None) -> dict[str, Any]:
    """One check result; status is derived from level and violations unless given."""
    if status is None:
        status = "pass" if not violations else ("fail" if level == "hard" else "warn")
    res = {"id": check_id, "description": description, "level": level, "applies_to": applies_to,
           "status": status, "violations": violations}
    if value is not None:
        res["value"] = value
    if threshold is not None:
        res["threshold"] = threshold
    if examples:
        res["examples"] = examples
    log_fn = log.error if status == "fail" else log.warning if status == "warn" else log.info
    log_fn("[%s] %s %s: %s violations=%s%s", status.upper(), level, check_id, description, violations,
           f" value={value}" if value is not None else "")
    return res


def run_checks(spark: SparkSession, cfg: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Run all §7 checks for the configured months. Returns (results, row counts)."""
    lake, months, q = cfg["paths"]["lake_dir"], cfg["months"], cfg["quality"]
    games = spark.read.parquet(f"{lake}/games").filter(F.col("source_month").isin(months))
    moves = spark.read.parquet(f"{lake}/moves").filter(F.col("source_month").isin(months))
    counts = {"games": games.count(), "moves": moves.count()}
    log.info("rows: games=%d moves=%d", counts["games"], counts["moves"])
    results = []

    # ---------- hard ----------
    dup_games = games.groupBy("game_id").count().filter("count > 1")
    null_ids = games.filter(F.col("game_id").isNull()).count()
    results.append(_result("games_pk", "game_id unique and not null", "hard", "games",
                           dup_games.count() + null_ids, _examples(dup_games)))

    dup_moves = moves.groupBy("game_id", "ply").count().filter("count > 1")
    results.append(_result("moves_pk", "(game_id, ply) unique", "hard", "moves",
                           dup_moves.count(), _examples(dup_moves)))

    for name, df, cols in (("games", games, GAMES_REQUIRED), ("moves", moves, MOVES_REQUIRED)):
        nulls = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]).first().asDict()
        bad_cols = {c: n for c, n in nulls.items() if n}
        res = _result(f"{name}_required_not_null", "required (NN) columns not null", "hard", name,
                      sum(bad_cols.values()))
        if bad_cols:
            res["null_counts"] = bad_cols
        results.append(res)

    bad_result = games.filter(~F.col("result").isin(RESULTS))
    results.append(_result("games_result_allowed", f"result in {RESULTS}", "hard", "games",
                           bad_result.count(), _examples(bad_result)))

    same_player = games.filter(F.col("white_id") == F.col("black_id"))
    results.append(_result("games_distinct_players", "white_id != black_id", "hard", "games",
                           same_player.count(), _examples(same_player)))

    orphans = moves.join(games.select("game_id"), "game_id", "left_anti")
    results.append(_result("moves_fk_games", "every moves.game_id exists in games", "hard", "moves",
                           orphans.count(), _examples(orphans)))

    move_counts = moves.groupBy("game_id").agg(F.count("*").alias("n_rows"))
    ply_mismatch = (games.join(move_counts, "game_id", "left")
                    .filter(F.col("n_plies") != F.coalesce(F.col("n_rows"), F.lit(0))))
    results.append(_result("n_plies_matches_moves", "n_plies = count of moves rows", "hard", "games, moves",
                           ply_mismatch.count(), _examples(ply_mismatch)))

    # ---------- soft ----------
    elo_lo, elo_hi = q["elo_min"], q["elo_max"]
    out_of_range = games.filter(~F.col("white_elo").between(elo_lo, elo_hi) | ~F.col("black_elo").between(elo_lo, elo_hi))
    results.append(_result("elo_range", f"Elo between {elo_lo} and {elo_hi}", "soft", "games",
                           out_of_range.count(), _examples(out_of_range), threshold=[elo_lo, elo_hi]))

    neg = moves.filter(F.col("move_time_s") < q["min_move_time_s"])
    results.append(_result("move_time_negative", f"move_time_s < {q['min_move_time_s']} s", "soft", "moves",
                           neg.count(), _examples(neg), threshold=q["min_move_time_s"]))

    zero = games.filter(F.col("n_plies") == 0)
    results.append(_result("zero_plies", "n_plies = 0", "soft", "games", zero.count(), _examples(zero)))

    no_clock = games.filter(~F.col("has_clock")).count()
    share = round(no_clock / counts["games"], 4) if counts["games"] else 0.0
    results.append(_result("share_no_clock", f"share of games with has_clock = false > {q['max_share_no_clock']:.0%}",
                           "soft", "games", no_clock, value=share, threshold=q["max_share_no_clock"],
                           status="warn" if share > q["max_share_no_clock"] else "pass"))

    bots = games.filter("has_bot")
    results.append(_result("bot_games", "games with a BOT-title player (kept, flagged, excluded from human baselines)",
                           "soft", "games", bots.count(), _examples(bots)))

    # ---------- labels: needs core.player_labels (build step 7) ----------
    results.append(_result("players_have_labels", "every player in games has a row in player_labels",
                           "soft", "labels", None, status="pending"))
    return results, counts


def write_report(results: list[dict[str, Any]], counts: dict[str, int], cfg: dict[str, Any], path: Path) -> None:
    """Save the results as JSON for the report."""
    summary = {s: sum(r["status"] == s for r in results) for s in ("pass", "fail", "warn", "pending")}
    report = {
        "step": STEP,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "env": cfg["env"],
        "source_months": cfg["months"],
        "row_counts": counts,
        "summary": summary,
        "checks": results,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    parser = build_arg_parser("Data quality checks on the Parquet lake")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="where to save the JSON results")
    args = parser.parse_args()
    cfg = load_config(args.config, args.months)
    setup_logging(STEP, cfg["paths"]["log_dir"])

    spark = get_spark(cfg, STEP)
    with log_duration(log, STEP):
        results, counts = run_checks(spark, cfg)
        write_report(results, counts, cfg, Path(args.output))
        log.info("results saved to %s", args.output)
    spark.stop()

    failed = [r["id"] for r in results if r["status"] == "fail"]
    if failed:
        log.error("hard checks failed: %s", failed)
        sys.exit(1)
    log.info("all hard checks passed")


if __name__ == "__main__":
    main()
