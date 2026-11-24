"""Month-to-month variability of the provisional baselines (feedback item 3.3).

Runs the provisional baselines (src/streaming/baselines_provisional.py, same population and same
definitions) separately for each month of --months, and writes one comparison CSV. It does NOT
write lake/baselines_provisional, so the streaming scorer's baselines stay as they are.

On the cluster: cluster/baselines_by_month.sbatch 2026-08,2026-09,2026-10
Locally (one month only, the sample):
    PYTHONPATH=. spark-submit --driver-memory 3g src/streaming/baselines_by_month.py \
        --config config/local.yaml --months 2026-09 --out /tmp/baselines_by_month.csv

One CSV row per (time_class, band_id, source_month), from:
  - level "move", feature move_time_s: n_moves, mean, sd and the percentiles p01..p99 of single
    move times (moves with move_time_s >= 0);
  - share_fast_moves: share of those moves under FAST_MOVE_S (2 s), over all moves of the cell;
  - level "game": n_player_games and mean_share_moves_under_2s, the mean over player-games of the
    per-game share of fast moves (the feature the scorer compares a live game with).
Comparison columns: for p50, p90 and share_fast_moves, the cell's mean over the months and this
month's relative deviation from it (dev_pct = 100 x (month - mean) / mean).
The aggregates are a few hundred rows, so they are collected to the driver.
"""

import csv
import logging
from pathlib import Path

from pyspark.sql import functions as F

from src.common.config import build_arg_parser, load_config
from src.common.logging_setup import log_duration, setup_logging
from src.common.spark import get_spark
from src.features.baselines import PCT_NAMES
from src.streaming.baselines_provisional import FAST_MOVE_S, build_provisional_baselines, move_rows

STEP = "baselines_by_month"
COMPARED = ["p50", "p90", "share_fast_moves"]
COLUMNS = (["time_class", "band_id", "source_month", "n_moves", "mean", "sd", *PCT_NAMES, "share_fast_moves",
            "n_player_games", "mean_share_moves_under_2s"]
           + [f"{c}_{s}" for c in COMPARED for s in ("months_mean", "dev_pct")])
log = logging.getLogger(STEP)


def month_rows(spark, lake: str, month: str) -> list[dict]:
    """Move-time distribution, fast-move shares and player-game counts per band x time class for one month."""
    games = spark.read.parquet(f"{lake}/games").filter(F.col("source_month") == month)
    moves = spark.read.parquet(f"{lake}/moves").filter(F.col("source_month") == month)

    base = build_provisional_baselines(games, moves).filter(
        ((F.col("level") == "move") & (F.col("feature") == "move_time_s"))
        | ((F.col("level") == "game") & F.col("feature").isin("n_moves", "share_moves_under_2s")))
    cells: dict[tuple[str, int], dict] = {}
    for r in base.collect():
        cell = cells.setdefault((r["time_class"], r["band_id"]),
                                {"time_class": r["time_class"], "band_id": r["band_id"], "source_month": month})
        if r["level"] == "move":
            cell.update({"n_moves": r["n"], "mean": r["mean"], "sd": r["sd"], **{p: r[p] for p in PCT_NAMES}})
        elif r["feature"] == "n_moves":
            cell["n_player_games"] = r["n"]
        else:
            cell["mean_share_moves_under_2s"] = r["mean"]

    fast = (move_rows(games, moves).filter(F.col("mt").isNotNull())
            .groupBy("time_class", "band_id").agg(F.avg((F.col("mt") < FAST_MOVE_S).cast("double")).alias("share")))
    for r in fast.collect():
        if (r["time_class"], r["band_id"]) in cells:
            cells[(r["time_class"], r["band_id"])]["share_fast_moves"] = r["share"]
    return list(cells.values())


def add_comparison(rows: list[dict]) -> None:
    """Add each cell's mean over the months and this month's % deviation from it, for COMPARED."""
    by_cell: dict[tuple[str, int], list[dict]] = {}
    for r in rows:
        by_cell.setdefault((r["time_class"], r["band_id"]), []).append(r)
    for group in by_cell.values():
        for c in COMPARED:
            vals = [r[c] for r in group if r.get(c) is not None]
            mean = sum(vals) / len(vals) if vals else None
            for r in group:
                r[f"{c}_months_mean"] = mean
                r[f"{c}_dev_pct"] = (100 * (r[c] - mean) / mean
                                     if mean and r.get(c) is not None else None)


def fmt(v):
    """Round floats for the CSV."""
    return round(v, 4) if isinstance(v, float) else v


def main() -> None:
    parser = build_arg_parser("Provisional baselines per month, compared (no lake writes)")
    parser.add_argument("--out", required=True, help="CSV to write, e.g. docs/validation_results/baselines_by_month.csv")
    args = parser.parse_args()
    cfg = load_config(args.config, args.months)
    setup_logging(STEP, cfg["paths"]["log_dir"])
    spark = get_spark(cfg, STEP)
    lake = cfg["paths"]["lake_dir"]
    log.info("months=%s lake=%s", cfg["months"], lake)

    rows: list[dict] = []
    for month in cfg["months"]:
        with log_duration(log, f"baselines {month}"):
            mr = month_rows(spark, lake, month)
        log.info("%s: %d cells, %d moves", month, len(mr), sum(r.get("n_moves") or 0 for r in mr))
        rows += mr
    add_comparison(rows)
    rows.sort(key=lambda r: (r["time_class"], r["band_id"] is None, r["band_id"] or 0, r["source_month"]))

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows({k: fmt(r.get(k)) for k in COLUMNS} for r in rows)
    log.info("written %d rows to %s", len(rows), args.out)
    spark.stop()


if __name__ == "__main__":
    main()
