"""Numbers for report chapter 8 (data at scale), written as two small CSVs.

1. dq_full_months.csv: the full-month data-quality reports (logs/dq_parquet-<job>.json, git-ignored)
   flattened to one row per month and check. The `examples` field (game ids) is dropped.
2. data_at_scale_by_month.csv: per month and time class, games and how many of them have clocks,
   evals, a BOT player, 0 plies or an Elo outside 400-3500 (lake/games); per month, distinct players
   (restricted/identity_by_month) and over all months (restricted/identity). The identity tables are
   counted from Parquet footers only: no column is read, so no username is ever loaded.
3. data_at_scale_zero_ply.csv: termination of the games with 0 plies, per month.

Reads the lake, so on the cluster it runs through srun, never on the login node:
    srun -p interactive --cpus-per-task=4 --mem=16G --time=00:30:00 \\
        python scripts/data_at_scale_stats.py --lake data/lake \\
        --dq 2026-08=logs/dq_parquet-28560.json --dq 2026-09=logs/dq_parquet-28559.json \\
        --dq 2026-10=logs/dq_parquet-28226.json --out-dir docs/validation_results
"""

import argparse
import csv
import json
import logging
from collections import defaultdict
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

log = logging.getLogger("data_at_scale_stats")
TIME_CLASSES = ["ultrabullet", "bullet", "blitz", "rapid", "classical", "correspondence"]
DQ_COLUMNS = ["source_month", "check_id", "level", "applies_to", "description", "status", "violations", "value",
              "threshold", "rows_games", "rows_moves", "generated_at"]
LAKE_COLUMNS = ["source_month", "time_class", "games", "moves", "players", "games_with_clock", "games_with_eval",
                "bot_games", "zero_ply_games", "games_elo_below_400", "games_elo_above_3500",
                "games_elo_above_3500_with_bot"]
ZERO_PLY_COLUMNS = ["source_month", "termination", "games"]


def dq_rows(reports: dict[str, Path]) -> list[dict]:
    """One row per (month, check) from the full-month DQ JSON reports, without examples."""
    out = []
    for month, path in sorted(reports.items()):
        report = json.loads(path.read_text())
        if report["source_months"] != [month]:
            raise ValueError(f"{path}: covers {report['source_months']}, expected {month}")
        for c in report["checks"]:
            out.append({"source_month": month, "check_id": c["id"], "level": c["level"],
                        "applies_to": c["applies_to"], "description": c["description"], "status": c["status"],
                        "violations": c.get("violations"), "value": c.get("value"),
                        "threshold": json.dumps(c["threshold"]) if "threshold" in c else None,
                        "rows_games": report["row_counts"]["games"], "rows_moves": report["row_counts"]["moves"],
                        "generated_at": report["generated_at"]})
    return out


def footer_rows(path: str) -> dict[str, int]:
    """Rows per source_month partition (or 'all' if unpartitioned), from Parquet footers only."""
    out: dict[str, int] = defaultdict(int)
    for frag in ds.dataset(path, format="parquet", partitioning="hive").get_fragments():
        month = ds.get_partition_keys(frag.partition_expression).get("source_month", "all")
        out[str(month)] += pq.ParquetFile(frag.path).metadata.num_rows
    return dict(out)


def lake_rows(lake: str, months: list[str], moves_by_month: dict[str, int],
              zero_ply: dict[tuple[str, str], int]) -> list[dict]:
    """Per (month, time class) game counts, plus one 'all' row per month and one for all months.
    Also fills `zero_ply` with the termination of the games that have 0 plies, per month."""
    games = ds.dataset(f"{lake}/games", format="parquet", partitioning="hive")
    cols = ["has_clock", "has_eval", "has_bot", "white_elo", "black_elo", "n_plies", "termination"]
    total = lambda arr: pc.sum(arr).as_py() or 0  # noqa: E731
    rows = []
    for month in months:
        month_total: dict[str, int] = defaultdict(int)
        for tc in TIME_CLASSES:
            r = defaultdict(int)
            flt = (ds.field("source_month") == month) & (ds.field("time_class") == tc)
            for b in games.to_batches(columns=cols, filter=flt, batch_size=1_000_000):
                r["games"] += b.num_rows
                r["games_with_clock"] += total(b["has_clock"])
                r["games_with_eval"] += total(b["has_eval"])
                r["bot_games"] += total(b["has_bot"])
                r["zero_ply_games"] += total(pc.equal(b["n_plies"], 0))
                r["games_elo_below_400"] += total(pc.or_(pc.less(b["white_elo"], 400), pc.less(b["black_elo"], 400)))
                high = pc.or_(pc.greater(b["white_elo"], 3500), pc.greater(b["black_elo"], 3500))
                r["games_elo_above_3500"] += total(high)
                r["games_elo_above_3500_with_bot"] += total(pc.and_(high, b["has_bot"]))
                empty = b.filter(pc.equal(b["n_plies"], 0))
                for term, n in zip(*pc.value_counts(empty["termination"]).flatten()):
                    zero_ply[(month, term.as_py())] += n.as_py()
            for k, v in r.items():
                month_total[k] += v
            rows.append({"source_month": month, "time_class": tc, **r})
            log.info("%s %s: %d games", month, tc, r["games"])
        rows.append({"source_month": month, "time_class": "all", **month_total})
    players = footer_rows(f"{lake}/restricted/identity_by_month")
    for r in rows:
        if r["time_class"] == "all":
            r["players"] = players.get(r["source_month"])
            r["moves"] = moves_by_month.get(r["source_month"])
    grand: dict[str, int] = defaultdict(int)
    for r in rows:
        if r["time_class"] == "all":
            for k in LAKE_COLUMNS[2:]:
                if k != "players":
                    grand[k] += r.get(k) or 0
    grand["players"] = footer_rows(f"{lake}/restricted/identity").get("all")
    rows.append({"source_month": "all", "time_class": "all", **grand})
    return rows


def write(path: Path, columns: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    log.info("written %d rows to %s", len(rows), path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Chapter 8 numbers: full-month DQ results and lake counts")
    parser.add_argument("--lake", required=True, help="lake directory (data/lake)")
    parser.add_argument("--dq", action="append", required=True, metavar="MONTH=PATH",
                        help="full-month DQ JSON report, once per month")
    parser.add_argument("--out-dir", default="docs/validation_results")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    reports = {m: Path(p) for m, p in (x.split("=", 1) for x in args.dq)}
    dq = dq_rows(reports)
    out = Path(args.out_dir)
    write(out / "dq_full_months.csv", DQ_COLUMNS, dq)
    moves = {r["source_month"]: r["rows_moves"] for r in dq}
    zero_ply: dict[tuple[str, str], int] = defaultdict(int)
    lake = lake_rows(args.lake, sorted(reports), moves, zero_ply)
    games_dq = {r["source_month"]: r["rows_games"] for r in dq}
    for r in lake:
        if r["time_class"] == "all" and r["source_month"] in games_dq and r["games"] != games_dq[r["source_month"]]:
            raise ValueError(f"{r['source_month']}: lake has {r['games']} games, DQ report {games_dq[r['source_month']]}")
    write(out / "data_at_scale_by_month.csv", LAKE_COLUMNS, lake)
    write(out / "data_at_scale_zero_ply.csv", ZERO_PLY_COLUMNS,
          [{"source_month": m, "termination": t, "games": n} for (m, t), n in sorted(zero_ply.items())])


if __name__ == "__main__":
    main()
