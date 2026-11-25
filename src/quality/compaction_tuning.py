"""Parquet write settings for the compacted lake, tested on ONE partition (games + moves, one
source_month x time_class). Writes only to --work-dir (node-local /scratch), never to the lake.

Run with cluster/compaction_tuning.sbatch. Variants, all with the partition's current file count:
  a  current settings: repartition(n, game_id), sorted by game_id (moves: game_id, ply), snappy
  b  as a, zstd
  c  range-partitioned and sorted by game start: games by (started_at, game_id), moves in the same
     game order (started_at, game_id, ply); snappy
  d  c + zstd
  e  the smallest of a-d, plus a larger Parquet dictionary page (--dict-page-mb, default 8 MB; 1 MB is
     parquet-mr's default, above which a column chunk falls back from dictionary to plain encoding)
For each variant: file count and size per table, write time, rows and checksum (must equal the
source), the two benchmark queries of compact_lake.py (--runs runs each; results must equal
variant a's), and which columns fell back to PLAIN encoding (first file of each table).
Every variant is read from the same node-local disk right after it is written, so read times are
comparable between variants, not with reads from the home lake.
Output: --out CSV (docs/validation_results/compaction_tuning.csv), one row per variant plus a
reference row for the partition as it is in the lake now.
"""

import csv
import logging
import shutil
import time
from pathlib import Path

import pyarrow.parquet as pq
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.compaction import MB, PARTITION_COLS, list_partitions, partition_checksums
from src.common.config import build_arg_parser, load_config
from src.common.logging_setup import log_duration, setup_logging
from src.common.spark import get_spark
from src.quality.compact_lake import time_queries

STEP = "compaction_tuning"
TABLES = ("games", "moves")
ID_COLUMNS = {"games": ["game_id", "white_id", "black_id"], "moves": ["game_id", "player_id"]}
VARIANTS = {
    "a": ("current: hash(game_id) files, sorted by game_id[, ply], snappy", "game_id", "snappy", None),
    "b": ("as a, zstd", "game_id", "zstd", None),
    "c": ("range files by game start, sorted by started_at, game_id[, ply], snappy", "started_at", "snappy", None),
    "d": ("as c, zstd", "started_at", "zstd", None),
}
COLUMNS = ["variant", "description", "games_files", "games_mb", "moves_files", "moves_mb", "total_mb",
           "total_vs_a_pct", "write_s", "rows_and_checksums_match", "q_count_first_s", "q_count_median_s",
           "q_percentiles_first_s", "q_percentiles_median_s", "query_results_match_a", "plain_encoded_id_columns"]
log = logging.getLogger(STEP)


def ordered(table: str, df: DataFrame, games: DataFrame, order: str, n: int) -> DataFrame:
    """The partition's rows in n files, in the variant's order."""
    if order == "game_id":
        sort = ["game_id"] if table == "games" else ["game_id", "ply"]
        return df.repartition(n, "game_id").sortWithinPartitions(*sort)
    if table == "games":
        return df.repartitionByRange(n, "started_at", "game_id").sortWithinPartitions("started_at", "game_id")
    start = games.select("game_id", F.col("started_at").alias("_start"))
    return (df.join(start, "game_id").repartitionByRange(n, "_start", "game_id")
            .sortWithinPartitions("_start", "game_id", "ply").drop("_start").select(*df.columns))


def plain_columns(table_dir: Path, table: str) -> list[str]:
    """ID columns that used PLAIN (non-dictionary) data pages in the first file of the table."""
    f = sorted(table_dir.rglob("*.parquet"))[0]
    md = pq.read_metadata(f)
    out = set()
    for g in range(md.num_row_groups):
        for c in range(md.num_columns):
            col = md.row_group(g).column(c)
            if col.path_in_schema in ID_COLUMNS[table] and "PLAIN" in col.encodings:
                out.add(col.path_in_schema)
    return sorted(out)


def write_variant(spark: SparkSession, src: dict[str, DataFrame], out: Path, order: str, codec: str,
                  dict_page_mb: float | None, n: dict[str, int]) -> float:
    """Write games and moves of one variant under out/<table>; returns seconds."""
    t0 = time.perf_counter()
    for t in TABLES:
        w = (ordered(t, src[t], src["games"], order, n[t]).write.mode("overwrite")
             .partitionBy(*PARTITION_COLS[t]).option("compression", codec))
        if dict_page_mb:
            w = w.option("parquet.dictionary.page.size", str(int(dict_page_mb * MB)))
        w.parquet(str(out / t))
    return time.perf_counter() - t0


def main() -> None:
    parser = build_arg_parser("Compare Parquet write settings on one partition (writes only to --work-dir)")
    parser.add_argument("--work-dir", required=True, help="node-local folder, e.g. /scratch/$USER/$SLURM_JOB_ID")
    parser.add_argument("--out", required=True, help="CSV, e.g. docs/validation_results/compaction_tuning.csv")
    parser.add_argument("--month", default="2026-10")
    parser.add_argument("--time-class", default="blitz")
    parser.add_argument("--band", type=int, default=6, help="band_id of the percentile query")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--dict-page-mb", type=float, default=8.0)
    args = parser.parse_args()
    cfg = load_config(args.config, args.months)
    setup_logging(STEP, cfg["paths"]["log_dir"])
    lake, work = Path(cfg["paths"]["lake_dir"]).resolve(), Path(args.work_dir) / "variants"
    part = (("source_month", args.month), ("time_class", args.time_class))
    spark = get_spark(cfg, STEP)

    src, n, sums, ref = {}, {}, {}, {"variant": "lake", "description": "partition as it is in the lake now (home)"}
    for t in TABLES:
        src[t] = spark.read.parquet(str(lake / t)).filter(
            (F.col("source_month") == args.month) & (F.col("time_class") == args.time_class))
        files = list_partitions(lake / t)[part]
        n[t] = files.count
        ref[f"{t}_files"], ref[f"{t}_mb"] = files.count, round(files.bytes / MB, 1)
        with log_duration(log, f"rows and checksum of lake {t} {dict(part)}"):
            sums[t] = partition_checksums(src[t], t)[part]
        log.info("source %s: %d files, %.1f MB, %d rows", t, files.count, files.bytes / MB, sums[t][0])
    ref["total_mb"] = round(ref["games_mb"] + ref["moves_mb"], 1)
    ref["plain_encoded_id_columns"] = " ".join(f"{t}:{c}" for t in TABLES
                                               for c in plain_columns(lake / t / f"source_month={args.month}"
                                                                      / f"time_class={args.time_class}", t))

    results: dict[str, dict] = {}

    def run(key: str, description: str, order: str, codec: str, dict_page_mb: float | None) -> None:
        out = work / key
        shutil.rmtree(out, ignore_errors=True)
        with log_duration(log, f"variant {key}: {description}"):
            write_s = write_variant(spark, src, out, order, codec, dict_page_mb, n)
        row = {"variant": key, "description": description, "write_s": round(write_s, 1)}
        ok = True
        for t in TABLES:
            files = list_partitions(out / t)[part]
            row[f"{t}_files"], row[f"{t}_mb"] = files.count, round(files.bytes / MB, 1)
            got = partition_checksums(spark.read.parquet(str(out / t)), t).get(part)
            ok &= got == sums[t]
        row["total_mb"] = round(row["games_mb"] + row["moves_mb"], 1)
        row["rows_and_checksums_match"] = ok
        q = time_queries(spark, out, args.month, args.band, args.runs)
        row.update({"q_count_first_s": round(q["count_moves_by_time_class"]["first_s"], 2),
                    "q_count_median_s": round(q["count_moves_by_time_class"]["median_s"], 2),
                    "q_percentiles_first_s": round(q["blitz_band_move_time_p10_p50_p90"]["first_s"], 2),
                    "q_percentiles_median_s": round(q["blitz_band_move_time_p10_p50_p90"]["median_s"], 2),
                    "_results": {k: v["result"] for k, v in q.items()}})
        row["plain_encoded_id_columns"] = " ".join(
            f"{t}:{c}" for t in TABLES for c in plain_columns(out / t, t))
        results[key] = row
        log.info("variant %s: games %.1f MB, moves %.1f MB, total %.1f MB, checksums %s, write %.0f s",
                 key, row["games_mb"], row["moves_mb"], row["total_mb"], "match" if ok else "MISMATCH", write_s)

    for key, (description, order, codec, dict_mb) in VARIANTS.items():
        run(key, description, order, codec, dict_mb)
    best = min("abcd", key=lambda k: results[k]["total_mb"])
    _, order, codec, _ = VARIANTS[best]
    run("e", f"as {best} (smallest of a-d), dictionary page {args.dict_page_mb:g} MB", order, codec, args.dict_page_mb)

    a_results, a_mb = results["a"]["_results"], results["a"]["total_mb"]
    log.info("query results (variant a): %s", a_results)
    rows = [ref]
    for row in results.values():
        row["total_vs_a_pct"] = round(100 * (row["total_mb"] - a_mb) / a_mb, 1)
        row["query_results_match_a"] = row.pop("_results") == a_results
        rows.append(row)
    ref["total_vs_a_pct"] = round(100 * (ref["total_mb"] - a_mb) / a_mb, 1)

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows({k: r.get(k, "") for k in COLUMNS} for r in rows)
    log.info("written %d rows to %s", len(rows), args.out)
    spark.stop()


if __name__ == "__main__":
    main()
