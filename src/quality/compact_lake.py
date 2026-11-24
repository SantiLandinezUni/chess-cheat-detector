"""One-off compaction of lake/games and lake/moves to right-sized files (docs/partition_compaction_proposal.md).

Run with cluster/compact_lake.sbatch, after every job that reads the lake has finished:
    sbatch --dependency=afterany:<jobs> cluster/compact_lake.sbatch

Steps, all inside one Spark session:
  1. before: file count and size per partition (listing), rows and checksum per partition
     (full scan; checksum = sum of xxhash64 over all data columns), schema;
  2. time the read queries on the current lake (QUERIES, --runs times each);
  3. compact every partition into lake/_compacted/<table> (src/common/compaction.py); the lake is
     not touched;
  4. verify the copy: same partitions, same rows and checksum per partition, same schema, the
     planned number of files. Any mismatch stops here (exit 1): lake unchanged, copy kept to inspect;
  5. swap, one table at a time, by directory rename: lake/<table> -> lake/_old_compaction/<stamp>/<table>,
     lake/_compacted/<table> -> lake/<table>. Refused (exit 2, nothing renamed) while another job from a
     chess-cheat-detector checkout is running;
  6. time the same queries on the swapped lake (results must match step 2);
  7. delete the old lake (only if steps 4 and 6 matched; --keep-old keeps it) and log the space freed.
Both timings run right after a full scan of the data they read (step 1 and step 4), so the file
system cache is in a similar state for both.
Output: --out CSV (docs/validation_results/partition_compaction.csv), long format
(section, table, source_month, time_class, item, before, after, change_pct, note).
"""

import csv
import logging
import os
import shutil
import statistics
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.compaction import (MB, Partition, compact_table, list_partitions, n_files_for,
                                   partition_checksums, target_file_mb)
from src.common.config import build_arg_parser, load_config
from src.common.logging_setup import log_duration, setup_logging
from src.common.spark import get_spark
from src.streaming.baselines_provisional import move_rows

STEP = "compact_lake"
TABLES = ("games", "moves")
CSV_COLUMNS = ["section", "table", "source_month", "time_class", "item", "before", "after", "change_pct", "note"]
SWAP_OK_JOB_NAMES = {"compaction-finish", "feedback-finish"}  # small git-only jobs, never read the lake
log = logging.getLogger(STEP)


def q_count_moves(spark: SparkSession, lake: Path, month: str, band: int) -> str:
    """Moves per time_class for one month."""
    rows = (spark.read.parquet(str(lake / "moves")).filter(F.col("source_month") == month)
            .groupBy("time_class").count().orderBy("time_class").collect())
    return " ".join(f"{r['time_class']}={r['count']}" for r in rows)


def q_blitz_band_percentiles(spark: SparkSession, lake: Path, month: str, band: int) -> str:
    """Move-time p10/p50/p90 of blitz moves in one rating band (moves x games join, as the baselines).

    Exact percentiles (not percentile_approx), so the result cannot depend on the file layout."""
    games = spark.read.parquet(str(lake / "games")).filter(
        (F.col("source_month") == month) & (F.col("time_class") == "blitz"))
    moves = spark.read.parquet(str(lake / "moves")).filter(
        (F.col("source_month") == month) & (F.col("time_class") == "blitz"))
    r = (move_rows(games, moves).filter((F.col("band_id") == band) & F.col("mt").isNotNull())
         .agg(F.count(F.lit(1)).alias("n"), F.expr("percentile(mt, array(0.1, 0.5, 0.9))").alias("p"))
         .first())
    return f"n={r['n']} p10={r['p'][0]} p50={r['p'][1]} p90={r['p'][2]}"


QUERIES = {"count_moves_by_time_class": q_count_moves, "blitz_band_move_time_p10_p50_p90": q_blitz_band_percentiles}


def time_queries(spark: SparkSession, lake: Path, month: str, band: int, runs: int) -> dict[str, dict]:
    """Each query `runs` times: first-run and median seconds, and the (identical every run) result."""
    out = {}
    for name, query in QUERIES.items():
        secs, results = [], set()
        for i in range(runs):
            t0 = time.perf_counter()
            results.add(query(spark, lake, month, band))
            secs.append(time.perf_counter() - t0)
            log.info("query %s run %d: %.1f s", name, i + 1, secs[-1])
        if len(results) != 1:
            raise RuntimeError(f"query {name} gave different results across runs: {results}")
        out[name] = {"first_s": secs[0], "median_s": statistics.median(secs), "result": results.pop()}
    return out


def other_running_jobs() -> list[str]:
    """Running jobs of this user from a chess-cheat-detector checkout, other than this one and git-only jobs."""
    me = os.getenv("SLURM_JOB_ID", "")
    out = subprocess.run(["squeue", "-u", os.environ["USER"], "-h", "-t", "RUNNING", "-o", "%A|%j|%Z"],
                         check=True, capture_output=True, text=True).stdout
    jobs = []
    for line in out.splitlines():
        job_id, name, workdir = line.split("|", 2)
        if job_id != me and name not in SWAP_OK_JOB_NAMES and "chess-cheat-detector" in workdir:
            jobs.append(f"{job_id} {name} {workdir}")
    return jobs


def du(path: Path) -> tuple[int, int]:
    """(apparent bytes, allocated bytes) of a folder."""
    apparent = int(subprocess.run(["du", "-sb", str(path)], check=True, capture_output=True, text=True)
                   .stdout.split()[0])
    allocated = int(subprocess.run(["du", "-sk", str(path)], check=True, capture_output=True, text=True)
                    .stdout.split()[0]) * 1024
    return apparent, allocated


def pct(before: float, after: float) -> float | str:
    """% change from before to after."""
    return round(100 * (after - before) / before, 1) if before else ""


def file_rows(table: str, before: dict, after: dict, sums_before: dict, sums_after: dict) -> list[dict]:
    """CSV rows: per-partition and total file statistics, before and after."""
    rows = []
    for part in sorted(before):
        b, a = before[part], after.get(part)
        kv = dict(part)
        base = {"section": "files", "table": table, "source_month": kv.get("source_month", ""),
                "time_class": kv.get("time_class", "")}
        stats = {
            "file_count": (b.count, a.count if a else ""),
            "total_mb": (round(b.bytes / MB, 1), round(a.bytes / MB, 1) if a else ""),
            "avg_file_mb": (round(b.bytes / b.count / MB, 2), round(a.bytes / a.count / MB, 2) if a else ""),
            "max_file_mb": (round(max(b.sizes) / MB, 2), round(max(a.sizes) / MB, 2) if a else ""),
            "rows": (sums_before[part][0], sums_after.get(part, ("", ""))[0]),
            "checksum": (sums_before[part][1], sums_after.get(part, ("", ""))[1]),
        }
        for item, (vb, va) in stats.items():
            note = ""
            if item in ("rows", "checksum"):
                note = "match" if vb == va else "MISMATCH"
            rows.append({**base, "item": item, "before": vb, "after": va,
                         "change_pct": pct(vb, va) if item not in ("checksum",) and va != "" else "", "note": note})
    tb = (sum(p.count for p in before.values()), sum(p.bytes for p in before.values()))
    ta = (sum(p.count for p in after.values()), sum(p.bytes for p in after.values()))
    for item, vb, va in (("file_count", tb[0], ta[0]), ("total_mb", round(tb[1] / MB, 1), round(ta[1] / MB, 1))):
        rows.append({"section": "total", "table": table, "source_month": "", "time_class": "", "item": item,
                     "before": vb, "after": va, "change_pct": pct(vb, va), "note": ""})
    return rows


def query_rows(before: dict, after: dict | None, month: str, band: int) -> list[dict]:
    """CSV rows: query timings and results, before and after."""
    rows = []
    for name, b in before.items():
        a = (after or {}).get(name, {})
        note = f"source_month={month}" + (f" band_id={band} time_class=blitz" if "band" in name else "")
        for item in ("first_s", "median_s"):
            rows.append({"section": "query", "table": "", "source_month": month, "time_class": "",
                         "item": f"{name}_{item}", "before": round(b[item], 2),
                         "after": round(a[item], 2) if a else "", "change_pct": pct(b[item], a[item]) if a else "",
                         "note": note})
        same = "same result" if a and a["result"] == b["result"] else ("RESULT DIFFERS" if a else "")
        rows.append({"section": "query", "table": "", "source_month": month, "time_class": "",
                     "item": f"{name}_result", "before": b["result"], "after": a.get("result", ""),
                     "change_pct": "", "note": same})
    return rows


def write_csv(path: str, rows: list[dict]) -> None:
    """Write the result CSV (also on failure, with what is known)."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    log.info("written %d rows to %s", len(rows), path)


def main() -> None:
    parser = build_arg_parser("Compact lake/games and lake/moves to right-sized Parquet files, then swap")
    parser.add_argument("--out", required=True, help="CSV, e.g. docs/validation_results/partition_compaction.csv")
    parser.add_argument("--query-month", default="2026-10", help="month of the timed queries")
    parser.add_argument("--query-band", type=int, default=6, help="band_id of the blitz percentile query (6 = 1600-1799)")
    parser.add_argument("--runs", type=int, default=3, help="runs per timed query")
    parser.add_argument("--keep-old", action="store_true", help="keep the old lake after a verified swap")
    parser.add_argument("--skip-job-check", action="store_true",
                        help="swap even if other chess jobs are running (only for a test lake)")
    args = parser.parse_args()
    cfg = load_config(args.config, args.months)
    setup_logging(STEP, cfg["paths"]["log_dir"])
    lake = Path(cfg["paths"]["lake_dir"]).resolve()
    target = target_file_mb(cfg)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    compacted, old = lake / "_compacted", lake / "_old_compaction" / stamp
    log.info("lake=%s target_file_mb=%s query_month=%s band=%d", lake, target, args.query_month, args.query_band)
    if (lake / "_old_compaction").exists():
        raise SystemExit(f"{lake / '_old_compaction'} exists from an earlier run: check it and remove it first")
    shutil.rmtree(compacted, ignore_errors=True)  # leftovers of a failed run

    spark = get_spark(cfg, STEP)
    files_before, sums_before, schema_before = {}, {}, {}
    for t in TABLES:
        files_before[t] = list_partitions(lake / t)
        with log_duration(log, f"rows and checksums of lake/{t}"):
            df = spark.read.parquet(str(lake / t))
            schema_before[t] = df.schema.simpleString()
            sums_before[t] = partition_checksums(df, t)
        log.info("before %s: %d partitions, %d files, %.1f GB, %d rows", t, len(files_before[t]),
                 sum(p.count for p in files_before[t].values()),
                 sum(p.bytes for p in files_before[t].values()) / 1024 / MB, sum(n for n, _ in sums_before[t].values()))
    with log_duration(log, "timed queries before"):
        timings_before = time_queries(spark, lake, args.query_month, args.query_band, args.runs)

    files_after, sums_after, errors = {}, {}, []
    for t in TABLES:
        with log_duration(log, f"compact lake/{t} -> {compacted / t}"):
            compact_table(spark, lake / t, compacted / t, t, target, log)
        files_after[t] = list_partitions(compacted / t)
        with log_duration(log, f"rows and checksums of {compacted / t}"):
            df = spark.read.parquet(str(compacted / t))
            sums_after[t] = partition_checksums(df, t)
            if df.schema.simpleString() != schema_before[t]:
                errors.append(f"{t}: schema differs: {schema_before[t]} vs {df.schema.simpleString()}")
        if set(files_after[t]) != set(files_before[t]):
            errors.append(f"{t}: partitions differ: {sorted(set(files_before[t]) ^ set(files_after[t]))}")
        for part, (n, h) in sums_before[t].items():
            if sums_after[t].get(part) != (n, h):
                errors.append(f"{t} {dict(part)}: rows/checksum {n}/{h} before, {sums_after[t].get(part)} after")
        for part, files in files_before[t].items():
            planned = n_files_for(files.bytes, target)
            if part in files_after[t] and files_after[t][part].count != planned:
                errors.append(f"{t} {dict(part)}: {files_after[t][part].count} files, planned {planned}")

    rows = [r for t in TABLES for r in file_rows(t, files_before[t], files_after[t], sums_before[t], sums_after[t])]
    if errors:
        for e in errors:
            log.error("verification: %s", e)
        write_csv(args.out, rows + query_rows(timings_before, None, args.query_month, args.query_band))
        raise SystemExit(f"verification failed ({len(errors)} errors): lake unchanged, copy kept in {compacted}")
    log.info("verification passed: every partition has the same rows and checksum, schemas equal")

    busy = [] if args.skip_job_check else other_running_jobs()
    if busy:
        write_csv(args.out, rows + query_rows(timings_before, None, args.query_month, args.query_band))
        log.error("other jobs are running from a chess-cheat-detector checkout: %s", busy)
        raise SystemExit(2)
    with log_duration(log, "swap"):
        old.mkdir(parents=True)
        for t in TABLES:
            (lake / t).rename(old / t)
            (compacted / t).rename(lake / t)
            log.info("swapped %s: old lake/%s now in %s", t, t, old / t)
        shutil.rmtree(compacted, ignore_errors=True)

    with log_duration(log, "timed queries after"):
        timings_after = time_queries(spark, lake, args.query_month, args.query_band, args.runs)
    rows += query_rows(timings_before, timings_after, args.query_month, args.query_band)
    same = all(timings_before[q]["result"] == timings_after[q]["result"] for q in QUERIES)

    if not same:
        log.error("query results differ after the swap: old lake kept in %s", old)
    elif args.keep_old:
        log.info("--keep-old: old lake kept in %s", old)
    else:
        apparent, allocated = du(old)
        new_apparent = sum(du(lake / t)[0] for t in TABLES)
        with log_duration(log, f"delete {old}"):
            shutil.rmtree(old)
            try:
                old.parent.rmdir()
            except OSError:
                pass
        log.info("old lake deleted: %.1f GB apparent, %.1f GB allocated on disk; new games+moves %.1f GB; net freed %.1f GB",
                 apparent / 1024**3, allocated / 1024**3, new_apparent / 1024**3, (apparent - new_apparent) / 1024**3)
        rows += [{"section": "space", "table": "", "source_month": "", "time_class": "", "item": item,
                  "before": round(v / 1024**3, 2), "after": "", "change_pct": "", "note": note}
                 for item, v, note in (("old_lake_deleted_gb_apparent", apparent, "du -sb"),
                                       ("old_lake_deleted_gb_allocated", allocated, "du -sk (ZFS, after compression)"),
                                       ("net_freed_gb_apparent", apparent - new_apparent,
                                        "old games+moves minus new games+moves, du -sb"))]
    write_csv(args.out, rows)
    spark.stop()
    if not same:
        raise SystemExit(3)


if __name__ == "__main__":
    main()
