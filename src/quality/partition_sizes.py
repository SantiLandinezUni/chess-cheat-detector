"""File count and file sizes per (source_month, time_class) partition of lake/games and lake/moves.

Measures partition skew (small or oversized Parquet files) before any compaction. Reads only the
file system listing and the Parquet footers (row counts), never the data pages, so it needs no Spark.
On the cluster it still reads the lake, so run it as a job, never on the login node:
    sbatch cluster/partition_sizes.sbatch
Locally:
    python -m src.quality.partition_sizes --config config/local.yaml --out docs/validation_results/partition_sizes_before.csv

One CSV row per (table, source_month, time_class): file_count, total / mean / median / min / max
file size in MB, files under --small-mb, rows (from the footers) and rows per file.
"""

import csv
import logging
import statistics
from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq

from src.common.config import build_arg_parser, load_config
from src.common.logging_setup import log_duration, setup_logging

STEP = "partition_sizes"
TABLES = ("games", "moves")
MB = 1024 * 1024
DEFAULT_SMALL_MB = 32.0
COLUMNS = ["table", "source_month", "time_class", "file_count", "total_mb", "avg_file_mb", "median_file_mb",
           "min_file_mb", "max_file_mb", "files_under_small_mb", "small_mb", "rows", "rows_per_file"]
log = logging.getLogger(STEP)


def partition_values(rel: Path) -> dict[str, str]:
    """{column: value} from the Hive-style folders of a path relative to the table root."""
    return dict(part.split("=", 1) for part in rel.parts[:-1] if "=" in part)


def scan_table(table_dir: Path, table: str, small_mb: float) -> list[dict]:
    """One row per (source_month, time_class) partition of one lake table."""
    sizes: dict[tuple[str, str], list[int]] = defaultdict(list)
    rows: dict[tuple[str, str], int] = defaultdict(int)
    for path in table_dir.rglob("*.parquet"):
        rel = path.relative_to(table_dir)
        if any(p.startswith(("_", ".")) for p in rel.parts):  # _temporary/, staging, hidden files
            continue
        values = partition_values(rel)
        key = (values.get("source_month", ""), values.get("time_class", ""))
        sizes[key].append(path.stat().st_size)
        rows[key] += pq.read_metadata(path).num_rows

    out = []
    for (month, time_class), s in sorted(sizes.items()):
        out.append({
            "table": table, "source_month": month, "time_class": time_class, "file_count": len(s),
            "total_mb": round(sum(s) / MB, 1), "avg_file_mb": round(statistics.mean(s) / MB, 2),
            "median_file_mb": round(statistics.median(s) / MB, 2), "min_file_mb": round(min(s) / MB, 3),
            "max_file_mb": round(max(s) / MB, 2), "files_under_small_mb": sum(x < small_mb * MB for x in s),
            "small_mb": small_mb, "rows": rows[(month, time_class)],
            "rows_per_file": round(rows[(month, time_class)] / len(s)),
        })
    return out


def main() -> None:
    parser = build_arg_parser("File count and sizes per partition of lake/games and lake/moves")
    parser.add_argument("--out", required=True, help="CSV to write, e.g. docs/validation_results/partition_sizes_before.csv")
    parser.add_argument("--small-mb", type=float, default=DEFAULT_SMALL_MB, help="'small file' threshold in MB")
    args = parser.parse_args()
    cfg = load_config(args.config, args.months)
    setup_logging(STEP, cfg["paths"]["log_dir"])
    lake = Path(cfg["paths"]["lake_dir"])

    result = []
    for table in TABLES:
        with log_duration(log, f"scan {table}"):
            rows = scan_table(lake / table, table, args.small_mb)
        log.info("%s: %d partitions, %d files, %.1f GB", table, len(rows), sum(r["file_count"] for r in rows),
                 sum(r["total_mb"] for r in rows) / 1024)
        result += rows

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(result)
    log.info("written %d rows to %s", len(result), args.out)


if __name__ == "__main__":
    main()
