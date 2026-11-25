"""Check that every kind of reader in the pipeline can read the lake after a codec change.

  - Spark (all steps): row count of every Parquet table under the lake;
  - pyarrow and pandas (scorer baselines, serving loader, replay cache, notebooks): the first file
    of every table, read whole;
  - the codec of the games and moves files (must be lake.parquet_codec);
  - pyarrow writer (scorer.write_final): write a small file with the configured codec and read it back.
Prints counts only, never row contents (restricted/ tables hold usernames).
Exit code 1 if any read fails or games/moves files use another codec.
On the cluster it reads the lake, so it runs inside a job:
    PYTHONPATH=. spark-submit --master local[8] src/quality/codec_read_check.py --config config/cluster.yaml --work-dir /scratch/...
"""

import logging
import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from src.common.config import build_arg_parser, load_config, parquet_codec
from src.common.logging_setup import setup_logging
from src.common.spark import get_spark
from src.streaming.scorer import FINAL_SCHEMA, write_final

STEP = "codec_read_check"
CODEC_TABLES = ("games", "moves")
log = logging.getLogger(STEP)


def parquet_tables(lake: Path) -> list[Path]:
    """Folders under the lake that hold Parquet files and are tables (not partitions of a table)."""
    tables = []
    for d in sorted(p for p in lake.rglob("*") if p.is_dir()):
        rel = d.relative_to(lake)
        if any(part.startswith(("_", ".")) or "=" in part for part in rel.parts):
            continue
        if any(t in d.parents for t in tables):
            continue
        if next(d.glob("*.parquet"), None) or next(d.glob("*=*/**/*.parquet"), None):
            tables.append(d)
    return tables


def main() -> None:
    parser = build_arg_parser("Read every lake table with Spark, pyarrow and pandas")
    parser.add_argument("--work-dir", required=True, help="scratch folder for the pyarrow writer test")
    args = parser.parse_args()
    cfg = load_config(args.config, args.months)
    setup_logging(STEP, cfg["paths"]["log_dir"])
    lake, codec = Path(cfg["paths"]["lake_dir"]).resolve(), parquet_codec(cfg)
    spark = get_spark(cfg, STEP)
    errors = []

    for table in parquet_tables(lake):
        name = str(table.relative_to(lake))
        try:
            n = spark.read.parquet(str(table)).count()
            first = sorted(p for p in table.rglob("*.parquet")
                           if not any(x.startswith(("_", ".")) for x in p.relative_to(table).parts))[0]
            n_arrow = pq.read_table(first).num_rows
            n_pandas = len(pd.read_parquet(first))
            codecs = {pq.ParquetFile(first).metadata.row_group(0).column(c).compression.lower()
                      for c in range(pq.ParquetFile(first).metadata.num_columns)}
            log.info("%s: spark %d rows; first file %s: pyarrow %d rows, pandas %d rows, codec %s",
                     name, n, first.name, n_arrow, n_pandas, ",".join(sorted(codecs)))
            if n_arrow != n_pandas:
                errors.append(f"{name}: pyarrow {n_arrow} rows, pandas {n_pandas}")
            if name in CODEC_TABLES and codecs != {codec}:
                errors.append(f"{name}: codec {codecs}, expected {codec}")
        except Exception as exc:  # noqa: BLE001 - report every table, then fail
            errors.append(f"{name}: {type(exc).__name__}: {exc}")

    row = {f.name: None for f in FINAL_SCHEMA}
    path = write_final([row], Path(args.work_dir) / "codec_read_check", "check", codec)
    written = pq.ParquetFile(path).metadata.row_group(0).column(0).compression.lower()
    log.info("pyarrow writer (scorer.write_final): %s, codec %s, %d rows back", path.name, written,
             len(pd.read_parquet(path)))
    if written != codec:
        errors.append(f"scorer.write_final wrote {written}, expected {codec}")
    spark.stop()

    for e in errors:
        log.error("%s", e)
    if errors:
        sys.exit(1)
    log.info("all readers OK with codec %s", codec)


if __name__ == "__main__":
    main()
