"""Right-sized Parquet files for the lake's partitioned tables (games, moves, quarantine).

Used by src/ingestion/parse_chunked.py (compacts a staged month before swapping it in) and by
src/quality/compact_lake.py (one-off compaction of the existing lake).

A partition gets n_files = max(1, ceil(bytes / target_mb)) files, from its current on-disk
(compressed) size. Each partition is written with repartition(n, "game_id") so that all rows of a
game land in one file, and sorted within the file (games by game_id, moves by game_id and ply).
"""

import logging
import math
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DEFAULT_TARGET_MB = 128
MB = 1024 * 1024
PARTITION_COLS = {"games": ["source_month", "time_class"], "moves": ["source_month", "time_class"],
                  "quarantine": ["source_month"]}
SORT_COLS = {"games": ["game_id"], "moves": ["game_id", "ply"], "quarantine": ["game_id"]}
log = logging.getLogger(__name__)

Partition = tuple[tuple[str, str], ...]  # (("source_month", "2026-10"), ("time_class", "blitz"))


@dataclass
class PartitionFiles:
    """Parquet files of one partition."""
    sizes: list[int] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.sizes)

    @property
    def bytes(self) -> int:
        return sum(self.sizes)


def target_file_mb(cfg: dict) -> float:
    """lake.target_file_mb from the config, default 128."""
    return float((cfg.get("lake") or {}).get("target_file_mb", DEFAULT_TARGET_MB))


def list_partitions(table_dir: Path) -> dict[Partition, PartitionFiles]:
    """Parquet files per Hive-style partition folder of a table (folders starting with _ or . skipped)."""
    out: dict[Partition, PartitionFiles] = {}
    if not table_dir.exists():
        return out
    for path in table_dir.rglob("*.parquet"):
        rel = path.relative_to(table_dir)
        if any(p.startswith(("_", ".")) for p in rel.parts):
            continue
        key = tuple(tuple(p.split("=", 1)) for p in rel.parts[:-1] if "=" in p)
        out.setdefault(key, PartitionFiles()).sizes.append(path.stat().st_size)
    return out


def n_files_for(n_bytes: int, target_mb: float) -> int:
    """Files a partition of n_bytes should have."""
    return max(1, math.ceil(n_bytes / (target_mb * MB)))


def partition_path(table_dir: Path, part: Partition) -> Path:
    """Folder of one partition."""
    return table_dir.joinpath(*(f"{k}={v}" for k, v in part))


def compact_table(spark: SparkSession, src_dir: Path, dst_dir: Path, table: str, target_mb: float,
                  logger: logging.Logger = log) -> dict[Partition, int]:
    """Rewrite every partition of src_dir into dst_dir with right-sized files; src_dir is not changed.

    Partitions are written one at a time with dynamic partition overwrite, so rerunning replaces them.
    Returns the number of files written per partition.
    """
    parts = list_partitions(src_dir)
    written: dict[Partition, int] = {}
    for part, files in sorted(parts.items()):
        n = n_files_for(files.bytes, target_mb)
        df = spark.read.option("basePath", str(src_dir)).parquet(str(partition_path(src_dir, part)))
        (df.repartition(n, "game_id").sortWithinPartitions(*SORT_COLS[table])
         .write.mode("overwrite").partitionBy(*PARTITION_COLS[table]).parquet(str(dst_dir)))
        written[part] = n
        logger.info("compacted %s %s: %d files, %.1f MB -> %d files",
                    table, dict(part), files.count, files.bytes / MB, n)
    return written


def partition_checksums(df: DataFrame, table: str) -> dict[Partition, tuple[int, int]]:
    """(rows, checksum) per partition; checksum = sum of xxhash64 over all data columns (wraps, order-free)."""
    pcols = PARTITION_COLS[table]
    data_cols = sorted(c for c in df.columns if c not in pcols)
    rows = (df.groupBy(*pcols)
            .agg(F.count(F.lit(1)).alias("n"), F.sum(F.xxhash64(*data_cols)).alias("h"))
            .collect())
    return {tuple((c, str(r[c])) for c in pcols): (r["n"], r["h"]) for r in rows}
