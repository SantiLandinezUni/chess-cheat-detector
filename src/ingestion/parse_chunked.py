"""Parse one full month from its .pgn.zst dump in chunks, inside one Slurm job (Workstream A).

Run with cluster/parse_chunked.sbatch, which passes the Spark settings and a node-local work dir:
    PYTHONPATH=. spark-submit --master local[64] --driver-memory 98304m src/ingestion/parse_chunked.py \
        --config config/cluster.yaml --months 2026-10 --work-dir /scratch/$USER/$SLURM_JOB_ID

A month is about 210 GB of PGN, too much to decompress at once. `zstd -dc` streams the dump and
split_pgn writes it to --work-dir in chunks of about input.chunk_gb GB, each cut at a game
boundary (the parser's record delimiter "\\n\\n[Event "), so every chunk starts with a complete
game. Each chunk is parsed with parse_games' code, appended to a staging area in the lake and
deleted before the next one is decompressed (zstd waits meanwhile).

After the last chunk, the month's games, moves and quarantine partitions are swapped in from
staging, identity_by_month is deduplicated across chunks, and restricted/identity is rebuilt once.
A run that fails leaves the lake's previous data for the month untouched; a rerun starts over.
Identity staging stays under restricted/ (it holds usernames).
"""

import shutil
import subprocess
import time
from collections import Counter
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO

from pyspark.sql import functions as F

from src.common.config import build_arg_parser, expand_path, load_config
from src.common.ids import load_hash_key
from src.common.logging_setup import log_duration, setup_logging
from src.common.spark import get_spark
from src.ingestion.parse_games import parse_and_write, rebuild_identity, write_parquet

STEP = "parse_chunked"
SEPARATOR = b"\n\n[Event "  # same as parse_games.RECORD_DELIMITER
READ_BYTES = 64 * 1024 * 1024
TABLES = ("games", "moves", "quarantine")


def split_pgn(stream: BinaryIO, out_dir: Path, chunk_bytes: int) -> Iterator[Path]:
    """Write the PGN stream to out_dir/chunk_NNNN.pgn files and yield each one when it is complete.

    A chunk is cut at the first game boundary after chunk_bytes: it keeps the blank line, and the
    next chunk starts with "[Event ". The stream isn't read while the caller handles a chunk.
    """
    idx, written, carry = 0, 0, b""
    path = out_dir / f"chunk_{idx:04d}.pgn"
    out = path.open("wb")
    while True:
        block = stream.read(READ_BYTES)
        data, carry = carry + block, b""
        while block and written + len(data) >= chunk_bytes:
            cut = data.find(SEPARATOR, max(0, chunk_bytes - written - len(SEPARATOR)))
            if cut == -1:
                break  # no boundary in this block yet
            cut += 2  # the blank line stays with this chunk
            out.write(data[:cut])
            out.close()
            yield path
            idx, written, data = idx + 1, 0, data[cut:]
            path = out_dir / f"chunk_{idx:04d}.pgn"
            out = path.open("wb")
        if not block:  # end of stream
            out.write(data)
            written += len(data)
            break
        keep = min(len(data), len(SEPARATOR) - 1)  # a separator can straddle two reads
        out.write(data[:len(data) - keep])
        written += len(data) - keep
        carry = data[len(data) - keep:]
    out.close()
    if written:
        yield path
    else:
        path.unlink()


def swap_month(staged_table: Path, lake_table: Path, month: str) -> None:
    """Replace the month's partition in the lake table with the staged one (none staged = no rows)."""
    src, dst = staged_table / f"source_month={month}", lake_table / f"source_month={month}"
    lake_table.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        shutil.rmtree(dst)
    if src.exists():
        src.rename(dst)


def main() -> None:
    parser = build_arg_parser("Parse one month's .pgn.zst dump in chunks into the Parquet lake")
    parser.add_argument("--work-dir", required=True, help="fast local folder for the chunks, e.g. /scratch/$USER/$SLURM_JOB_ID")
    parser.add_argument("--zst", help="input .pgn.zst; replaces input.zst_pattern (e.g. a compressed sample for tests)")
    parser.add_argument("--chunk-gb", type=float, help="chunk size in GB of PGN; replaces input.chunk_gb")
    args = parser.parse_args()
    cfg = load_config(args.config, args.months)
    if len(cfg["months"]) != 1:
        parser.error(f"exactly one month per run, got {cfg['months']}")
    month = cfg["months"][0]
    zst = Path(args.zst or expand_path(cfg, cfg["input"]["zst_pattern"]).format(month=month))
    chunk_bytes = int((args.chunk_gb or cfg["input"]["chunk_gb"]) * 1e9)
    log = setup_logging(STEP, cfg["paths"]["log_dir"])
    lake = Path(cfg["paths"]["lake_dir"])
    chunk_dir = Path(args.work_dir) / "chunks"
    chunk_dir.mkdir(parents=True, exist_ok=True)

    # Staging for this month; leftovers of a failed run are removed first
    stage, stage_identity = lake / "_staging" / month, lake / "restricted" / "_staging" / month
    for d in (stage, stage_identity):
        shutil.rmtree(d, ignore_errors=True)
    outputs = {t: str(stage / t) for t in TABLES}
    outputs["identity_by_month"] = str(stage_identity / "identity_by_month")

    key = load_hash_key()
    run_ts = datetime.now(timezone.utc)
    log.info("month=%s input=%s (%.1f GB) chunk_gb=%.2f work_dir=%s lake=%s",
             month, zst, zst.stat().st_size / 1e9, chunk_bytes / 1e9, args.work_dir, lake)

    spark = get_spark(cfg, STEP)
    n_files = spark.sparkContext.defaultParallelism  # = N cores: fewer, larger files per chunk
    totals: Counter[str] = Counter()
    with log_duration(log, STEP):
        proc = subprocess.Popen(["zstd", "-dc", str(zst)], stdout=subprocess.PIPE)
        try:
            n_chunks, t_split = 0, time.monotonic()
            for n_chunks, chunk in enumerate(split_pgn(proc.stdout, chunk_dir, chunk_bytes), start=1):
                log.info("chunk %d: %.2f GB decompressed in %.0f s", n_chunks,
                         chunk.stat().st_size / 1e9, time.monotonic() - t_split)
                with log_duration(log, f"chunk {n_chunks}"):
                    counts = parse_and_write(spark, [(month, str(chunk))], key, run_ts, outputs, log,
                                             mode="append", n_files=n_files)
                chunk.unlink()
                totals.update(counts)
                log.info("after chunk %d: records=%d games=%d moves=%d quarantined=%d", n_chunks,
                         totals["records"], totals["games"], totals["moves"], totals["quarantine"])
                t_split = time.monotonic()
        finally:
            if proc.poll() is None:
                proc.kill()
        if proc.wait() != 0:
            raise RuntimeError(f"zstd -dc {zst} failed with exit code {proc.returncode}")
        if n_chunks == 0:
            raise RuntimeError(f"{zst} decompressed to nothing; the lake was not changed")

        with log_duration(log, "write restricted/identity_by_month (deduplicated across chunks)"):
            by_month = (spark.read.parquet(outputs["identity_by_month"])
                        .groupBy("source_month", "player_id")
                        .agg(F.min("username").alias("username"))
                        .select("player_id", "username", "source_month"))
            write_parquet(by_month, str(lake / "restricted" / "identity_by_month"), ["source_month"])

        with log_duration(log, f"swap {month} into games, moves, quarantine"):
            for t in TABLES:
                swap_month(stage / t, lake / t, month)

        rebuild_identity(spark, str(lake), log)
        for d in (stage, stage_identity):
            shutil.rmtree(d)
            try:
                d.parent.rmdir()  # the _staging folder, if no other month is staged
            except OSError:
                pass
        log.info("month=%s chunks=%d records=%d games=%d moves=%d quarantined=%d", month, n_chunks,
                 totals["records"], totals["games"], totals["moves"], totals["quarantine"])
    spark.stop()


if __name__ == "__main__":
    main()
