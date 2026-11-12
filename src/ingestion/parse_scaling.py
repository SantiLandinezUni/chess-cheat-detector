"""Strong-scaling test of the parser (Workstream A): parse one fixed chunk of a month with N cores.

Run with cluster/parse_scaling.sbatch, once per core count (same input, same memory per core):
    PYTHONPATH=. spark-submit --master local[16] --driver-memory 24576m src/ingestion/parse_scaling.py \
        --config config/cluster.yaml --months 2026-10 --work-dir /scratch/$USER/$SLURM_JOB_ID \
        --result-out logs/parse_scaling/parse_scaling-$SLURM_JOB_ID.json

The input is the month's first chunk exactly as parse_chunked cuts it (`zstd -dc` of the dump, cut
at the first game boundary after --chunk-gb GB, default input.chunk_gb), so every run parses the
same games. The parse is parse_games.parse_and_write, the code parse_chunked runs per chunk.
Everything it writes (games, moves, quarantine and identity_by_month, which holds usernames) goes
to <work-dir>/out on node-local scratch, which the sbatch deletes at the end; the step refuses
to write inside the lake. The result JSON (counts and timings only) feeds parse_scaling_report.py.
"""

import json
import os
import socket
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from src.common.config import REPO_ROOT, build_arg_parser, expand_path, load_config
from src.common.ids import load_hash_key
from src.common.logging_setup import log_duration, setup_logging
from src.common.spark import get_spark
from src.ingestion.parse_chunked import split_pgn
from src.ingestion.parse_games import parse_and_write

STEP = "parse_scaling"


def extract_first_chunk(zst: Path, chunk_dir: Path, chunk_bytes: int) -> Path:
    """Decompress only the dump's first chunk (cut as in parse_chunked), then stop zstd."""
    proc = subprocess.Popen(["zstd", "-dc", str(zst)], stdout=subprocess.PIPE)
    chunks = split_pgn(proc.stdout, chunk_dir, chunk_bytes)
    try:
        return next(chunks)  # complete (closed) when yielded
    finally:
        chunks.close()
        proc.kill()
        proc.wait()


def check_outside_lake(out_dir: Path, cfg: dict) -> None:
    """Refuse to write the test output into the real lake (or anywhere under data/)."""
    out = out_dir.resolve()
    for protected in (Path(cfg["paths"]["lake_dir"]), Path(expand_path(cfg, cfg["paths"]["data_dir"])),
                      REPO_ROOT / "data"):
        if out == protected.resolve() or protected.resolve() in out.parents:
            raise SystemExit(f"--work-dir output {out} is inside {protected}: use node-local scratch")


def main() -> None:
    parser = build_arg_parser("Parse one fixed chunk of a month, for the strong-scaling test")
    parser.add_argument("--work-dir", required=True, help="node-local scratch folder, e.g. /scratch/$USER/$SLURM_JOB_ID")
    parser.add_argument("--zst", help="input .pgn.zst; replaces input.zst_pattern")
    parser.add_argument("--chunk-gb", type=float, help="chunk size in GB of PGN; replaces input.chunk_gb")
    parser.add_argument("--result-out", required=True, help="JSON file for the counts and timings")
    args = parser.parse_args()
    cfg = load_config(args.config, args.months)
    if len(cfg["months"]) != 1:
        parser.error(f"exactly one month per run, got {cfg['months']}")
    month = cfg["months"][0]
    zst = Path(args.zst or expand_path(cfg, cfg["input"]["zst_pattern"]).format(month=month))
    chunk_gb = args.chunk_gb or cfg["input"]["chunk_gb"]
    work = Path(args.work_dir)
    out = work / "out"
    check_outside_lake(out, cfg)
    log = setup_logging(STEP, cfg["paths"]["log_dir"])
    chunk_dir = work / "chunks"
    chunk_dir.mkdir(parents=True, exist_ok=True)

    log.info("month=%s input=%s chunk_gb=%.2f work_dir=%s (test output, deleted at the end)",
             month, zst, chunk_gb, work)
    t0 = time.monotonic()
    chunk = extract_first_chunk(zst, chunk_dir, int(chunk_gb * 1e9))
    extract_s = time.monotonic() - t0
    log.info("first chunk: %.2f GB in %.0f s", chunk.stat().st_size / 1e9, extract_s)

    spark = get_spark(cfg, STEP, python_heavy=True)
    cores = spark.sparkContext.defaultParallelism
    outputs = {t: str(out / t) for t in ("games", "moves", "quarantine")}
    outputs["identity_by_month"] = str(out / "restricted" / "identity_by_month")
    with log_duration(log, f"parse the chunk with {cores} cores"):
        t0 = time.monotonic()
        counts = parse_and_write(spark, [(month, str(chunk))], load_hash_key(), datetime.now(timezone.utc),
                                 outputs, log, mode="append", n_files=cores)
        parse_s = time.monotonic() - t0
    spark.stop()

    result = {"job_id": os.getenv("SLURM_JOB_ID"), "node": socket.gethostname().split(".")[0],
              "cpus": cores, "mem_mb": int(os.getenv("SLURM_MEM_PER_NODE", "0")) or None,
              "month": month, "chunk_bytes": chunk.stat().st_size, "records": counts["records"],
              "games": counts["games"], "moves": counts["moves"], "quarantined": counts["quarantine"],
              "extract_s": round(extract_s, 1), "parse_s": round(parse_s, 1),
              "games_per_s": round(counts["games"] / parse_s, 1)}
    log.info("result: %s", json.dumps(result))
    Path(args.result_out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.result_out).write_text(json.dumps(result, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
