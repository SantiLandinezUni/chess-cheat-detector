"""Strong-scaling table (Workstream A): parse_scaling results + Slurm accounting -> CSV.

Run after the parse_scaling jobs, e.g. as cluster/parse_scaling_report.sbatch (afterany on them):
    python src/ingestion/parse_scaling_report.py --jobs 28601,28602,28603,28604 \
        --output docs/validation_results/parse_scaling.csv

For each job: parse time and counts from logs/parse_scaling/parse_scaling-<job id>.json, elapsed
time and MaxRSS from sacct. Speedup and efficiency are relative to the run with the fewest cores:
    speedup(N) = parse_s(N_min) / parse_s(N),   efficiency(N) = speedup(N) / (N / N_min)
(also on the job's elapsed time, which adds the chunk extraction and Spark start-up).
`--note JOB=TEXT` adds a free-text note to a job's row (e.g. why a run differs from the others).
"""

import argparse
import csv
import json
import logging
import subprocess
from pathlib import Path
from typing import Any

from src.common.config import REPO_ROOT

RESULTS_DIR = REPO_ROOT / "logs" / "parse_scaling"
COLUMNS = ["cpus", "job_id", "state", "node", "mem_gb", "mem_per_cpu_gb", "games", "moves", "chunk_gb",
           "elapsed_s", "extract_s", "parse_s", "games_per_s", "max_rss_gb",
           "speedup", "efficiency", "speedup_elapsed", "efficiency_elapsed", "note"]
UNITS = {"K": 1 / 1024**2, "M": 1 / 1024, "G": 1.0, "T": 1024.0}

log = logging.getLogger("parse_scaling_report")


def gib(value: str) -> float | None:
    """sacct memory value (e.g. 212926272K) in GiB."""
    if not value:
        return None
    if value[-1] in UNITS:
        return float(value[:-1]) * UNITS[value[-1]]
    return float(value) / 1024**3


def sacct(job_id: str) -> dict[str, Any]:
    """State, elapsed seconds, the largest MaxRSS over the job's steps, and (for runs without a
    result file, e.g. failed ones) the allocated CPUs, memory and node."""
    out = subprocess.run(["sacct", "-j", job_id, "-n", "-P",
                          "--format=JobID,State,ElapsedRaw,MaxRSS,AllocCPUS,ReqMem,NodeList"],
                         capture_output=True, text=True, check=True).stdout
    rows = [line.split("|") for line in out.splitlines() if line]
    main = next(r for r in rows if r[0] == job_id)
    rss = [gib(r[3]) for r in rows if r[3]]
    return {"state": main[1].split()[0], "elapsed_s": int(main[2]),
            "max_rss_gb": round(max(rss), 1) if rss else None,
            "cpus": int(main[4]), "mem_gb": round(gib(main[5]) or 0) or None, "node": main[6]}


def build_rows(job_ids: list[str], notes: dict[str, str]) -> list[dict[str, Any]]:
    """One row per job, sorted by cores, with speedup and efficiency against the smallest run."""
    rows = []
    for job_id in job_ids:
        row: dict[str, Any] = {"job_id": job_id} | sacct(job_id)
        result_file = RESULTS_DIR / f"parse_scaling-{job_id}.json"
        if result_file.exists():
            r = json.loads(result_file.read_text())
            row |= {k: r[k] for k in ("cpus", "node", "games", "moves", "extract_s", "parse_s", "games_per_s")}
            row["mem_gb"] = round(r["mem_mb"] / 1024) if r.get("mem_mb") else None
            row["chunk_gb"] = round(r["chunk_bytes"] / 1e9, 2)
        else:
            log.warning("job %s has no result file (state %s)", job_id, row["state"])
        if row.get("mem_gb") and row.get("cpus"):
            row["mem_per_cpu_gb"] = round(row["mem_gb"] / row["cpus"], 1)
        row["note"] = notes.get(job_id, "")
        rows.append(row)

    done = sorted((r for r in rows if r.get("parse_s")), key=lambda r: r["cpus"])
    if done:
        base = done[0]
        for r in done:
            scale = r["cpus"] / base["cpus"]
            r["speedup"] = round(base["parse_s"] / r["parse_s"], 2)
            r["efficiency"] = round(r["speedup"] / scale, 2)
            r["speedup_elapsed"] = round(base["elapsed_s"] / r["elapsed_s"], 2)
            r["efficiency_elapsed"] = round(r["speedup_elapsed"] / scale, 2)
    return sorted(rows, key=lambda r: (r.get("cpus") is None, r.get("cpus") or 0))


def main() -> None:
    parser = argparse.ArgumentParser(description="Strong-scaling table from parse_scaling jobs")
    parser.add_argument("--jobs", required=True, help="comma-separated Slurm job ids of the parse_scaling runs")
    parser.add_argument("--output", required=True, help="CSV to write")
    parser.add_argument("--note", action="append", default=[], metavar="JOB=TEXT",
                        help="note for one job's row; repeat for several jobs")
    args = parser.parse_args()
    notes = {}
    for item in args.note:
        job, sep, text = item.partition("=")
        if not sep:
            parser.error(f"--note must be JOB=TEXT, got {item!r}")
        notes[job.strip()] = text.strip()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    rows = build_rows([j.strip() for j in args.jobs.split(",") if j.strip()], notes)
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    for r in rows:
        log.info("%s", {k: r.get(k) for k in COLUMNS})
    log.info("written %s", args.output)


if __name__ == "__main__":
    main()
