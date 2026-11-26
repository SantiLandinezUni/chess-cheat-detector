"""Cluster cost per pipeline step from Slurm accounting (feedback item 2.1).

Runs `sacct` for the current user and sums, per pipeline step (and month where the job had one):
  - core_hours          = allocated CPUs x elapsed time (what the allocation costs)
  - cpu_used_hours      = TotalCPU, the CPU time the processes actually used
  - mem_alloc_gb_hours  = requested memory (GB) x elapsed time
  - peak_rss_gb         = largest MaxRSS of any step of the step's jobs
Jobs of other projects (working directory or submit line under ~/crimewatch) are left out, as are
jobs that have not finished yet. Only the current user's jobs are visible to sacct: jobs a teammate
ran from their own account are not included.

sacct reads Slurm's database, not the lake, so this is fine on the login node:
    python scripts/cluster_usage.py --out docs/validation_results/cluster_usage.csv
"""

import argparse
import csv
import logging
import re
import subprocess
from collections import defaultdict
from dataclasses import dataclass, field

log = logging.getLogger("cluster_usage")
FIELDS = ["JobID", "JobName", "State", "ElapsedRaw", "AllocCPUS", "ReqMem", "TotalCPU", "MaxRSS", "WorkDir",
          "SubmitLine"]
EXCLUDE = ("crimewatch",)  # another course project on the same account
UNFINISHED = ("PENDING", "RUNNING", "REQUEUED", "SUSPENDED")
MONTH = re.compile(r"\b(20\d\d-\d\d)(?![-\d])")  # 2026-10, not --begin=2026-11-24
# (job name regex, step) in order; the first match wins
STEPS = [
    (r"^download$", "download (dump to home)"),
    (r"^parse-chunked$", "parse (full month)"),
    (r"^parse-scaling-report$", "parse scaling experiment"),
    (r"^parse-scaling-\d+$", "parse scaling experiment"),
    (r"^(parse|parse-chunked-test|parse-chunked-cancel-test|parse-refactor-test|mem-rule-.*)$", "parse (tests)"),
    (r"^features$", "features"),
    (r"^baselines-prov$", "baselines (provisional)"),
    (r"^baselines-month$", "baselines (per month, feedback 3.3)"),
    (r"^(scorer-.*|compare-scores|smoke-rebalance)$", "scorer runs"),
    (r"^(kafka.*|smoke-timelimit)$", "kafka streaming tests"),
    (r"^serving-v2$", "serving_v2 replay (full day 2026-10-22)"),
    (r"^serving.*$", "serving"),
    (r"^labels$", "labels"),
    (r"^partition-sizes$", "partition sizes (feedback 3.2)"),
    (r"^(compact-lake|compaction-.*|zstd-test)$", "lake compaction (feedback 3.2)"),
    (r"^(labels-zip|zip-exports)$", "exports (packaging)"),
]
COLUMNS = ["step", "month", "jobs", "jobs_failed_or_cancelled", "elapsed_h", "core_hours", "cpu_used_hours",
           "cpu_efficiency", "mem_alloc_gb_hours", "peak_rss_gb", "job_ids"]


@dataclass
class Usage:
    """Running totals for one (step, month)."""
    job_ids: list[str] = field(default_factory=list)
    failed: int = 0
    elapsed_s: float = 0.0
    core_s: float = 0.0
    cpu_used_s: float = 0.0
    mem_gb_s: float = 0.0
    peak_rss_gb: float = 0.0


def to_seconds(t: str) -> float:
    """sacct durations: [DD-][HH:]MM:SS[.mmm] -> seconds."""
    if not t:
        return 0.0
    days, _, rest = t.rpartition("-")
    parts = [float(p) for p in rest.split(":")]
    while len(parts) < 3:
        parts.insert(0, 0.0)
    return (int(days) if days else 0) * 86400 + parts[0] * 3600 + parts[1] * 60 + parts[2]


def to_gb(mem: str) -> float:
    """sacct sizes such as 256G, 512M, 1234K, 1.5T (ReqMem may end in n or c) -> GB."""
    m = re.match(r"^([\d.]+)([KMGT]?)", mem or "")
    if not m:
        return 0.0
    return float(m.group(1)) * {"": 1 / 1024 ** 3, "K": 1 / 1024 ** 2, "M": 1 / 1024, "G": 1, "T": 1024}[m.group(2)]


def classify(name: str, submit: str) -> str:
    """Pipeline step of a job, from its name (and submit line for the generic srun names)."""
    for pattern, step in STEPS:
        if re.match(pattern, name):
            return step
    if "probe_lichess" in submit or "probe_auth" in submit:
        return "labels (API probes)"
    if "parse_scaling" in submit:
        return "parse scaling experiment"
    if "/compaction/" in submit:
        return "lake compaction (feedback 3.2)"
    return "setup and checks"


def sacct(start: str) -> list[dict]:
    """All job and step records of the current user since `start`."""
    out = subprocess.run(["sacct", "-S", start, "-P", "--noheader", "--format=" + ",".join(FIELDS)],
                         check=True, capture_output=True, text=True).stdout
    records = []
    for line in out.splitlines():
        values = line.split("|")
        if len(values) < len(FIELDS):  # a submit line with a newline in it: continuation of the previous record
            continue
        # SubmitLine may itself contain "|": rejoin the tail
        rec = dict(zip(FIELDS[:-1], values[:len(FIELDS) - 1]))
        rec["SubmitLine"] = "|".join(values[len(FIELDS) - 1:])
        records.append(rec)
    return records


def usage(records: list[dict]) -> dict[tuple[str, str], Usage]:
    """Sum the job-level records per (step, month); MaxRSS comes from the step records."""
    peak: dict[str, float] = defaultdict(float)
    for r in records:
        if "." in r["JobID"]:  # step record, e.g. 28226.batch
            job = r["JobID"].split(".")[0]
            peak[job] = max(peak[job], to_gb(r["MaxRSS"]))

    totals: dict[tuple[str, str], Usage] = defaultdict(Usage)
    skipped = 0
    for r in records:
        if "." in r["JobID"] or any(x in r["WorkDir"] + r["SubmitLine"] for x in EXCLUDE):
            continue
        if r["State"].split()[0] in UNFINISHED:
            skipped += 1
            continue
        elapsed = float(r["ElapsedRaw"] or 0)
        if elapsed == 0 and r["State"].startswith("CANCELLED"):
            continue  # cancelled while pending: used nothing
        month = MONTH.search(r["SubmitLine"])
        u = totals[(classify(r["JobName"], r["SubmitLine"]), month.group(1) if month else "")]
        u.job_ids.append(r["JobID"])
        u.failed += not r["State"].startswith("COMPLETED")
        u.elapsed_s += elapsed
        u.core_s += elapsed * int(r["AllocCPUS"] or 0)
        u.cpu_used_s += to_seconds(r["TotalCPU"])
        u.mem_gb_s += elapsed * to_gb(r["ReqMem"])
        u.peak_rss_gb = max(u.peak_rss_gb, peak[r["JobID"]])
    log.info("left out %d unfinished jobs", skipped)
    return totals


def rows(totals: dict[tuple[str, str], Usage]) -> list[dict]:
    """CSV rows, sorted by step and month, plus a TOTAL row."""
    def row(step: str, month: str, u: Usage) -> dict:
        return {"step": step, "month": month, "jobs": len(u.job_ids), "jobs_failed_or_cancelled": u.failed,
                "elapsed_h": round(u.elapsed_s / 3600, 2), "core_hours": round(u.core_s / 3600, 1),
                "cpu_used_hours": round(u.cpu_used_s / 3600, 1),
                "cpu_efficiency": round(u.cpu_used_s / u.core_s, 3) if u.core_s else "",
                "mem_alloc_gb_hours": round(u.mem_gb_s / 3600, 1), "peak_rss_gb": round(u.peak_rss_gb, 1),
                "job_ids": " ".join(u.job_ids)}

    out = [row(s, m, u) for (s, m), u in sorted(totals.items())]
    total = Usage()
    for u in totals.values():
        total.job_ids += u.job_ids
        total.failed += u.failed
        total.elapsed_s += u.elapsed_s
        total.core_s += u.core_s
        total.cpu_used_s += u.cpu_used_s
        total.mem_gb_s += u.mem_gb_s
        total.peak_rss_gb = max(total.peak_rss_gb, u.peak_rss_gb)
    t = row("TOTAL", "", total)
    t["job_ids"] = ""
    return out + [t]


def main() -> None:
    parser = argparse.ArgumentParser(description="Core-hours and memory-hours per pipeline step from sacct")
    parser.add_argument("--out", required=True, help="CSV to write")
    parser.add_argument("--start", default="2026-09-01", help="first day to include (sacct -S)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    result = rows(usage(sacct(args.start)))
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(result)
    log.info("written %d rows to %s", len(result), args.out)


if __name__ == "__main__":
    main()
