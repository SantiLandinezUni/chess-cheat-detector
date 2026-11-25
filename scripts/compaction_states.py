"""Merge the zstd re-compaction run into partition_compaction.csv as a third state, for the report.

Input:
  --base  docs/validation_results/partition_compaction.csv: first compaction (job 37612), columns
          before/after (or already merged: before/compacted/compacted_zstd)
  --run   CSV of the zstd re-compaction (compact_lake.py --out), columns before/after where
          before = compacted (snappy), after = compacted with zstd
Output (--out, may equal --base): one row per (section, table, source_month, time_class, item) with
  before          original lake (~700 small files per partition, snappy)
  compacted       after the first compaction (~128 MB target, snappy)
  compacted_zstd  after the re-compaction with zstd
plus % changes. Query rows: the "compacted" timings are those of job 37612; the zstd run re-timed
the compacted lake just before its swap, and that re-measurement is in the note, so before/after
of the zstd step can also be compared within one job.
Space rows: du of games+moves in each state (apparent bytes; allocated where measured).
"""

import argparse
import csv

KEY = ("section", "table", "source_month", "time_class", "item")
COLUMNS = [*KEY, "before", "compacted", "compacted_zstd", "compacted_vs_before_pct", "zstd_vs_compacted_pct",
           "zstd_vs_before_pct", "note"]


def num(v: str) -> float | None:
    """Float value of a CSV cell, None if not numeric."""
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def pct(a: str, b: str) -> float | str:
    """% change from a to b, blank if either is not a nonzero number."""
    x, y = num(a), num(b)
    return round(100 * (y - x) / x, 1) if x and y is not None else ""


def read(path: str) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base", required=True)
    parser.add_argument("--run", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    base = read(args.base)
    for r in base:  # first-run layout: after = compacted
        if "after" in r:
            r["compacted"] = r.pop("after")
    run = {tuple(r[k] for k in KEY): r for r in read(args.run)}
    space_base = {r["item"]: r for r in base if r["section"] == "space"}
    space_run = {k[4]: r for k, r in run.items() if k[0] == "space"}

    out = []
    for r in base:
        if r["section"] == "space":
            continue
        k = tuple(r[c] for c in KEY)
        z = run.get(k, {})
        row = {c: r.get(c, "") for c in COLUMNS}
        row["compacted_zstd"] = z.get("after", "")
        notes = [n for n in (r.get("note", ""),) if n]
        if r["section"] == "query" and not r["item"].endswith("_result") and z:
            notes.append(f"compacted re-timed in the zstd job just before its swap: {z['before']} s")
        if r["item"] in ("rows", "checksum") or r["item"].endswith("_result"):
            same = r["before"] == r["compacted"] == row["compacted_zstd"]
            notes = ["same in all three states" if same else "DIFFERS"]
        elif r["item"] in ("file_count", "total_mb") and z and z.get("before") != r["compacted"]:
            notes.append(f"zstd job measured compacted as {z.get('before')}")
        row["note"] = "; ".join(notes)
        if r["item"] != "checksum" and not r["item"].endswith("_result"):
            row["compacted_vs_before_pct"] = pct(r["before"], r["compacted"])
            row["zstd_vs_compacted_pct"] = pct(r["compacted"], row["compacted_zstd"])
            row["zstd_vs_before_pct"] = pct(r["before"], row["compacted_zstd"])
        out.append(row)

    if "games_moves_gb_apparent" in space_base:  # already merged: keep as is
        out += [{c: r.get(c, "") for c in COLUMNS} for r in space_base.values()]
    else:
        orig = num(space_base["old_lake_deleted_gb_apparent"]["before"])
        comp = orig - num(space_base["net_freed_gb_apparent"]["before"])
        comp_run = num(space_run["old_lake_deleted_gb_apparent"]["before"])
        zstd = comp_run - num(space_run["net_freed_gb_apparent"]["before"])
        apparent = {"before": round(orig, 2), "compacted": round(comp_run, 2), "compacted_zstd": round(zstd, 2)}
        allocated = {"before": space_base["old_lake_deleted_gb_allocated"]["before"],
                     "compacted": space_run["old_lake_deleted_gb_allocated"]["before"], "compacted_zstd": ""}
        for item, vals, note in (
                ("games_moves_gb_apparent", apparent,
                 f"du -sb of lake/games + lake/moves; job 37612 measured compacted as {comp:.2f}"),
                ("games_moves_gb_allocated", allocated, "du -sk, on-disk size after ZFS compression")):
            row = {"section": "space", "table": "games+moves", "source_month": "", "time_class": "",
                   "item": item, **{k: str(v) for k, v in vals.items()}, "note": note}
            row["compacted_vs_before_pct"] = pct(row["before"], row["compacted"])
            row["zstd_vs_compacted_pct"] = pct(row["compacted"], row["compacted_zstd"])
            row["zstd_vs_before_pct"] = pct(row["before"], row["compacted_zstd"])
            out.append(row)

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(out)
    print(f"written {len(out)} rows to {args.out}")


if __name__ == "__main__":
    main()
