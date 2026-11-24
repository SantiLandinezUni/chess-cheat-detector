# Partition skew in lake/games and lake/moves: measurement and proposed fix (feedback 3.2)

Status: **proposal only. The lake has not been rewritten.** Compaction waits for approval.

## Measurement

`src/quality/partition_sizes.py` (job 37602, `cluster/partition_sizes.sbatch`) lists every Parquet file of
`lake/games` and `lake/moves` and reads only the footers (row counts). Result:
[`validation_results/partition_sizes_before.csv`](validation_results/partition_sizes_before.csv), one row
per `source_month` × `time_class`.

| | games | moves |
|---|---|---|
| partitions | 18 | 18 |
| files | 12,882 | 12,882 |
| total size | 11.4 GB | 117.0 GB |
| largest partition | 2026-10 blitz, 1,879 MB | 2026-08 blitz, 20,517 MB |
| smallest partition | 2026-09 correspondence, 11.5 MB | 2026-10 correspondence, 23.9 MB |
| largest file | 3.1 MB | 34.4 MB |
| files under 32 MB | 12,882 (100%) | 12,497 (97%) |

**The problem is the file count, not uneven files inside a partition.** Every partition of a month has the
same number of files (704 for 2026-08 and 2026-09, 739 for 2026-10), whatever its size. Within the large
partitions (blitz, bullet, rapid), file sizes are even: median ≈ mean, max ≤ 1.4 × mean.
So moves/blitz files average 28 MB, while games/correspondence files average 20 KB, which is 704 files for
12 MB of data.

**Cause:** `src/ingestion/parse_chunked.py` parses a month in about 11 chunks of 20 GB and appends each
chunk with `n_files = defaultParallelism` (64). `coalesce(64)` runs before `partitionBy(source_month,
time_class)`, so each of the 64 tasks writes one file into every time class it saw: 64 files × ~11 chunks
per partition, for every time class alike.

**Why it matters:** Spark already packs small files into ~128 MB read splits, so the scan task count is not
the main cost. The costs are per file: listing, opening and reading a footer on the shared network home
(the footer-only scan of 25,764 files took 5 minutes), file-system metadata and quota, and slow reads for
anything outside Spark (pyarrow/pandas in notebooks, the replay producer).

## Proposed fix

### 1. Target file size: 128 MB

128 MB matches `spark.sql.files.maxPartitionBytes` (one read task per file) and the Parquet row-group
size. 256 MB would halve the file count again, but it would leave moves/rapid with only 29 files per
month, fewer than the 64 cores of a typical job.

### 2. Files per partition

`n_files(partition) = max(1, ceil(partition_bytes / 128 MB))`, from the current on-disk size (this CSV's
`total_mb`, or the staged size during a parse). Snappy-compressed size is the right measure because
that is what ends up on disk; the ratio after rewriting stays about the same.

For 2026-08 this gives:

| time_class | games: files now → after | moves: files now → after |
|---|---|---|
| blitz | 704 → 15 | 704 → 161 |
| bullet | 704 → 10 | 704 → 101 |
| rapid | 704 → 7 | 704 → 58 |
| classical | 704 → 1 | 704 → 3 |
| ultrabullet | 704 → 1 | 704 → 3 |
| correspondence | 704 → 1 | 704 → 1 |

Whole lake: **25,764 → 1,048 files** (537 at 256 MB).

How to write it: one write per (table, month, time class), `df.repartition(n, "game_id")
.sortWithinPartitions("game_id", "ply")` (games: `"game_id"` only). Hashing on `game_id` keeps every
game's moves in one file, and the sort keeps them in ply order, which helps compression and the replay
producer. `repartition` rather than `coalesce` gives even file sizes.

### 3. Compact the existing lake on a copy, then swap

A new step, `src/quality/compact_lake.py` with `cluster/compact_lake.sbatch` (DataFrame-only, 75% heap,
`/scratch` for shuffle). It takes `--months`, `--tables` and `--target-mb`:

1. **Copy:** for each (table, month, time class), read the partition, compute `n_files`, and write to
   `lake/_compacted/{table}/source_month=…/time_class=…`. Nothing under `lake/games` or `lake/moves`
   is touched. Extra space needed: about 128 GB on home while both copies exist. **Check the home
   quota or storage location with the admins first.**
2. **Verify, per partition, old vs copy:** row count; `count(distinct game_id)`; a checksum
   `sum(xxhash64(game_id, ply, move_time_s, …))` over all columns; and an identical schema. Then rerun
   `partition_sizes.py` on the copy, which becomes `partition_sizes_after.csv`. Any mismatch stops the job
   before the swap.
3. **Swap, one table at a time:** `mv lake/moves lake/_old/moves_<UTC stamp>`, then
   `mv lake/_compacted/moves lake/moves`. These are directory renames on the same file system, so they
   are instant and no data is copied. No job may read the lake during the swap (check `squeue`; the
   labels job reads `restricted/`, not games or moves).
4. **Check downstream:** rerun `src/quality/checks.py` and the provisional baselines on the swapped lake
   and compare with the current numbers. Only then delete `lake/_old/`.
   Rollback = the two `mv` commands reversed.

### 4. Stop it from coming back

In `parse_chunked.py`, compact the staged month before `swap_month`, with the same code as step 3. It
reads ~43 GB of staged Parquet per month, minutes next to the ~2 h parse. After that, the per-chunk
`n_files` only affects staging. `parse_games.py` (single input, `local[N]`) has the same pattern at
smaller scale and can use the same function.

## Decision needed

- Approve the 128 MB target and the copy → verify → swap plan.
- Confirm there is ~128 GB of free space on home (or another location) for the temporary copy.
