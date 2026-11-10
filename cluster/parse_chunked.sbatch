#!/bin/bash
# Parse one full Lichess month from its .pgn.zst dump in ~20 GB chunks, then run the quality checks.
# Submit from the repo root once cluster/download.sbatch has finished (or chain the two):
#     sbatch cluster/parse_chunked.sbatch 2026-10
#     sbatch --dependency=afterok:<download job id> cluster/parse_chunked.sbatch 2026-10
# Optional environment variables (sbatch passes them to the job):
#     ZST       another .pgn.zst instead of input.zst_pattern, e.g. a compressed sample for a test
#     CHUNK_GB  chunk size in GB of PGN instead of input.chunk_gb
#     CONFIG    default config/cluster.yaml
# Scratch: the PGN chunks and Spark's spill/shuffle files go to /scratch/$USER/$SLURM_JOB_ID, the
# node's local disk, much faster than the network home folder. /scratch is local to each node: rust
# and the other nodes can't see it, so only temporary files go there. The folder is deleted when the
# job ends, also on failure, scancel or time limit (trap). The lake itself is written to home.
# Output: logs/parse-chunked-<job id>.out (with peak /scratch use), step logs in logs/,
#         DQ results in logs/dq_parquet-<job id>.json
#SBATCH --job-name=parse-chunked
#SBATCH --partition=cpu
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
# 256G: the parser's 64 Python workers live outside the JVM heap. Job 28202 (128G, heap 75% = 96G,
# 32G left) slowed down at the memory limit and lost a Python worker in chunk 2; job 28226 (256G,
# heap 75% = 192G) finished with MaxRSS 203 GiB. The parse step now gets 50% (128G heap, 128G for
# the Python workers) and the DataFrame-only checks 75%; see below.
#SBATCH --mem=256G
#SBATCH --time=3-00:00:00
#SBATCH --signal=B:TERM@300
#SBATCH --output=logs/%x-%j.out

set -euo pipefail
MONTH="${1:?usage: sbatch cluster/parse_chunked.sbatch <month, e.g. 2026-10>}"
CONFIG="${CONFIG:-config/cluster.yaml}"

cd "$SLURM_SUBMIT_DIR"  # the repo root
source cluster/env.sh
export PYTHONPATH=.

WORK="/scratch/$USER/$SLURM_JOB_ID"
export SPARK_LOCAL_DIRS="$WORK/spark"  # replaces env.sh's ~/chess/spark-tmp for this job
mkdir -p "$SPARK_LOCAL_DIRS"
PEAK_FILE="$WORK/peak_bytes"
CHILD=""

# Every 30 s: size of $WORK; the largest value so far goes to $PEAK_FILE
monitor_scratch() {
    local peak=0 now
    while sleep 30; do
        now="$(du -sb "$WORK" 2>/dev/null | cut -f1 || true)"
        if [[ -n "$now" ]] && (( now > peak )); then
            peak=$now
            echo "$peak" > "$PEAK_FILE"
        fi
    done
}

# Runs on every exit (success, failure, TERM): report peak scratch use, then delete $WORK
cleanup() {
    local status=$?
    if [[ -n "${MONITOR:-}" ]]; then kill "$MONITOR" 2>/dev/null || true; fi
    if [[ -f "$PEAK_FILE" ]]; then
        echo "peak /scratch use: $(numfmt --to=iec --suffix=B "$(cat "$PEAK_FILE")") ($(cat "$PEAK_FILE") bytes, sampled every 30 s)"
    fi
    rm -rf "$WORK"
    echo "removed $WORK  exit_status=$status  end=$(date -u +%FT%TZ)"
}
trap cleanup EXIT
# scancel or the time limit (--signal=B:TERM@300): stop the running step, then exit (runs cleanup)
trap 'echo "received TERM/INT"; if [[ -n "$CHILD" ]]; then kill -TERM "$CHILD" 2>/dev/null || true; wait "$CHILD" || true; fi; exit 143' TERM INT

# Run a step in the background and wait for it, so a TERM is handled right away
run() {
    "$@" &
    CHILD=$!
    wait "$CHILD"
    CHILD=""
}

monitor_scratch &
MONITOR=$!

# local[N] with N = Slurm CPUs. Driver memory (JVM heap) as a share of --mem (SLURM_MEM_PER_NODE is in MB):
# 50% for the Python-heavy parse, 75% for the DataFrame-only checks
CPUS="$SLURM_CPUS_PER_TASK"
DRIVER_MEM_PYTHON="$(( SLURM_MEM_PER_NODE / 2 ))m"
DRIVER_MEM="$(( SLURM_MEM_PER_NODE * 3 / 4 ))m"
SPARK_SUBMIT_PYTHON=(spark-submit --master "local[$CPUS]" --driver-memory "$DRIVER_MEM_PYTHON")
SPARK_SUBMIT=(spark-submit --master "local[$CPUS]" --driver-memory "$DRIVER_MEM")

PARSE_ARGS=(--config "$CONFIG" --months "$MONTH" --work-dir "$WORK")
if [[ -n "${ZST:-}" ]]; then PARSE_ARGS+=(--zst "$ZST"); fi
if [[ -n "${CHUNK_GB:-}" ]]; then PARSE_ARGS+=(--chunk-gb "$CHUNK_GB"); fi

echo "job=$SLURM_JOB_ID node=$(hostname) cpus=$CPUS driver_memory=$DRIVER_MEM_PYTHON (parse) / $DRIVER_MEM (checks) month=$MONTH config=$CONFIG"
echo "zst=${ZST:-<from config>} chunk_gb=${CHUNK_GB:-<from config>} work=$WORK start=$(date -u +%FT%TZ)"
df -h /scratch | tail -1

echo "===== parse_chunked ====="
t0=$SECONDS
run "${SPARK_SUBMIT_PYTHON[@]}" src/ingestion/parse_chunked.py "${PARSE_ARGS[@]}"
echo "parse_chunked took $((SECONDS - t0)) s"

echo "===== quality checks ====="
t0=$SECONDS
run "${SPARK_SUBMIT[@]}" src/quality/checks.py --config "$CONFIG" --months "$MONTH" \
    --output "logs/dq_parquet-$SLURM_JOB_ID.json"
echo "checks took $((SECONDS - t0)) s"

du -sh "$(python -c "from src.common.config import load_config; print(load_config('$CONFIG')['paths']['lake_dir'])")"
