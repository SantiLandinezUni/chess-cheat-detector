#!/bin/bash
# Parquet write settings tested on ONE partition (src/quality/compaction_tuning.py): current settings,
# zstd, games sorted by start time, both, and the smallest plus a larger dictionary page. Reads the
# partition from the lake; writes every variant to node-local /scratch only (removed on exit).
#     mkdir -p logs
#     sbatch cluster/compaction_tuning.sbatch [out.csv] [extra args, e.g. --month 2026-10 --time-class blitz]
# Default output: docs/validation_results/compaction_tuning.csv
# Optional environment variables: CONFIG (default config/cluster.yaml)
# DataFrame-only step, so the JVM heap gets 75% of --mem. Five rewrites of ~25 GB each on /scratch.
#SBATCH --job-name=compaction-tuning
#SBATCH --partition=cpu
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=256G
#SBATCH --time=04:00:00
#SBATCH --signal=B:TERM@300
#SBATCH --output=logs/%x-%j.out

set -euo pipefail
OUT="${1:-docs/validation_results/compaction_tuning.csv}"
shift || true
CONFIG="${CONFIG:-config/cluster.yaml}"

cd "$SLURM_SUBMIT_DIR"  # the repo root
source cluster/env.sh
export PYTHONPATH=.

WORK="/scratch/$USER/$SLURM_JOB_ID"
export SPARK_LOCAL_DIRS="$WORK/spark"  # replaces env.sh's ~/chess/spark-tmp for this job
mkdir -p "$SPARK_LOCAL_DIRS"
CHILD=""

cleanup() {
    local status=$?
    rm -rf "$WORK"
    echo "removed $WORK  exit_status=$status  end=$(date -u +%FT%TZ)"
}
trap cleanup EXIT
# scancel or the time limit (--signal=B:TERM@300): stop the running step, then exit (runs cleanup).
trap 'echo "received TERM/INT"; if [[ -n "$CHILD" ]]; then kill -TERM "$CHILD" 2>/dev/null || true; wait "$CHILD" || true; fi; exit 143' TERM INT

CPUS="$SLURM_CPUS_PER_TASK"
DRIVER_MEM="$(( SLURM_MEM_PER_NODE * 3 / 4 ))m"
echo "job=$SLURM_JOB_ID node=$(hostname) cpus=$CPUS driver_memory=$DRIVER_MEM out=$OUT config=$CONFIG args=$*"
echo "work=$WORK start=$(date -u +%FT%TZ)"

t0=$SECONDS
spark-submit --master "local[$CPUS]" --driver-memory "$DRIVER_MEM" \
    src/quality/compaction_tuning.py --config "$CONFIG" --out "$OUT" --work-dir "$WORK" "$@" &
CHILD=$!
wait "$CHILD"
CHILD=""
echo "compaction_tuning took $((SECONDS - t0)) s"
