#!/bin/bash
# Feedback 3.3: provisional baselines run separately per month and compared
# (src/streaming/baselines_by_month.py) -> one CSV. Does not touch lake/baselines_provisional.
# Submit from the repo root (logs/ must exist first). The month list is the first argument:
#     mkdir -p logs
#     sbatch cluster/baselines_by_month.sbatch 2026-08,2026-09,2026-10 [out.csv]
# Default output: docs/validation_results/baselines_by_month.csv
# Optional environment variables: CONFIG (default config/cluster.yaml)
# DataFrame-only step, so the JVM heap gets 75% of --mem. Same size as baselines_provisional.sbatch
# (one month took ~15 min on 64 CPUs, job 29063); here two queries per month, months one after another.
# Scratch: Spark's shuffle/spill files go to /scratch/$USER/$SLURM_JOB_ID, deleted on exit by a trap.
#SBATCH --job-name=baselines-month
#SBATCH --partition=cpu
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=256G
#SBATCH --time=04:00:00
#SBATCH --signal=B:TERM@300
#SBATCH --output=logs/%x-%j.out

set -euo pipefail
MONTHS="${1:?usage: sbatch cluster/baselines_by_month.sbatch <month>[,<month>...] [out.csv]}"
OUT="${2:-docs/validation_results/baselines_by_month.csv}"
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
# scancel or the time limit (--signal=B:TERM@300): stop the running step, then exit (runs cleanup)
trap 'echo "received TERM/INT"; if [[ -n "$CHILD" ]]; then kill -TERM "$CHILD" 2>/dev/null || true; wait "$CHILD" || true; fi; exit 143' TERM INT

CPUS="$SLURM_CPUS_PER_TASK"
DRIVER_MEM="$(( SLURM_MEM_PER_NODE * 3 / 4 ))m"
echo "job=$SLURM_JOB_ID node=$(hostname) cpus=$CPUS driver_memory=$DRIVER_MEM months=$MONTHS out=$OUT config=$CONFIG"
echo "work=$WORK start=$(date -u +%FT%TZ)"

t0=$SECONDS
spark-submit --master "local[$CPUS]" --driver-memory "$DRIVER_MEM" \
    src/streaming/baselines_by_month.py --config "$CONFIG" --months "$MONTHS" --out "$OUT" &
CHILD=$!
wait "$CHILD"
CHILD=""
echo "baselines_by_month took $((SECONDS - t0)) s"
