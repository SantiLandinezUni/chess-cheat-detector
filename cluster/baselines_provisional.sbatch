#!/bin/bash
# E1: provisional baselines for the streaming scorer (src/streaming/baselines_provisional.py) ->
# lake/baselines_provisional. Stand-in for workstream B's lake/baselines until that runs on the full data.
# Submit from the repo root (logs/ must exist first). The month list is the first argument:
#     mkdir -p logs
#     sbatch cluster/baselines_provisional.sbatch 2026-10
# Optional environment variables (sbatch passes them to the job):
#     CONFIG  default config/cluster.yaml
# DataFrame-only step, so the JVM heap gets 75% of --mem. One query: moves x games join, then per-game
# and move-level aggregates with approximate percentiles; the output is a few hundred rows.
# Scratch: Spark's shuffle/spill files go to /scratch/$USER/$SLURM_JOB_ID (node-local disk), deleted on
# exit by a trap. The lake itself is read from and written to home.
# Output: logs/baselines-prov-<job id>.out, step log in logs/
#SBATCH --job-name=baselines-prov
#SBATCH --partition=cpu
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=256G
#SBATCH --time=06:00:00
#SBATCH --signal=B:TERM@300
#SBATCH --output=logs/%x-%j.out

set -euo pipefail
MONTHS="${1:?usage: sbatch cluster/baselines_provisional.sbatch <month>[,<month>...]}"
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

# local[N] with N = Slurm CPUs; driver memory = 75% of --mem (SLURM_MEM_PER_NODE is in MB): 256G -> 196608m
CPUS="$SLURM_CPUS_PER_TASK"
DRIVER_MEM="$(( SLURM_MEM_PER_NODE * 3 / 4 ))m"
echo "job=$SLURM_JOB_ID node=$(hostname) cpus=$CPUS driver_memory=$DRIVER_MEM months=$MONTHS config=$CONFIG"
echo "work=$WORK start=$(date -u +%FT%TZ)"

t0=$SECONDS
spark-submit --master "local[$CPUS]" --driver-memory "$DRIVER_MEM" \
    src/streaming/baselines_provisional.py --config "$CONFIG" --months "$MONTHS" &
CHILD=$!
wait "$CHILD"
CHILD=""
echo "baselines_provisional took $((SECONDS - t0)) s"
