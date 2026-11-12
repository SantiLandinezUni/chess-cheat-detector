#!/bin/bash
# Workstream B: features and baselines on the parsed months in the Parquet lake.
# Submit from the repo root (logs/ must exist first). The month list is the first argument:
#     mkdir -p logs
#     sbatch cluster/features.sbatch 2026-10                      # one month
#     sbatch cluster/features.sbatch 2026-08,2026-09,2026-10      # several, comma-separated
# Optional environment variables (sbatch passes them to the job):
#     CONFIG  default config/cluster.yaml
# Steps (all DataFrame-only, so the JVM heap gets 75% of --mem):
#     1. src/features/derived.py      -> lake/derived/players, lake/derived/baselines_part1
#     2. src/features/game_player.py  -> lake/features/game_player (partitioned by month, time class)
# derived.py runs with --approx-percentiles: exact percentiles over billions of moves don't fit in memory.
# Scratch: Spark's shuffle/spill files go to /scratch/$USER/$SLURM_JOB_ID (node-local disk), deleted on
# exit by a trap. The lake itself is read from and written to home.
# Output: logs/features-<job id>.out, step logs in logs/
#SBATCH --job-name=features
#SBATCH --partition=cpu
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=256G
#SBATCH --time=1-00:00:00
#SBATCH --signal=B:TERM@300
#SBATCH --output=logs/%x-%j.out

set -euo pipefail
MONTHS="${1:?usage: sbatch cluster/features.sbatch <month>[,<month>...]}"
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

# Run a step in the background and wait for it, so a TERM is handled right away
run() {
    "$@" &
    CHILD=$!
    wait "$CHILD"
    CHILD=""
}

# local[N] with N = Slurm CPUs; driver memory = 75% of --mem (SLURM_MEM_PER_NODE is in MB): 256G -> 196608m
CPUS="$SLURM_CPUS_PER_TASK"
DRIVER_MEM="$(( SLURM_MEM_PER_NODE * 3 / 4 ))m"
SPARK_SUBMIT=(spark-submit --master "local[$CPUS]" --driver-memory "$DRIVER_MEM")
ARGS=(--config "$CONFIG" --months "$MONTHS")

echo "job=$SLURM_JOB_ID node=$(hostname) cpus=$CPUS driver_memory=$DRIVER_MEM months=$MONTHS config=$CONFIG"
echo "work=$WORK start=$(date -u +%FT%TZ)"

echo "===== derived (players, baselines_part1) ====="
t0=$SECONDS
run "${SPARK_SUBMIT[@]}" src/features/derived.py "${ARGS[@]}" --approx-percentiles
echo "derived took $((SECONDS - t0)) s"

echo "===== game_player features ====="
t0=$SECONDS
run "${SPARK_SUBMIT[@]}" src/features/game_player.py "${ARGS[@]}"
echo "game_player took $((SECONDS - t0)) s"
