#!/bin/bash
# One-off compaction of lake/games and lake/moves to right-sized files (src/quality/compact_lake.py,
# docs/partition_compaction_proposal.md): copy -> verify rows and checksums -> swap -> time queries ->
# delete the old lake. Nothing may read the lake meanwhile, so submit it after those jobs:
#     mkdir -p logs
#     sbatch --dependency=afterany:<job>[:<job>...] cluster/compact_lake.sbatch [out.csv] [extra args]
# Default output: docs/validation_results/partition_compaction.csv
# Optional environment variables: CONFIG (default config/cluster.yaml)
# DataFrame-only step, so the JVM heap gets 75% of --mem. Reads the 128 GB of games+moves three
# times (checksums before, compaction, checksums after) and needs ~128 GB of extra space on home
# until the old lake is deleted. Shuffle files go to /scratch/$USER/$SLURM_JOB_ID (trap removes them).
#SBATCH --job-name=compact-lake
#SBATCH --partition=cpu
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=256G
#SBATCH --time=12:00:00
#SBATCH --signal=B:TERM@300
#SBATCH --output=logs/%x-%j.out

set -euo pipefail
OUT="${1:-docs/validation_results/partition_compaction.csv}"
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
# A stop before the swap leaves the lake unchanged; lake/_compacted is removed by the next run.
trap 'echo "received TERM/INT"; if [[ -n "$CHILD" ]]; then kill -TERM "$CHILD" 2>/dev/null || true; wait "$CHILD" || true; fi; exit 143' TERM INT

CPUS="$SLURM_CPUS_PER_TASK"
DRIVER_MEM="$(( SLURM_MEM_PER_NODE * 3 / 4 ))m"
echo "job=$SLURM_JOB_ID node=$(hostname) cpus=$CPUS driver_memory=$DRIVER_MEM out=$OUT config=$CONFIG args=$*"
echo "work=$WORK start=$(date -u +%FT%TZ)"

t0=$SECONDS
spark-submit --master "local[$CPUS]" --driver-memory "$DRIVER_MEM" \
    src/quality/compact_lake.py --config "$CONFIG" --out "$OUT" "$@" &
CHILD=$!
wait "$CHILD"
CHILD=""
echo "compact_lake took $((SECONDS - t0)) s"
