#!/bin/bash
# Strong-scaling test of the parser: parse the first ~20 GB chunk of a month (2026-10: 8,568,719 games)
# with the job's CPUs. Submit once per core count, with the same memory per core, from the repo root:
#     for n in 16 32 64 128; do
#         sbatch --cpus-per-task=$n --mem=$((3 * n))G --job-name=parse-scaling-$n cluster/parse_scaling.sbatch 2026-10
#     done
#     sbatch --dependency=afterany:<the four job ids> cluster/parse_scaling_report.sbatch <the four job ids>
# Driver memory (JVM heap) = 50% of --mem, as for the full-month parse (Python-heavy step).
# The chunk, Spark's temporary files and ALL the parse output (a throwaway lake, including
# identity_by_month with usernames) live in node-local /scratch/$USER/$SLURM_JOB_ID, deleted on
# any exit (trap). Nothing is written to the real lake or restricted/.
# Output: logs/<job name>-<job id>.out, result JSON in logs/parse_scaling/parse_scaling-<job id>.json
# The log also gets the CPUs the job may use and, every 5 min, the node load and the cores your own
# processes kept busy: job 28564 (128 CPUs, julia) parsed ~19x slower than the 64-CPU run and hit the time
# limit, and these lines tell a starved job from a busy node. For 128 CPUs, --time=01:00:00 is plenty
# (the 64-CPU run took 11 min), so a stall fails fast. The QOS allows 128 CPUs per user in total:
# the 128-CPU run only starts when no other job of yours is running.
#SBATCH --job-name=parse-scaling
#SBATCH --partition=cpu
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=48G
#SBATCH --time=04:00:00
#SBATCH --signal=B:TERM@120
#SBATCH --output=logs/%x-%j.out

set -euo pipefail
MONTH="${1:?usage: sbatch cluster/parse_scaling.sbatch <month, e.g. 2026-10>}"
CONFIG="${CONFIG:-config/cluster.yaml}"

cd "$SLURM_SUBMIT_DIR"  # the repo root
source cluster/env.sh
export PYTHONPATH=.

WORK="/scratch/$USER/$SLURM_JOB_ID"
export SPARK_LOCAL_DIRS="$WORK/spark"
mkdir -p "$SPARK_LOCAL_DIRS"
CHILD="" MONITOR=""
cleanup() {
    local status=$?
    if [[ -n "$MONITOR" ]]; then kill "$MONITOR" 2>/dev/null || true; fi
    echo "scratch before cleanup: $(du -sh "$WORK" 2>/dev/null | cut -f1)"
    rm -rf "$WORK"
    echo "removed $WORK  exit_status=$status  end=$(date -u +%FT%TZ)"
}
trap cleanup EXIT
trap 'echo "received TERM/INT"; if [[ -n "$CHILD" ]]; then kill -TERM "$CHILD" 2>/dev/null || true; wait "$CHILD" || true; fi; exit 143' TERM INT

CPUS="$SLURM_CPUS_PER_TASK"
DRIVER_MEM="$(( SLURM_MEM_PER_NODE / 2 ))m"
echo "job=$SLURM_JOB_ID node=$(hostname) cpus=$CPUS mem=${SLURM_MEM_PER_NODE}M driver_memory=$DRIVER_MEM month=$MONTH work=$WORK start=$(date -u +%FT%TZ)"
echo "nproc=$(nproc) allowed_cpus=$(taskset -cp $$ | cut -d: -f2 | tr -d ' ') node_load=$(cut -d' ' -f1-3 /proc/loadavg)"
# Every 5 min: node load average and how many cores your processes on this node kept busy in the
# interval (from their cumulative CPU seconds; processes that ended in between can make it low).
# No process names, so nothing about other users' jobs ends up in the log.
my_cpu_s() { ps -u "$USER" -o times= | awk '{s += $1} END {print s + 0}'; }
monitor() {
    local prev now
    prev="$(my_cpu_s)"
    while sleep 300; do
        now="$(my_cpu_s)"
        echo "monitor $(date -u +%T) node_load=$(cut -d' ' -f1-3 /proc/loadavg) my_cores_busy=$(awk -v a="$prev" -v b="$now" 'BEGIN {d = (b - a) / 300; printf "%.1f", (d < 0 ? 0 : d)}')"
        prev="$now"
    done
}
monitor &
MONITOR=$!

spark-submit --master "local[$CPUS]" --driver-memory "$DRIVER_MEM" src/ingestion/parse_scaling.py \
    --config "$CONFIG" --months "$MONTH" --work-dir "$WORK" \
    --result-out "logs/parse_scaling/parse_scaling-$SLURM_JOB_ID.json" &
CHILD=$!
wait "$CHILD"
CHILD=""
