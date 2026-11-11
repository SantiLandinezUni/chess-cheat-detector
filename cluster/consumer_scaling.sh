#!/bin/bash
# Consumer scaling test: the maximum-throughput replay (no pacing) read by 1, 2, 4 and 8 count
# consumers in one consumer group, each run with a fresh broker (cluster/kafka_job.sh), in two modes:
#   count   the members only count messages and measure latency
#   decode  the members also decode every JSON value and check its fields (like a real scorer)
# One CSV row per run (src/streaming/consumer_group.py). Run it inside a job, from the repo root:
#     srun -p interactive --cpus-per-task=16 --mem=32G --time=01:00:00 cluster/consumer_scaling.sh 2026-10 60000
# 16 CPUs so that 8 consumers, the producer and the broker don't compete for cores.
# The first run reads the games from the lake with Spark and caches them under
# paths.processed_dir/replay_cache; the other runs reuse the cache.
# Variables: CONSUMERS ("1 2 4 8"), MODES ("count decode"), CONFIG (config/cluster.yaml),
#   OUT_CSV (docs/validation_results/consumer_scaling.csv: written only if every run succeeded).
# Output: logs/streaming/consumer_scaling-<job id>.csv (all runs, also failed ones), then OUT_CSV.

set -euo pipefail
: "${SLURM_JOB_ID:?run inside a Slurm job (srun or sbatch), never on the login node}"
MONTH="${1:-2026-10}"
GAMES="${2:-60000}"
CONSUMERS="${CONSUMERS:-1 2 4 8}"
MODES="${MODES:-count decode}"
CONFIG="${CONFIG:-config/cluster.yaml}"
OUT_CSV="${OUT_CSV:-docs/validation_results/consumer_scaling.csv}"

cd "$(dirname "${BASH_SOURCE[0]}")/.."  # the repo root
source cluster/env.sh
export PYTHONPATH=.
mkdir -p logs/streaming
RUN_CSV="logs/streaming/consumer_scaling-$SLURM_JOB_ID.csv"
CACHE="$(python -c "from src.common.config import load_config; print(load_config('$CONFIG')['paths']['processed_dir'])")/replay_cache"

echo "job=$SLURM_JOB_ID node=$(hostname) cpus=$SLURM_CPUS_PER_TASK month=$MONTH games=$GAMES consumers=($CONSUMERS) modes=($MODES)"
STATUS=0
for mode in $MODES; do
    flag=""
    [[ "$mode" == decode ]] && flag="--check-values"
    for n in $CONSUMERS; do
        echo "########## mode=$mode consumers=$n  $(date -u +%FT%TZ)"
        psum="logs/streaming/producer-$SLURM_JOB_ID-$mode-c$n.json"
        RUN_TAG="$mode-c$n" \
        CONSUMER_CMD="python src/streaming/consumer_group.py --config $CONFIG --consumers $n $flag --csv $RUN_CSV --producer-summary $psum" \
        PRODUCER_CMD="spark-submit --driver-memory 6g src/streaming/replay_producer.py --config $CONFIG --month $MONTH --max-games $GAMES --speed 0 --cache-dir $CACHE --summary-out $psum" \
            bash cluster/kafka_job.sh || { echo "run mode=$mode consumers=$n FAILED"; STATUS=1; }
    done
done

echo "########## results ($RUN_CSV)"
column -s, -t < "$RUN_CSV" || cat "$RUN_CSV"
if (( STATUS == 0 )); then
    cp "$RUN_CSV" "$OUT_CSV"
    echo "copied to $OUT_CSV"
else
    echo "some runs failed: $OUT_CSV not updated"
fi
exit "$STATUS"
