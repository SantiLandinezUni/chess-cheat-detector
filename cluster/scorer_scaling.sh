#!/bin/bash
# Streaming scorer test (E1): replay producer -> lichess.moves -> src/streaming/scorer.py group ->
# lichess.scores, each run with a fresh broker (cluster/kafka_job.sh):
#   1. paced:     PACED_GAMES games (1000) at PACED_SPEED (100x) through 1 scorer
#   2. no pacing: GAMES games (60000) as fast as possible, through 1, 2 and 4 scorers in one group
# One CSV row per run (src/streaming/scorer_group.py): throughput, score latency (move sent -> score
# emitted) and ack latency (-> score acknowledged by the broker).
# Needs the scorer's baselines (paths.scorer_baselines_dir, e.g. cluster/baselines_provisional.sbatch).
# Run it inside a job, from the repo root (or with cluster/scorer_scaling.sbatch):
#     srun -p interactive --cpus-per-task=16 --mem=32G --time=02:00:00 cluster/scorer_scaling.sh 2026-10 60000
# The first run of each selection reads the games from the lake with Spark and caches them under
# paths.processed_dir/replay_cache; later runs reuse the cache.
# Variables: CONSUMERS ("1 2 4"), PACED_GAMES (1000), PACED_SPEED (100), EMIT_EVERY (10),
#   CONFIG (config/cluster.yaml), OUT_CSV (docs/validation_results/scorer_scaling.csv: written only if
#   every run succeeded).
# Output: logs/streaming/scorer_scaling-<job id>.csv (all runs, also failed ones), then OUT_CSV.
# Final per-game scores: paths.live_scores_dir/run_id=<job id>-<run tag>/

set -euo pipefail
: "${SLURM_JOB_ID:?run inside a Slurm job (srun or sbatch), never on the login node}"
MONTH="${1:-2026-10}"
GAMES="${2:-60000}"
CONSUMERS="${CONSUMERS:-1 2 4}"
PACED_GAMES="${PACED_GAMES:-1000}"
PACED_SPEED="${PACED_SPEED:-100}"
EMIT_EVERY="${EMIT_EVERY:-10}"
CONFIG="${CONFIG:-config/cluster.yaml}"
OUT_CSV="${OUT_CSV:-docs/validation_results/scorer_scaling.csv}"

cd "$(dirname "${BASH_SOURCE[0]}")/.."  # the repo root
source cluster/env.sh
export PYTHONPATH=.
mkdir -p logs/streaming
RUN_CSV="logs/streaming/scorer_scaling-$SLURM_JOB_ID.csv"
read -r CACHE BASELINES < <(python -c "
from src.common.config import load_config
p = load_config('$CONFIG')['paths']
print(p['processed_dir'] + '/replay_cache', p['scorer_baselines_dir'])")
[[ -d "$BASELINES" ]] || { echo "no baselines in $BASELINES: run cluster/baselines_provisional.sbatch first" >&2; exit 1; }

CHILD=""
trap 'echo "received TERM/INT"; if [[ -n "$CHILD" ]]; then kill -TERM "$CHILD" 2>/dev/null || true; wait "$CHILD" || true; fi; exit 143' TERM INT

# one_run <tag> <speed> <games> <consumers>: a fresh broker, the scorer group and the replay producer
one_run() {
    local tag="$1" speed="$2" games="$3" n="$4"
    local psum="logs/streaming/producer-$SLURM_JOB_ID-$tag.json"
    echo "########## run=$tag speed=$speed games=$games scorers=$n  $(date -u +%FT%TZ)"
    RUN_TAG="$tag" EXTRA_TOPICS="lichess.scores" \
    CONSUMER_CMD="python src/streaming/scorer_group.py --config $CONFIG --consumers $n --run-id $SLURM_JOB_ID-$tag --emit-every $EMIT_EVERY --csv $RUN_CSV --producer-summary $psum" \
    PRODUCER_CMD="spark-submit --driver-memory 6g src/streaming/replay_producer.py --config $CONFIG --month $MONTH --max-games $games --speed $speed --cache-dir $CACHE --summary-out $psum" \
        bash cluster/kafka_job.sh &
    CHILD=$!
    local status=0
    wait "$CHILD" || status=$?
    CHILD=""
    (( status == 0 )) || echo "run $tag FAILED (exit $status)"
    return "$status"
}

echo "job=$SLURM_JOB_ID node=$(hostname) cpus=$SLURM_CPUS_PER_TASK month=$MONTH baselines=$BASELINES"
echo "paced: $PACED_GAMES games at ${PACED_SPEED}x, 1 scorer; no pacing: $GAMES games, scorers=($CONSUMERS)"
STATUS=0
one_run "speed$PACED_SPEED-c1" "$PACED_SPEED" "$PACED_GAMES" 1 || STATUS=1
for n in $CONSUMERS; do
    one_run "nopacing-c$n" 0 "$GAMES" "$n" || STATUS=1
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
