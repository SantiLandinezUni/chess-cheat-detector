#!/bin/bash
# Forced-rebalance test of the streaming scorer group (fix for job 32798). Two cases, each run twice on a
# fresh broker (cluster/kafka_job.sh) with the same replay:
#   paced:     PACED_GAMES games (1000) at PACED_SPEED (100x), PACED_SCORERS (2) scorers
#   no pacing: GAMES games (60000) as fast as possible, SCORERS (4) scorers
#   ref-<case>:   no rebalance (reference)
#   leave-<case>: scorer 0 leaves the group after PACED_LEAVE_AFTER (15000) / LEAVE_AFTER (400000) moves,
#                 while the producer is still sending: its partitions move to the other scorers, which
#                 continue from its committed offsets (cooperative-sticky rebalance).
# Every run must pass scorer_group.py's checks (each game scored exactly once, games scored = games sent,
# 0 duplicate rows, 0 moves read twice; in the leave runs every open game handed over is restored from
# lichess.scorer_state). Then src/streaming/check_rebalance.py compares leave vs ref row by row: the final
# scores must be identical. Results: logs/streaming/scorer_rebalance-<job id>.csv (group rows) and
# logs/streaming/scorer_rebalance-<job id>-check.csv, copied to OUT_CSV
# (docs/validation_results/scorer_rebalance.csv) only if every run and check passed.
# Run inside a job, from the repo root (or with cluster/scorer_rebalance.sbatch):
#     srun -p interactive --cpus-per-task=16 --mem=32G --time=01:00:00 cluster/scorer_rebalance.sh 2026-10
# Variables: CASES ("paced nopacing"), PACED_GAMES, PACED_SPEED, PACED_SCORERS, PACED_LEAVE_AFTER, GAMES,
#   SCORERS, LEAVE_AFTER, EMIT_EVERY (10), CONFIG (config/cluster.yaml), OUT_CSV.

set -euo pipefail
: "${SLURM_JOB_ID:?run inside a Slurm job (srun or sbatch), never on the login node}"
MONTH="${1:-2026-10}"
CASES="${CASES:-paced nopacing}"
PACED_GAMES="${PACED_GAMES:-1000}"
PACED_SPEED="${PACED_SPEED:-100}"
PACED_SCORERS="${PACED_SCORERS:-2}"
PACED_LEAVE_AFTER="${PACED_LEAVE_AFTER:-15000}"
GAMES="${GAMES:-60000}"
SCORERS="${SCORERS:-4}"
LEAVE_AFTER="${LEAVE_AFTER:-400000}"
EMIT_EVERY="${EMIT_EVERY:-10}"
CONFIG="${CONFIG:-config/cluster.yaml}"
OUT_CSV="${OUT_CSV:-docs/validation_results/scorer_rebalance.csv}"

cd "$(dirname "${BASH_SOURCE[0]}")/.."  # the repo root
source cluster/env.sh
export PYTHONPATH=.
mkdir -p logs/streaming
RUN_CSV="logs/streaming/scorer_rebalance-$SLURM_JOB_ID.csv"
CHECK_CSV="logs/streaming/scorer_rebalance-$SLURM_JOB_ID-check.csv"
CACHE=$(python -c "from src.common.config import load_config; print(load_config('$CONFIG')['paths']['processed_dir'] + '/replay_cache')")

CHILD=""
trap 'echo "received TERM/INT"; if [[ -n "$CHILD" ]]; then kill -TERM "$CHILD" 2>/dev/null || true; wait "$CHILD" || true; fi; exit 143' TERM INT

# one_run <tag> <speed> <games> <scorers> [leave_after]
one_run() {
    local tag="$1" speed="$2" games="$3" n="$4" leave="${5:-}"
    local psum="logs/streaming/producer-$SLURM_JOB_ID-$tag.json"
    echo "########## run=$tag speed=$speed games=$games scorers=$n leave_one_after=${leave:-none}  $(date -u +%FT%TZ)"
    RUN_TAG="$tag" EXTRA_TOPICS="lichess.scores lichess.scorer_state" \
    CONSUMER_CMD="python src/streaming/scorer_group.py --config $CONFIG --consumers $n --run-id $SLURM_JOB_ID-$tag --emit-every $EMIT_EVERY --csv $RUN_CSV --producer-summary $psum ${leave:+--leave-one-after $leave}" \
    PRODUCER_CMD="spark-submit --driver-memory 6g src/streaming/replay_producer.py --config $CONFIG --month $MONTH --max-games $games --speed $speed --cache-dir $CACHE --summary-out $psum" \
        bash cluster/kafka_job.sh &
    CHILD=$!
    local status=0
    wait "$CHILD" || status=$?
    CHILD=""
    (( status == 0 )) || echo "run $tag FAILED (exit $status)"
    return "$status"
}

echo "job=$SLURM_JOB_ID node=$(hostname) cpus=$SLURM_CPUS_PER_TASK month=$MONTH cases=($CASES)"
STATUS=0
for c in $CASES; do
    case "$c" in
        paced)    args=("$PACED_SPEED" "$PACED_GAMES" "$PACED_SCORERS"); leave="$PACED_LEAVE_AFTER"; name="speed$PACED_SPEED-c$PACED_SCORERS" ;;
        nopacing) args=(0 "$GAMES" "$SCORERS"); leave="$LEAVE_AFTER"; name="nopacing-c$SCORERS" ;;
        *) echo "unknown case $c" >&2; exit 2 ;;
    esac
    ok=1
    one_run "ref-$name" "${args[@]}" || ok=0
    one_run "leave-$name" "${args[@]}" "$leave" || ok=0
    if (( ok )); then
        python src/streaming/check_rebalance.py --config "$CONFIG" --case "$name" \
            --ref-run "$SLURM_JOB_ID-ref-$name" --test-run "$SLURM_JOB_ID-leave-$name" \
            --group-csv "$RUN_CSV" --out "$CHECK_CSV" || ok=0
    fi
    (( ok )) || STATUS=1
done

echo "########## group rows ($RUN_CSV)"
column -s, -t < "$RUN_CSV" || cat "$RUN_CSV"
echo "########## checks ($CHECK_CSV)"
column -s, -t < "$CHECK_CSV" 2>/dev/null || cat "$CHECK_CSV" 2>/dev/null || true
if (( STATUS == 0 )); then
    cp "$CHECK_CSV" "$OUT_CSV"
    echo "copied to $OUT_CSV"
else
    echo "some runs or checks failed: $OUT_CSV not updated"
fi
exit "$STATUS"
