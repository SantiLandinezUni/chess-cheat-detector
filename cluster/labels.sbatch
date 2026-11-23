#!/bin/bash
# Workstream C: labels at scale. Sample players, fetch their Lichess profiles, build labels and stats.
# Submit from the repo root (logs/ must exist first), after cluster/features.sbatch has written
# lake/anomaly/player for the same months:
#     mkdir -p logs
#     sbatch cluster/labels.sbatch 2026-10
#     N_RANDOM=80000 N_HIGH_ANOMALY=20000 sbatch cluster/labels.sbatch 2026-08,2026-09,2026-10
# Optional environment variables (sbatch passes them to the job):
#     CONFIG          default config/cluster.yaml
#     N_RANDOM        default 80000   players drawn at random from lake/anomaly/player
#     N_HIGH_ANOMALY  default 20000   extra players with the highest anomaly_pct
#     SEED            default 2026    seed of the random draw (hash order, reproducible)
#     PAUSE_S         default 20      seconds between API requests (Lichess allows ~20 quick requests,
#                                     then throttles for minutes; pausing avoids 429s and the 403 block)
#     RESAMPLE        default 0       1 = rebuild lake/labels_sample even if it exists
# Steps:
#     1. src/ingestion/sample_labels.py       -> lake/labels_sample (skipped if it exists, unless RESAMPLE=1)
#     2. src/ingestion/collect_labels.py      -> data/raw/profiles/*.ndjson (cache: saved names are skipped)
#     3. src/ingestion/build_labels_spark.py  -> lake/labels
#     4. src/ingestion/label_stats.py         -> docs/validation_results/labels_*_cluster.csv
# Long and light: the time goes into waiting for the API (~3 requests of 300 names per minute with
# PAUSE_S=20, so ~100k players in ~2 h). Resumable: resubmit and step 2 fetches only the missing names.
# If step 2 ends with failed batches it runs once more after 10 minutes; if batches still fail (e.g.
# HTTP 403), the job stops before step 3, so no partial labels are written.
# Privacy: the profiles hold usernames and stay in data/raw/profiles on the cluster. lake/labels and the
# CSV files hold no usernames; only the CSVs are committed.
# Output: logs/labels-<job id>.out, step logs in logs/
#SBATCH --job-name=labels
#SBATCH --partition=cpu
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=12:00:00
#SBATCH --signal=B:TERM@300
#SBATCH --output=logs/%x-%j.out

set -euo pipefail
MONTHS="${1:?usage: sbatch cluster/labels.sbatch <month>[,<month>...]}"
CONFIG="${CONFIG:-config/cluster.yaml}"
N_RANDOM="${N_RANDOM:-80000}"
N_HIGH_ANOMALY="${N_HIGH_ANOMALY:-20000}"
SEED="${SEED:-2026}"
PAUSE_S="${PAUSE_S:-20}"
RESAMPLE="${RESAMPLE:-0}"

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
# The collector saves each batch atomically, so a stopped run loses at most the batch in flight.
trap 'echo "received TERM/INT"; if [[ -n "$CHILD" ]]; then kill -TERM "$CHILD" 2>/dev/null || true; wait "$CHILD" || true; fi; exit 143' TERM INT

# Run a step in the background and wait for it, so a TERM is handled right away
run() {
    "$@" &
    CHILD=$!
    wait "$CHILD"
    CHILD=""
}

# local[N] with N = Slurm CPUs; driver memory = 75% of --mem (DataFrame-only steps): 16G -> 12288m
CPUS="$SLURM_CPUS_PER_TASK"
DRIVER_MEM="$(( SLURM_MEM_PER_NODE * 3 / 4 ))m"
SPARK_SUBMIT=(spark-submit --master "local[$CPUS]" --driver-memory "$DRIVER_MEM")
ARGS=(--config "$CONFIG" --months "$MONTHS")
LAKE="$(python -c "from src.common.config import load_config; print(load_config('$CONFIG')['paths']['lake_dir'])")"

echo "job=$SLURM_JOB_ID node=$(hostname) cpus=$CPUS driver_memory=$DRIVER_MEM months=$MONTHS config=$CONFIG"
echo "n_random=$N_RANDOM n_high_anomaly=$N_HIGH_ANOMALY seed=$SEED pause_s=$PAUSE_S resample=$RESAMPLE"
echo "work=$WORK start=$(date -u +%FT%TZ)"

echo "===== 1. sample ====="
t0=$SECONDS
if [[ -d "$LAKE/labels_sample" && "$RESAMPLE" != "1" ]]; then
    echo "lake/labels_sample exists: reusing it (RESAMPLE=1 rebuilds it)"
else
    run "${SPARK_SUBMIT[@]}" src/ingestion/sample_labels.py "${ARGS[@]}" \
        --n-random "$N_RANDOM" --n-high-anomaly "$N_HIGH_ANOMALY" --seed "$SEED"
fi
echo "sample took $((SECONDS - t0)) s"

echo "===== 2. collect profiles (Lichess API) ====="
t0=$SECONDS
# Not via run(): inside `if`, set -e is off and run() would return the status of its last line (0)
collect() {
    local status=0
    "${SPARK_SUBMIT[@]}" src/ingestion/collect_labels.py "${ARGS[@]}" --from-sample --pause-s "$PAUSE_S" &
    CHILD=$!
    wait "$CHILD" || status=$?
    CHILD=""
    return "$status"
}
if ! collect; then
    echo "collect ended with failed batches; retrying the missing ones in 10 minutes"
    sleep 600 & CHILD=$!; wait "$CHILD"; CHILD=""
    if ! collect; then
        echo "collect still has failed batches: stopping before build. Resubmit later; saved batches are skipped." >&2
        exit 1
    fi
fi
echo "collect took $((SECONDS - t0)) s"

echo "===== 3. build labels ====="
t0=$SECONDS
run "${SPARK_SUBMIT[@]}" src/ingestion/build_labels_spark.py "${ARGS[@]}"
echo "build took $((SECONDS - t0)) s"

echo "===== 4. label statistics ====="
t0=$SECONDS
run "${SPARK_SUBMIT[@]}" src/ingestion/label_stats.py "${ARGS[@]}"
echo "stats took $((SECONDS - t0)) s"
ls -l docs/validation_results/labels_*_cluster.csv
