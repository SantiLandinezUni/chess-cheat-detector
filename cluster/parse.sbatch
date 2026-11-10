#!/bin/bash
# Parse Lichess PGN months into the Parquet lake, then run the data-quality checks on them.
# Submit from the repo root (logs/ must exist first). The month list is the first argument:
#     mkdir -p logs
#     sbatch cluster/parse.sbatch 2026-09                  # one month
#     sbatch cluster/parse.sbatch 2026-08,2026-09          # several, comma-separated
# Optional environment variables (sbatch passes them to the job):
#     PGN_PATTERN  input path with {month}, e.g. the Part 1 sample:
#                  PGN_PATTERN='data/raw/lichess_{month}_sample.pgn' sbatch cluster/parse.sbatch 2026-09
#     CONFIG       default config/cluster.yaml
# Output: logs/parse-<job id>.out, step logs in logs/, DQ results in logs/dq_parquet-<job id>.json
#SBATCH --job-name=parse
#SBATCH --partition=cpu
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=02:00:00
#SBATCH --output=logs/%x-%j.out

set -euo pipefail
MONTHS="${1:?usage: sbatch cluster/parse.sbatch <month>[,<month>...]}"
CONFIG="${CONFIG:-config/cluster.yaml}"

cd "$SLURM_SUBMIT_DIR"  # the repo root
source cluster/env.sh
export PYTHONPATH=.

# local[N] with N = Slurm CPUs. Driver memory (JVM heap) as a share of --mem (SLURM_MEM_PER_NODE is in MB):
# 50% for the parser, which runs python-chess in N Python workers outside the heap (64G -> 32768m),
# 75% for the DataFrame-only checks (64G -> 49152m). The config's "auto" values resolve to the same
# numbers; spark-submit needs them on the command line.
CPUS="$SLURM_CPUS_PER_TASK"
DRIVER_MEM_PYTHON="$(( SLURM_MEM_PER_NODE / 2 ))m"
DRIVER_MEM="$(( SLURM_MEM_PER_NODE * 3 / 4 ))m"
SPARK_SUBMIT_PYTHON=(spark-submit --master "local[$CPUS]" --driver-memory "$DRIVER_MEM_PYTHON")
SPARK_SUBMIT=(spark-submit --master "local[$CPUS]" --driver-memory "$DRIVER_MEM")

PARSE_ARGS=(--config "$CONFIG" --months "$MONTHS")
if [[ -n "${PGN_PATTERN:-}" ]]; then PARSE_ARGS+=(--pgn-pattern "$PGN_PATTERN"); fi

echo "job=$SLURM_JOB_ID node=$(hostname) cpus=$CPUS driver_memory=$DRIVER_MEM_PYTHON (parse) / $DRIVER_MEM (checks) months=$MONTHS config=$CONFIG"
echo "pgn_pattern=${PGN_PATTERN:-<from config>}  start=$(date -u +%FT%TZ)"

echo "===== parse_games ====="
t0=$SECONDS
"${SPARK_SUBMIT_PYTHON[@]}" src/ingestion/parse_games.py "${PARSE_ARGS[@]}"
echo "parse_games took $((SECONDS - t0)) s"

echo "===== quality checks ====="
t0=$SECONDS
"${SPARK_SUBMIT[@]}" src/quality/checks.py --config "$CONFIG" --months "$MONTHS" \
    --output "logs/dq_parquet-$SLURM_JOB_ID.json"
echo "checks took $((SECONDS - t0)) s"

echo "end=$(date -u +%FT%TZ)"
