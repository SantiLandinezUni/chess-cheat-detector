#!/bin/bash
# Builds docs/validation_results/parse_scaling.csv from the parse_scaling jobs (see parse_scaling.sbatch).
# Submit with a dependency so it runs when they have all ended (also if one failed):
#     sbatch --dependency=afterany:<id1>:<id2>:<id3>:<id4> cluster/parse_scaling_report.sbatch <id1> <id2> <id3> <id4>
# Optional notes for single rows go after the ids, passed through to the script:
#     ... cluster/parse_scaling_report.sbatch <id1> <id2> --note "<id2>=2 GB per CPU instead of 3"
#SBATCH --job-name=parse-scaling-report
#SBATCH --partition=cpu
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=00:15:00
#SBATCH --output=logs/%x-%j.out

set -euo pipefail
(( $# > 0 )) || { echo "usage: sbatch cluster/parse_scaling_report.sbatch <job id> ... [--note JOB=TEXT ...]" >&2; exit 1; }
IDS=() OPTS=()
while (( $# > 0 )); do
    if [[ "$1" == --* ]]; then OPTS+=("$1" "$2"); shift 2; else IDS+=("$1"); shift; fi
done
cd "$SLURM_SUBMIT_DIR"  # the repo root
source cluster/env.sh
export PYTHONPATH=.
sleep 30  # let Slurm accounting record the last job's MaxRSS
JOBS="$(IFS=,; echo "${IDS[*]}")"
python src/ingestion/parse_scaling_report.py --jobs "$JOBS" --output docs/validation_results/parse_scaling.csv "${OPTS[@]}"
column -s, -t < docs/validation_results/parse_scaling.csv
