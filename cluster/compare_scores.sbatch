#!/bin/bash
# Compare the final scores of two streaming-scorer runs on the same games (src/streaming/compare_scores.py):
# correlation, risk tier changes and top-1% overlap. Small pandas step (two L13 runs, ~120k rows each).
# Submit from the repo root, usually after a scorer test (cluster/scorer_scaling.sbatch):
#     sbatch --dependency=afterok:<scorer job> cluster/compare_scores.sbatch \
#         29064-nopacing-c1 <scorer job>-nopacing-c1 docs/validation_results/scorer_baselines_comparison.csv
# Optional environment variables: CONFIG (default config/cluster.yaml)
# Output: logs/compare-scores-<job id>.out, step log in logs/
#SBATCH --job-name=compare-scores
#SBATCH --partition=cpu
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --time=00:15:00
#SBATCH --output=logs/%x-%j.out

set -euo pipefail
RUN_A="${1:?usage: sbatch cluster/compare_scores.sbatch <run_id A> <run_id B> <out.csv>}"
RUN_B="${2:?usage: sbatch cluster/compare_scores.sbatch <run_id A> <run_id B> <out.csv>}"
OUT="${3:?usage: sbatch cluster/compare_scores.sbatch <run_id A> <run_id B> <out.csv>}"
CONFIG="${CONFIG:-config/cluster.yaml}"

cd "$SLURM_SUBMIT_DIR"  # the repo root
source cluster/env.sh
export PYTHONPATH=.
echo "job=$SLURM_JOB_ID node=$(hostname) run_a=$RUN_A run_b=$RUN_B out=$OUT start=$(date -u +%FT%TZ)"
python src/streaming/compare_scores.py --config "$CONFIG" --run-a "$RUN_A" --run-b "$RUN_B" --out "$OUT"
column -s, -t < "$OUT" || cat "$OUT"
echo "end=$(date -u +%FT%TZ)"
