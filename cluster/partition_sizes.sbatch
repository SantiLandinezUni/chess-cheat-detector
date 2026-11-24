#!/bin/bash
# Partition skew check (src/quality/partition_sizes.py): file count and file sizes per
# (source_month, time_class) partition of lake/games and lake/moves. Lists files and reads Parquet
# footers only; no Spark. Runs as a job because the login node may not read the lake.
# Submit from the repo root (logs/ must exist first):
#     mkdir -p logs
#     sbatch cluster/partition_sizes.sbatch [out.csv]
# Default output: docs/validation_results/partition_sizes_before.csv
# Optional environment variables: CONFIG (default config/cluster.yaml)
#SBATCH --job-name=partition-sizes
#SBATCH --partition=cpu
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
#SBATCH --time=00:30:00
#SBATCH --output=logs/%x-%j.out

set -euo pipefail
OUT="${1:-docs/validation_results/partition_sizes_before.csv}"
CONFIG="${CONFIG:-config/cluster.yaml}"

cd "$SLURM_SUBMIT_DIR"  # the repo root
source cluster/env.sh
export PYTHONPATH=.

echo "job=$SLURM_JOB_ID node=$(hostname) out=$OUT config=$CONFIG start=$(date -u +%FT%TZ)"
python -m src.quality.partition_sizes --config "$CONFIG" --out "$OUT"
echo "end=$(date -u +%FT%TZ)"
