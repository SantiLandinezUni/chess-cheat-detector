#!/bin/bash
# Forced-rebalance test of the scorer group (cluster/scorer_rebalance.sh) as a batch job. Submit from the repo root:
#     sbatch cluster/scorer_rebalance.sbatch 2026-10
# Same resources as cluster/scorer_scaling.sbatch. Output: logs/scorer-rebalance-<job id>.out
#SBATCH --job-name=scorer-rebalance
#SBATCH --partition=cpu
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=32G
#SBATCH --time=01:00:00
#SBATCH --signal=B:TERM@180
#SBATCH --output=logs/%x-%j.out

cd "$SLURM_SUBMIT_DIR"  # the repo root
exec bash cluster/scorer_rebalance.sh "$@"
