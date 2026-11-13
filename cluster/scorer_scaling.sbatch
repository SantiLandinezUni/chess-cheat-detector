#!/bin/bash
# Streaming scorer test (cluster/scorer_scaling.sh) as a batch job. Submit from the repo root:
#     sbatch cluster/scorer_scaling.sbatch 2026-10 60000
#     sbatch --dependency=afterok:<baselines job> cluster/scorer_scaling.sbatch 2026-10 60000
# 16 CPUs: up to 4 scorers + the producer (Spark, then the send loop) + the broker without competing for cores.
# Output: logs/scorer-scaling-<job id>.out
#SBATCH --job-name=scorer-scaling
#SBATCH --partition=cpu
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=32G
#SBATCH --time=02:00:00
#SBATCH --signal=B:TERM@180
#SBATCH --output=logs/%x-%j.out

cd "$SLURM_SUBMIT_DIR"  # the repo root
exec bash cluster/scorer_scaling.sh "$@"
