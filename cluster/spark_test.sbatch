#!/bin/bash
# Spark smoke test on a compute node. Submit from the repo root (logs/ must exist first):
#     mkdir -p logs && sbatch cluster/spark_test.sbatch
# Output: logs/spark-test-<job id>.out
#SBATCH --job-name=spark-test
#SBATCH --partition=cpu
#SBATCH --cpus-per-task=16
#SBATCH --mem=32G
#SBATCH --time=00:30:00
#SBATCH --output=logs/%x-%j.out

set -euo pipefail
cd "$SLURM_SUBMIT_DIR"  # the repo root
source cluster/env.sh

# Driver memory = 75% of the Slurm allocation (SLURM_MEM_PER_NODE is in MB): 32G -> 24576m
export SPARK_DRIVER_MEM="$(( ${SLURM_MEM_PER_NODE:-32768} * 3 / 4 ))m"

python cluster/spark_test.py
