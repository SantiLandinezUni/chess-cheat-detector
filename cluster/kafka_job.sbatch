#!/bin/bash
# Kafka broker + consumer + producer in one Slurm job (see cluster/kafka_job.sh for the variables).
# Submit from the repo root (sbatch passes the environment to the job):
#     CONSUMER_CMD='python src/streaming/count_consumer.py --config config/cluster.yaml --summary-out logs/consumer-speed100.json' \
#     PRODUCER_CMD='spark-submit --driver-memory 6g src/streaming/replay_producer.py --config config/cluster.yaml --month 2026-10 --max-games 1000 --speed 100' \
#     sbatch cluster/kafka_job.sbatch
# Memory: broker heap 1G + producer (Spark driver --driver-memory, then the moves in pandas) +
# consumer. Pass --driver-memory explicitly: config "auto" would take 75% of --mem for Spark alone.
# Output: logs/kafka-job-<job id>.out, broker log logs/kafka-broker-<job id>.log
#SBATCH --job-name=kafka-job
#SBATCH --partition=cpu
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --time=01:00:00
#SBATCH --signal=B:TERM@120
#SBATCH --output=logs/%x-%j.out

cd "$SLURM_SUBMIT_DIR"  # the repo root
exec bash cluster/kafka_job.sh
