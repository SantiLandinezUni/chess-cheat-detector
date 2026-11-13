#!/bin/bash
# Single-node Kafka (KRaft, no ZooKeeper) inside ONE Slurm job: start a broker on this node, create
# the topic, run a consumer and a producer command against it, then stop the broker and delete its
# data. Run it inside a job, from the repo root, with the two commands in environment variables:
#
#   CONSUMER_CMD='python src/streaming/count_consumer.py --config config/cluster.yaml' \
#   PRODUCER_CMD='spark-submit --driver-memory 6g src/streaming/replay_producer.py --config config/cluster.yaml --month 2026-10 --max-games 1000 --speed 100' \
#   srun -p interactive --cpus-per-task=8 --mem=16G --time=00:30:00 cluster/kafka_job.sh
#
# or with sbatch: cluster/kafka_job.sbatch (same variables).
#
# The commands run with `setsid bash -c` (own process group, so stopping one stops all its children)
# and get these exported variables:
#   KAFKA_BOOTSTRAP       127.0.0.1:<port> of the broker (free ports picked at start; localhost only)
#   KAFKA_TOPIC           the topic (default lichess.moves)
#   KAFKA_HOME            the Kafka install (default ~/opt/kafka), e.g. for kafka-*-perf-test.sh
#   KAFKA_PARTITIONS      partitions of the topic
#   KAFKA_WORK_DIR        this run's scratch folder, for temporary files (deleted at the end)
#   KAFKA_CONSUMER_READY  file the consumer touches once it is assigned; the producer starts after it
#                         appears (count_consumer.py does this), or after READY_TIMEOUT_S seconds
# Optional variables: TOPIC (lichess.moves), EXTRA_TOPICS (space-separated topics also created with
#   PARTITIONS partitions, e.g. "lichess.scores" for src/streaming/scorer.py), PARTITIONS (8), KAFKA_HEAP (1G), READY_TIMEOUT_S (60),
#   CONSUMER_WAIT_S (900: how long to wait for the consumer after the producer ends before killing it),
#   RUN_TAG (added to the broker log name, for several runs in one job).
# Broker data and logs live in node-local /scratch/$USER/$SLURM_JOB_ID/kafka; the broker log is
# copied to logs/kafka-broker-<job id>[-<RUN_TAG>].log, then the folder is deleted on any exit (trap), also on
# failure, scancel or the time limit. Exit status: non-zero if the producer or consumer failed.

set -euo pipefail
: "${SLURM_JOB_ID:?run inside a Slurm job (srun or sbatch), never on the login node}"
: "${PRODUCER_CMD:?set PRODUCER_CMD}"
: "${CONSUMER_CMD:?set CONSUMER_CMD}"

cd "$(dirname "${BASH_SOURCE[0]}")/.."  # the repo root
source cluster/env.sh
export PYTHONPATH=.
mkdir -p logs

export KAFKA_HOME="${KAFKA_HOME:-$HOME/opt/kafka}"
export KAFKA_TOPIC="${TOPIC:-lichess.moves}"
export KAFKA_PARTITIONS="${PARTITIONS:-8}"
PARTITIONS="$KAFKA_PARTITIONS"
READY_TIMEOUT_S="${READY_TIMEOUT_S:-60}"
CONSUMER_WAIT_S="${CONSUMER_WAIT_S:-900}"
JOB_SCRATCH="/scratch/$USER/$SLURM_JOB_ID"
WORK="$JOB_SCRATCH/kafka"
export KAFKA_WORK_DIR="$WORK"
STEP_TAG="$SLURM_JOB_ID${SLURM_STEP_ID:+.$SLURM_STEP_ID}${RUN_TAG:+-$RUN_TAG}"
BROKER_PID="" CONSUMER_PID="" PRODUCER_PID=""

[[ -x "$KAFKA_HOME/bin/kafka-server-start.sh" ]] || { echo "no Kafka in $KAFKA_HOME: see cluster/README.md" >&2; exit 1; }

# stop_pid <pid> <name> <seconds>: TERM to its process group (the commands run under setsid, so
# this reaches e.g. the JVM and Python driver under spark-submit), wait, then KILL
stop_pid() {
    local pid="$1" name="$2" t="$3"
    [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null || return 0
    echo "stopping $name (pid $pid)"
    kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
    for ((i = 0; i < t * 2; i++)); do kill -0 "$pid" 2>/dev/null || return 0; sleep 0.5; done
    echo "$name did not stop in ${t}s: KILL"
    kill -KILL -- "-$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null || true
}

cleanup() {
    local status=$?
    set +e
    stop_pid "$PRODUCER_PID" producer 10
    stop_pid "$CONSUMER_PID" consumer 10
    stop_pid "$BROKER_PID" broker 30
    wait 2>/dev/null
    if [[ -f "$WORK/logs/server.log" ]]; then
        cp "$WORK/logs/server.log" "logs/kafka-broker-$STEP_TAG.log"
        echo "broker log: logs/kafka-broker-$STEP_TAG.log"
    fi
    rm -rf "$WORK"
    rmdir "$JOB_SCRATCH" 2>/dev/null  # only if nothing else of this job is there
    echo "removed $WORK  exit_status=$status  end=$(date -u +%FT%TZ)"
}
trap cleanup EXIT
trap 'echo "received TERM/INT"; exit 143' TERM INT

# Two free localhost ports: broker (clients) and controller (KRaft quorum)
read -r PORT CONTROLLER_PORT < <(python - <<'EOF'
import socket
socks = [socket.socket() for _ in range(2)]
for s in socks:
    s.bind(("127.0.0.1", 0))
print(*(s.getsockname()[1] for s in socks))
EOF
)
export KAFKA_BOOTSTRAP="127.0.0.1:$PORT"

mkdir -p "$WORK/data" "$WORK/logs"
cat > "$WORK/server.properties" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:$CONTROLLER_PORT
listeners=PLAINTEXT://127.0.0.1:$PORT,CONTROLLER://127.0.0.1:$CONTROLLER_PORT
advertised.listeners=PLAINTEXT://127.0.0.1:$PORT
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
log.dirs=$WORK/data
num.partitions=$PARTITIONS
auto.create.topics.enable=false
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
share.coordinator.state.topic.replication.factor=1
share.coordinator.state.topic.min.isr=1
group.initial.rebalance.delay.ms=0
EOF

echo "job=$STEP_TAG node=$(hostname) cpus=${SLURM_CPUS_PER_TASK:-?} mem=${SLURM_MEM_PER_NODE:-?}M kafka=$(readlink -f "$KAFKA_HOME")"
echo "bootstrap=$KAFKA_BOOTSTRAP controller=127.0.0.1:$CONTROLLER_PORT topic=$KAFKA_TOPIC partitions=$PARTITIONS work=$WORK start=$(date -u +%FT%TZ)"

"$KAFKA_HOME/bin/kafka-storage.sh" format -t "$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)" \
    -c "$WORK/server.properties" > "$WORK/logs/format.out"

KAFKA_HEAP_OPTS="-Xms${KAFKA_HEAP:-1G} -Xmx${KAFKA_HEAP:-1G}" LOG_DIR="$WORK/logs" \
    setsid "$KAFKA_HOME/bin/kafka-server-start.sh" "$WORK/server.properties" > "$WORK/logs/broker.out" 2>&1 &
BROKER_PID=$!

# Wait until the broker answers (max 60 s)
t0=$SECONDS
until "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --list > /dev/null 2>&1; do
    if ! kill -0 "$BROKER_PID" 2>/dev/null; then echo "broker died at start:"; tail -30 "$WORK/logs/broker.out"; exit 1; fi
    if (( SECONDS - t0 > 60 )); then echo "broker not ready after 60 s"; tail -30 "$WORK/logs/broker.out"; exit 1; fi
    sleep 1
done
echo "broker ready in $((SECONDS - t0)) s"

"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --create --topic "$KAFKA_TOPIC" \
    --partitions "$PARTITIONS" --replication-factor 1
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --describe --topic "$KAFKA_TOPIC" | head -1
for t in ${EXTRA_TOPICS:-}; do
    "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --create --topic "$t" \
        --partitions "$PARTITIONS" --replication-factor 1
done

export KAFKA_CONSUMER_READY="$WORK/consumer.ready"
echo "===== consumer: $CONSUMER_CMD"
setsid bash -c "$CONSUMER_CMD" &
CONSUMER_PID=$!
for ((i = 0; i < READY_TIMEOUT_S * 2; i++)); do
    [[ -f "$KAFKA_CONSUMER_READY" ]] && break
    kill -0 "$CONSUMER_PID" 2>/dev/null || break
    sleep 0.5
done
[[ -f "$KAFKA_CONSUMER_READY" ]] || echo "WARNING: consumer not ready after ${READY_TIMEOUT_S}s (or it exited); starting the producer anyway"

echo "===== producer: $PRODUCER_CMD"
t0=$SECONDS
setsid bash -c "$PRODUCER_CMD" &
PRODUCER_PID=$!
PRODUCER_STATUS=0
wait "$PRODUCER_PID" || PRODUCER_STATUS=$?
PRODUCER_PID=""
echo "producer exit_status=$PRODUCER_STATUS after $((SECONDS - t0)) s"

# The consumer stops by itself once it has been idle; give it CONSUMER_WAIT_S at most
CONSUMER_STATUS=0
for ((i = 0; i < CONSUMER_WAIT_S * 2; i++)); do kill -0 "$CONSUMER_PID" 2>/dev/null || break; sleep 0.5; done
if kill -0 "$CONSUMER_PID" 2>/dev/null; then
    echo "consumer still running after ${CONSUMER_WAIT_S}s: stopping it"
    stop_pid "$CONSUMER_PID" consumer 10
fi
wait "$CONSUMER_PID" || CONSUMER_STATUS=$?
CONSUMER_PID=""
echo "consumer exit_status=$CONSUMER_STATUS"

"$KAFKA_HOME/bin/kafka-get-offsets.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$KAFKA_TOPIC" \
    | awk -F: '{n += $3} END {print "messages in topic: " n}'
for t in ${EXTRA_TOPICS:-}; do
    "$KAFKA_HOME/bin/kafka-get-offsets.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$t" \
        | awk -F: -v t="$t" '{n += $3} END {print "messages in " t ": " n}'
done
du -sh "$WORK/data" | awk '{print "broker data on /scratch: " $1}'

(( PRODUCER_STATUS == 0 && CONSUMER_STATUS == 0 ))
