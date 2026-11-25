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
#   KAFKA_PRODUCER_DONE   file touched when the producer has exited (or was stopped); scorer_group.py
#                         stops its scorers only after it appears and the group's lag is 0
# Optional variables: TOPIC (lichess.moves), EXTRA_TOPICS (space-separated topics also created with
#   PARTITIONS partitions, e.g. "lichess.scores" for src/streaming/scorer.py), PARTITIONS (8), KAFKA_HEAP (1G), READY_TIMEOUT_S (60),
#   CONSUMER_WAIT_S (0 = no limit: seconds to wait for the consumer after the producer ends, then stop it
#     gracefully and fail), STOP_RESERVE_S (300), CONSUMER_STOP_GRACE_S (150),
#   RUN_TAG (added to the broker log name, for several runs in one job).
# Shutdown order (fix for job 32798): producer, then the consumer's whole process group (SIGTERM, then
# up to CONSUMER_STOP_GRACE_S for every process of the group, e.g. each scorer, to write its output and
# exit), and only then the broker. Time limit: STOP_RESERVE_S seconds before the job's end time, the run
# is stopped this way, the consumer reports how much it processed, and the exit status is 124
# (TIME_LIMIT_EXIT). The trap does the same on SIGTERM (e.g. sbatch --signal=B:TERM@...).
# Broker data and logs live in node-local /scratch/$USER/$SLURM_JOB_ID/kafka; the broker log is
# copied to logs/kafka-broker-<job id>[-<RUN_TAG>].log, then the folder is deleted on any exit (trap), also on
# failure, scancel or the time limit. Exit status: non-zero if the producer or consumer failed, 124 if the
# run was stopped at the time limit.

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
CONSUMER_WAIT_S="${CONSUMER_WAIT_S:-0}"
STOP_RESERVE_S="${STOP_RESERVE_S:-300}"
CONSUMER_STOP_GRACE_S="${CONSUMER_STOP_GRACE_S:-150}"
TIME_LIMIT_EXIT=124
JOB_SCRATCH="/scratch/$USER/$SLURM_JOB_ID"
WORK="$JOB_SCRATCH/kafka"
export KAFKA_WORK_DIR="$WORK"
STEP_TAG="$SLURM_JOB_ID${SLURM_STEP_ID:+.$SLURM_STEP_ID}${RUN_TAG:+-$RUN_TAG}"
BROKER_PID="" CONSUMER_PID="" PRODUCER_PID=""

[[ -x "$KAFKA_HOME/bin/kafka-server-start.sh" ]] || { echo "no Kafka in $KAFKA_HOME: see cluster/README.md" >&2; exit 1; }

# alive <pid>: true while any process of its process group is running (the commands run under setsid,
# so pid = group id; a group's children, e.g. the scorers of scorer_group.py, can outlive the leader)
alive() { [[ -n "$1" ]] && { kill -0 -- "-$1" 2>/dev/null || kill -0 "$1" 2>/dev/null; }; }

# stop_pid <pid> <name> <seconds>: TERM to its process group (this reaches e.g. the JVM and Python driver
# under spark-submit, and every scorer), wait until the whole group has exited, then KILL
stop_pid() {
    local pid="$1" name="$2" t="$3"
    alive "$pid" || return 0
    echo "stopping $name (pid $pid, whole process group; up to ${t}s)  $(date -u +%FT%TZ)"
    kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
    for ((i = 0; i < t * 2; i++)); do alive "$pid" || { echo "$name stopped"; return 0; }; sleep 0.5; done
    echo "$name did not stop in ${t}s: KILL"
    kill -KILL -- "-$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null || true
}

# Job end time (epoch s) from Slurm: SLURM_JOB_END_TIME if set, else squeue; empty if unknown
job_end() {
    if [[ "${SLURM_JOB_END_TIME:-}" =~ ^[0-9]+$ ]]; then echo "$SLURM_JOB_END_TIME"; return; fi
    local e
    e=$(squeue -h -j "$SLURM_JOB_ID" -o %e 2>/dev/null | head -1)
    [[ -n "$e" && "$e" != "N/A" && "$e" != "Unknown" ]] && date -d "$e" +%s 2>/dev/null || true
}
past_deadline() { [[ -n "$DEADLINE" ]] && (( $(date +%s) >= DEADLINE )); }

cleanup() {
    local status=$?
    set +e
    stop_pid "$PRODUCER_PID" producer 10
    touch "${KAFKA_PRODUCER_DONE:-/dev/null}" 2>/dev/null
    stop_pid "$CONSUMER_PID" consumer "$CONSUMER_STOP_GRACE_S"  # every scorer writes its output first
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
END_TIME=$(job_end)
DEADLINE=""
if [[ -n "$END_TIME" ]]; then
    DEADLINE=$((END_TIME - STOP_RESERVE_S))
    echo "job ends $(date -u -d "@$END_TIME" +%FT%TZ): the run is stopped at $(date -u -d "@$DEADLINE" +%FT%TZ) if not done (STOP_RESERVE_S=$STOP_RESERVE_S)"
else
    echo "WARNING: job end time unknown: no time-limit guard (only the trap on SIGTERM)"
fi
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
export KAFKA_PRODUCER_DONE="$WORK/producer.done"
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
TIME_UP=0
while kill -0 "$PRODUCER_PID" 2>/dev/null; do
    if past_deadline; then TIME_UP=1; break; fi
    sleep 1
done
PRODUCER_STATUS=0
if (( TIME_UP )); then
    echo "TIME LIMIT: producer still running ${STOP_RESERVE_S}s before the job ends"
    stop_pid "$PRODUCER_PID" producer 10
    PRODUCER_STATUS=$TIME_LIMIT_EXIT
fi
wait "$PRODUCER_PID" || PRODUCER_STATUS=$?
PRODUCER_PID=""
touch "$KAFKA_PRODUCER_DONE"
echo "producer exit_status=$PRODUCER_STATUS after $((SECONDS - t0)) s"

# The consumer stops by itself: a count consumer when idle, scorer_group.py once the producer is done and
# its lag is 0. Wait for its whole process group, until the time-limit deadline or CONSUMER_WAIT_S if set.
t1=$SECONDS
while (( ! TIME_UP )) && alive "$CONSUMER_PID"; do
    if past_deadline; then
        TIME_UP=1
        echo "TIME LIMIT: consumer still running ${STOP_RESERVE_S}s before the job ends"
    elif (( CONSUMER_WAIT_S > 0 && SECONDS - t1 >= CONSUMER_WAIT_S )); then
        echo "consumer still running ${CONSUMER_WAIT_S}s after the producer (CONSUMER_WAIT_S): stopping it"
        break
    fi
    (( TIME_UP )) || sleep 1
done
FORCED_STOP=0
if alive "$CONSUMER_PID"; then
    FORCED_STOP=1
    stop_pid "$CONSUMER_PID" consumer "$CONSUMER_STOP_GRACE_S"  # each scorer still writes its output
fi
CONSUMER_STATUS=0
wait "$CONSUMER_PID" || CONSUMER_STATUS=$?
CONSUMER_PID=""
echo "consumer exit_status=$CONSUMER_STATUS after $((SECONDS - t1)) s after the producer  $(date -u +%FT%TZ)"
(( FORCED_STOP && CONSUMER_STATUS == 0 )) && CONSUMER_STATUS=1  # stopped from outside: never a clean run

"$KAFKA_HOME/bin/kafka-get-offsets.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$KAFKA_TOPIC" \
    | awk -F: '{n += $3} END {print "messages in topic: " n}'
for t in ${EXTRA_TOPICS:-}; do
    "$KAFKA_HOME/bin/kafka-get-offsets.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$t" \
        | awk -F: -v t="$t" '{n += $3} END {print "messages in " t ": " n}'
done
du -sh "$WORK/data" | awk '{print "broker data on /scratch: " $1}'

if (( TIME_UP )); then
    echo "FAILED: TIME LIMIT reached: the run was stopped before the end; see the consumer's INCOMPLETE line above for how much was processed"
    exit "$TIME_LIMIT_EXIT"
fi
(( PRODUCER_STATUS == 0 && CONSUMER_STATUS == 0 ))
