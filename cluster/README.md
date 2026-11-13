# Running on the university Slurm cluster (IE Sci-Tech)

These steps set up one teammate's account on the cluster. Each person does them once. The project rules for the cluster (what you may run where, and the resource limits) are in the "Cluster rules" section of [CLAUDE.md](../CLAUDE.md).

## The cluster

| | |
|---|---|
| Gateway | `ssh.iesci.tech` (you can only reach the cluster through it) |
| Login node | `rust` (`rust.scitech.lab`). Use it to edit files, run git and pip, and submit jobs. **Don't run Spark, parsing or anything heavy here.** |
| Compute nodes | `fortran`, `haskell`, `julia` (512 cores and 400–750 GB RAM each) |
| Partitions | `cpu` (default, max 7 days) · `gpu` (max 3 days) · `interactive` (max 8 h) |
| Home folder | `/home/<user>`, a shared network drive that every node can see |

Folder layout used below:

```
~/chess-cheat-detector/    the git repo (this folder)
~/chess/venv/              Python virtual environment
~/chess/spark-tmp/         Spark scratch space (SPARK_LOCAL_DIRS)
```

## 1. SSH access

On your laptop, add this to `~/.ssh/config` and replace `<user>` with your cluster username:

```
Host iesci-gw
    HostName ssh.iesci.tech
    User <user>

Host rust
    HostName rust.scitech.lab
    User <user>
    ProxyJump iesci-gw
```

After that, `ssh rust` connects through the gateway in one step. VS Code Remote-SSH can use the same `rust` entry. To avoid typing your password twice, copy your key to both hosts: `ssh-copy-id iesci-gw`, then `ssh-copy-id rust`.

## 2. Clone the repo

On `rust`:

```bash
git clone https://github.com/SantiLandinezUni/chess-cheat-detector.git ~/chess-cheat-detector
cd ~/chess-cheat-detector
```

## 3. Python environment

Use the cluster modules for Java 17 and Python 3.11, the same versions we use locally:

```bash
module load Java/17.0.15 Python/3.11.5-GCCcore-13.2.0
mkdir -p ~/chess
python -m venv ~/chess/venv
source ~/chess/venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
pip check
```

`requirements.txt` pins PySpark 3.5.3 and `pandas<3` (PySpark 3.5 doesn't work with pandas 3). Don't install packages one by one without these pins.

From then on, start every login shell, `srun` session and `sbatch` job with:

```bash
source cluster/env.sh      # from the repo root
```

[env.sh](env.sh) loads the two modules, activates `~/chess/venv` and sets `SPARK_LOCAL_DIRS=~/chess/spark-tmp`. It only uses `$HOME` paths, so it works for every account. To keep the venv somewhere else, set `CHESS_HOME` before you source it.

## 4. Secrets (.env)

The pipeline reads `LICHESS_HASH_KEY` from `.env` in the repo root (`~/chess-cheat-detector/.env`):

```bash
cp .env.example .env
chmod 600 .env             # readable by you only; home folders are on a shared drive
```

Paste the group key as `LICHESS_HASH_KEY`. **It must be the same key we use on our laptops.** Otherwise every `player_id` changes and cluster data won't join with the Part 1 data. Get the key privately from Santi. Never generate a new one here. `.env` is git-ignored: never commit it, and never paste it in the group chat or on GitHub.

## 5. Running jobs

Slurm enforces CPU and memory limits, so Spark always runs as `local[N]` inside one job, with N = `$SLURM_CPUS_PER_TASK`. The Spark driver memory (the JVM heap) depends on the step:

- **about 50% of `--mem` for Python-heavy steps**, which run Python code inside Spark tasks (RDD `map`/`mapPartitions`, UDFs), like `parse_games.py` and `parse_chunked.py`. Their N Python workers live outside the heap and need the other half.
- **about 75% for DataFrame-only steps**, like `checks.py` and `load_postgres.py`, where almost all the work happens in the JVM.

With 75% for everything, the first full-month parse (job 28202, 128 GB) lost a Python worker at the memory limit.

**Quick test (`srun`, partition `interactive`):** the command runs on a compute node and its output appears in your terminal.

```bash
source cluster/env.sh
srun -p interactive --cpus-per-task=8 --mem=16G --time=00:30:00 python cluster/spark_test.py
```

For an interactive shell on a compute node, use `srun -p interactive --cpus-per-task=8 --mem=16G --time=01:00:00 --pty bash`, then `source cluster/env.sh` again inside it.

**Real runs (`sbatch`, partition `cpu`):** the job runs in the background, even after you log out. Use [spark_test.sbatch](spark_test.sbatch) as the template.

```bash
cd ~/chess-cheat-detector
mkdir -p logs              # Slurm writes the job output here; it doesn't create the folder
sbatch cluster/spark_test.sbatch
```

The smoke test should finish in a few seconds and end with `OK  cores=16  rows=50M ...` in `logs/spark-test-<job id>.out`.

**Monitoring:**

| Command | Shows |
|---|---|
| `squeue -u $USER` | your queued and running jobs |
| `sacct -j <job id> --format=JobID,State,Elapsed,MaxRSS,ExitCode` | status and resource use of a finished job |
| `scancel <job id>` | cancels a job |
| `tail -f logs/<name>-<job id>.out` | follows a job's output |
| `sinfo -s` | partitions and node availability |

## 6. Storage

Work inside your home folder: `~/chess` and `~/chess-cheat-detector`. Lichess monthly dumps are tens of GB compressed and several times that once decompressed. **Ask the group before downloading more than a few GB**, because we haven't yet confirmed with the admins where large data should go. For development, use the small early Lichess months (2013), which are a few MB each.

Each compute node also has a fast local disk at `/scratch` (fortran 7 TB NVMe, julia 3.5 TB NVMe, haskell 3.7 TB SATA). It is **local to that node**: `rust` and the other nodes can't see it. Use it only for temporary files inside a job (`/scratch/$USER/$SLURM_JOB_ID`) and delete them before the job ends.

## 7. Parsing a full month

A full month is about 30 GB compressed and about 210 GB of PGN, so it is never decompressed whole. Two jobs, from the repo root:

```bash
mkdir -p logs
sbatch cluster/download.sbatch 2026-10                 # ~30 GB into data/raw/, resumes if resubmitted, checks the SHA-256
sbatch --dependency=afterok:<download job id> cluster/parse_chunked.sbatch 2026-10
```

`parse_chunked.sbatch` (64 CPUs, 256 GB) streams the dump through `zstd -dc`, writes ~20 GB chunks to `/scratch/$USER/$SLURM_JOB_ID`, always cut at a game boundary, and parses them one at a time into the lake in home (each chunk is deleted before the next one is decompressed). Spark's temporary files go to the same scratch folder. At the end it swaps the month into the lake, rebuilds `restricted/identity` once, runs the quality checks and deletes the scratch folder, also if the job fails or is cancelled. If it fails, the lake keeps its previous data for that month; just resubmit.

Submit from a normal login shell. `cluster/env.sh` needs the `module` command, which a non-login shell such as `ssh rust 'sbatch ...'` doesn't define (use `ssh rust 'bash -lc "sbatch ..."'`).

To test on the Part 1 sample (about 6 chunks, a few minutes):

```bash
zstd -q data/raw/lichess_2026-09_sample.pgn -o data/raw/lichess_2026-09_sample.pgn.zst   # inside srun
ZST=data/raw/lichess_2026-09_sample.pgn.zst CHUNK_GB=0.05 \
    sbatch --cpus-per-task=16 --mem=32G --time=01:00:00 cluster/parse_chunked.sbatch 2026-09
```

It must give the Part 1 numbers: 124,913 games, 8,302,882 moves, 1 quarantined record, 69,392 identity rows.

## 8. Kafka (streaming, Part 2)

There are no permanent services on the cluster, so a single-node Kafka broker (KRaft mode, no ZooKeeper) runs **inside a Slurm job**, next to its producer and consumer, and is deleted at the end. Install it once, on `rust`, outside the repo:

```bash
source cluster/env.sh                                    # Java 17
mkdir -p ~/opt/downloads/kafka-4.4.0 && cd ~/opt/downloads/kafka-4.4.0
curl -fLO https://downloads.apache.org/kafka/4.4.0/kafka_2.13-4.4.0.tgz
curl -fLO https://downloads.apache.org/kafka/4.4.0/kafka_2.13-4.4.0.tgz.sha512
cat kafka_2.13-4.4.0.tgz.sha512; sha512sum kafka_2.13-4.4.0.tgz   # the two hashes must match
cd ~/opt && tar -xzf downloads/kafka-4.4.0/kafka_2.13-4.4.0.tgz && ln -sfn kafka_2.13-4.4.0 kafka
~/opt/kafka/bin/kafka-topics.sh --version                # 4.4.0
cd ~/chess-cheat-detector && pip install -r requirements.txt   # confluent-kafka
```

`cluster/kafka_job.sh` starts the broker on free localhost ports with its data in `/scratch/$USER/$SLURM_JOB_ID/kafka`, creates `lichess.moves` (8 partitions), runs `CONSUMER_CMD` and then `PRODUCER_CMD`, stops the broker and deletes the folder, also on failure. Replay 1,000 games of 2026-10 at 100× speed:

```bash
CONSUMER_CMD='python src/streaming/count_consumer.py --config config/cluster.yaml --summary-out logs/streaming/consumer-$SLURM_JOB_ID.json' \
PRODUCER_CMD='spark-submit --driver-memory 6g src/streaming/replay_producer.py --config config/cluster.yaml --month 2026-10 --max-games 1000 --speed 100' \
sbatch cluster/kafka_job.sbatch        # or: srun -p interactive --cpus-per-task=8 --mem=16G --time=00:30:00 cluster/kafka_job.sh
```

Use single quotes so that `$SLURM_JOB_ID`, `$KAFKA_BOOTSTRAP` and `$KAFKA_HOME` are expanded inside the job. `--speed 0` sends without pacing (maximum throughput). Pass `--driver-memory` explicitly: the config's `auto` would give Spark 75% of the job memory, leaving too little for the broker and the consumer.

**Consumer scaling.** `cluster/consumer_scaling.sh` replays the same games without pacing, read by 1, 2, 4 and 8 consumers in one consumer group (`src/streaming/consumer_group.py`), each run with a fresh broker, in two modes: `count` (members only count) and `decode` (members also parse every JSON value, as a scorer would). The first run caches the selected moves under `data/processed/replay_cache/`, so Spark reads the lake only once:

```bash
srun -p interactive --cpus-per-task=16 --mem=32G --time=01:00:00 cluster/consumer_scaling.sh 2026-10 60000
```

It writes one row per run to `docs/validation_results/consumer_scaling.csv` (only if every run succeeded): group throughput = all messages / (last message received by any member − first one), latency percentiles over every message of every member, and the producer's own send rate (`producer_msg_s`), which caps the group throughput.

## 9. Strong-scaling test of the parser

Parses the same input, the first 20 GB chunk of 2026-10 (8,568,719 games), with 16, 32, 64 and 128 CPUs, 3 GB of memory per CPU. All output goes to a throwaway lake on the node's `/scratch` (deleted at the end); the step refuses to write inside `data/`. From the repo root:

```bash
for n in 16 32 64 128; do
    sbatch --cpus-per-task=$n --mem=$((3 * n))G --job-name=parse-scaling-$n cluster/parse_scaling.sbatch 2026-10
done
sbatch --dependency=afterany:<the four job ids> cluster/parse_scaling_report.sbatch <the four job ids>
```

The report job writes `docs/validation_results/parse_scaling.csv`: elapsed time and MaxRSS from `sacct`, extraction and parse time from each run, and speedup and efficiency against the smallest run (on the parse time, and on the elapsed time). The QOS allows 128 CPUs per user across all running jobs, so the 128-CPU run only starts when nothing else of yours is running. Nodes differ: with 64 CPUs, julia parses a chunk about 20% slower than haskell, so note the `node` column when comparing runs.

**128 CPUs doesn't work yet.** Two 128-CPU runs failed at their time limit (28564 on julia, 28808 on fortran); the 16/32/64-CPU runs and the 64-CPU full-month parses never did. In 28808 the parse finished in 255 s, then writing `moves` stalled: no busy cores and a quiet node (load ~2) for 50 minutes, no errors, memory well below the limit. The open-file and process limits are high (524,288 and 128,178), and a quiet load average rules out processes stuck on the network home folder. The `note` column of the CSV has the details. Before the next 128-CPU run, add thread dumps of the Spark driver (`jcmd <pid> Thread.print`) when the monitor sees no busy cores, so the stall shows where it waits.

