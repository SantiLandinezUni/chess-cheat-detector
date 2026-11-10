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

Slurm enforces CPU and memory limits, so Spark always runs as `local[N]` inside one job, with N = `$SLURM_CPUS_PER_TASK`. Set the Spark driver memory to about 75% of the job's `--mem`.

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
