# Chess Cheat Detector — Big Data Technologies group project

## What this project is

A distributed pipeline to detect engine assistance and rating manipulation in online chess, using Lichess open data. The goal is a **move-by-move cheating risk score for live Lichess games**.

**Part 1 (done, on `main`; Group Assessment 1, delivered Tue 3 Nov 2026)** was data generation + ingestion: a dataset with at least one CSV, one JSON and one SQL-generated dataset; the data dictionary (`docs/data_dictionary.md` — the source of truth for every table and column); the architecture design; a reliable ingestion pipeline; and validation queries. Everything it built (tables, columns, privacy and reliability rules below) stays in force.

**Part 2 (current)** builds the detection side, on the university Slurm cluster (see Cluster rules). Scope, from the dashed components in the architecture diagram:
- **Unlabelled layer:** full Lichess monthly dumps parsed on the cluster → population baselines per rating band × time class. Anomaly is defined relative to the population.
- **Labelled layer:** players labelled via the Lichess API `tosViolation` flag (Part 1 label rules). Labels are proxies, not ground truth. Classes are heavily imbalanced: evaluate with precision-recall metrics, never accuracy. Cache API lookups so no account is queried twice.
- **Features:** move-time distributions, clock management under time pressure, within-session consistency, accuracy relative to rating. Engine-derived features only on games that already carry `[%eval]`; timing features on all games.
- **Models and scoring:** a game-level risk score updated move by move.
- **Streaming and serving:** Kafka, live and replay producers, streaming scorer, Stockfish workers for games without evals, dashboard (see Architecture).

Part 2 deliverables, deadline and build order are not fixed yet; add them here when known.

## Decisions already made (don't change without asking)

| Topic | Decision |
|---|---|
| Source | Lichess monthly standard rated dumps (`database.lichess.org`). Chess.com was dropped. |
| Months | Local: `2026-09`, first 2,500,000 lines of the dump = 124,914 games (last one truncated). Cluster: 3 most recent complete months (configurable list). |
| Time controls | Ingest ALL, store `time_class` column. Analysis starts with blitz + classical via filter. |
| Formats | PGN = games · JSON = Lichess `/api/users` profile responses · CSV = flattened player label table · SQL-generated = `ref.*` tables + baselines |
| Storage | Parquet lake (partitioned by `source_month`, `time_class`) + PostgreSQL 16. Raw JSON kept in a JSONB column. No MongoDB. |
| Labels | `positive` = `tosViolation: true` · `negative` = active, no flag · `unknown` = disabled / closed / not returned (kept, excluded from training). Always store `label_fetched_at`. Label every player in the local sample. |
| Engine evals | Parse existing `[%eval]` comments into nullable columns. No Stockfish in Part 1. |
| Privacy | Keyed hash IDs (see Privacy rules). |
| Reliability | "Solid": logging, data-quality checks, quarantine table, validation queries. Plus 60 s wait-and-retry on HTTP 429. |
| Code style | Python scripts run with `spark-submit` for the pipeline; notebooks only for exploration and validation demos. |

## Architecture

**Built in Part 1 (solid lines in diagram)**

```
PGN sample / dump ──► [parse_games.py  PySpark + python-chess] ──► Parquet lake
                                                                     ├─ games/
                                                                     ├─ moves/
                                                                     ├─ quarantine/
                                                                     └─ restricted/identity/   (username ↔ player_id)
Parquet lake ──► [load_postgres.py  Spark JDBC] ──► PostgreSQL core.games, core.moves, restricted.identity

restricted/identity ──► [collect_labels.py  Lichess POST /api/users, 300 ids/request]
        ──► data/raw/profiles/*.ndjson  (JSON source)  +  restricted.user_profiles_raw (JSONB)
        ──► [build_labels.py] ──► data/processed/player_labels.csv  (CSV source)
        ──► [load_labels.py  COPY] ──► core.player_labels

sql/generate_reference.sql ──► ref.rating_bands, ref.time_classes          (SQL-generated)
sql/generate_derived.sql   ──► core.players, core.baselines                 (SQL-generated)
sql/validation/*.sql       ──► validation report
```

**Part 2, now being built (dashed lines in diagram):** Kafka topic `lichess.moves` (one message per half-move, key = `game_id`); live producer from Lichess game-stream endpoints; replay producer reading `moves/` Parquet with original move timing (load testing); streaming scorer (`src/streaming/scorer.py`) keeping a running risk score per player per game against the baselines at `paths.scorer_baselines_dir`, writing topic `lichess.scores` and `lake/scores/live_game`; Stockfish workers for games without evals; dashboard.

## Repo layout

```
chess-cheat-detector/
├── CLAUDE.md
├── README.md
├── .env.example            # template; real .env is git-ignored
├── .gitignore              # .env, data/, logs/, .venv/, *.pgn, *.zst
├── requirements.txt
├── config/
│   ├── local.yaml          # paths, spark master local[4], driver memory 3g, months, sample size
│   └── cluster.yaml        # same keys; paths under paths.data_dir, Spark settings "auto" from the Slurm job
├── cluster/                # Slurm cluster: env.sh, parse*.sbatch, features.sbatch, baselines_provisional.sbatch, kafka_job.sh/.sbatch, consumer_scaling.sh, scorer_scaling.sh/.sbatch, spark_test.py/.sbatch, README.md (setup steps)
├── src/
│   ├── common/             # config.py, spark.py (session factory), logging_setup.py, ids.py (HMAC)
│   ├── ingestion/          # parse_games.py, collect_labels.py, build_labels.py
│   ├── db/                 # load_postgres.py, load_labels.py
│   ├── quality/            # checks.py
│   └── streaming/          # replay_producer.py, count_consumer.py, consumer_group.py, baselines_provisional.py, scorer.py, scorer_group.py (Kafka, Part 2)
├── sql/
│   ├── schema.sql          # schemas core, ref, restricted + all CREATE TABLE
│   ├── generate_reference.sql
│   ├── generate_derived.sql
│   └── validation/         # 01_row_counts.sql, 02_keys.sql, ...
├── notebooks/
│   ├── 01_dataset_analysis.ipynb   # charts for the report
│   └── 02_validation.ipynb         # runs validation queries, shows results
├── docs/
│   ├── data_dictionary.md
│   └── diagrams/           # architecture.drawio, er_diagram.drawio, pipeline.drawio + PNG exports
├── data/                   # git-ignored: raw/, lake/, processed/
└── logs/                   # git-ignored
```

## Environment constraints

- **Laptop (local runs):** MacBook Apple Silicon, **8 GB RAM**. Spark `local[4]`, `spark.driver.memory=3g`. Keep memory use low: no `collect()` / `toPandas()` on full tables, never cache large DataFrames.
- PostgreSQL 16 via Homebrew. **No Docker locally** (not enough RAM).
- Java 17, Python 3.11, PySpark 3.5.x (the most common version on university clusters).
- **Cluster:** Slurm (IE Sci-Tech), Spark `local[N]` inside one job, no HDFS/YARN, **no PostgreSQL server**. See Cluster rules. Everything environment-specific (paths, Spark master, memory, JDBC URL, months) lives in `config/*.yaml`. Code never hardcodes paths or URLs. Every step runs with `spark-submit src/... --config config/local.yaml`.
- PostgreSQL JDBC driver via `spark.jars.packages=org.postgresql:postgresql:42.7.4`.
- Cluster note: Hadoop can't split `.zst`, and the cluster has no HDFS. Full months are never decompressed whole (~210 GB): `src/ingestion/parse_chunked.py` streams the `.pgn.zst` from home through `zstd -dc` into ~20 GB chunks on node-local `/scratch`, cut at game boundaries, and parses them one at a time (`cluster/parse_chunked.sbatch`).

## Cluster rules (Slurm, IE Sci-Tech)

Setup steps for a new account are in `cluster/README.md`.

- You are on the LOGIN node `rust`. Never run Spark, parsing or any heavy compute here.
  Editing files, pip install, git and submitting jobs are fine.
- Every shell / job must start with: `source cluster/env.sh` from the repo root (Java 17, Python 3.11, venv in `~/chess/venv`, `SPARK_LOCAL_DIRS=~/chess/spark-tmp`).
- Compute nodes: fortran, haskell, julia (512 cores, 400–750 GB RAM each).
  Partitions: `cpu` (default, 7 days), `gpu` (3 days), `interactive` (8 h).
- Quick tests: `srun -p interactive --cpus-per-task=8 --mem=16G --time=00:30:00 python script.py`
- Real runs: sbatch scripts modelled on `cluster/spark_test.sbatch`.
- CPU limits are enforced: Spark runs `local[N]` with N = $SLURM_CPUS_PER_TASK;
  set spark.driver.memory (the JVM heap) to ~50% of the Slurm --mem for Python-heavy steps (Python code
  inside Spark tasks: RDD map/mapPartitions, UDFs; e.g. parse_games, parse_chunked), because their N Python
  workers live outside the heap, and ~75% for DataFrame-only steps (e.g. checks, load_postgres).
  With 75% the full-month parse lost a Python worker at the memory limit (job 28202).
- Monitor jobs: `squeue -u $USER`, `sacct -j <id>`, logs in `logs/<name>-<id>.out`.
- Storage: work inside your home folder (`~/chess` for the venv and scratch, `~/chess-cheat-detector` for the repo). Ask before downloading more than a few GB (storage
  location still to be confirmed with the admins). Develop on small 2013 Lichess months.
- Node-local scratch: every compute node has its own `/scratch` (fortran 7 TB NVMe, julia 3.5 TB NVMe, haskell 3.7 TB SATA),
  invisible from `rust` and the other nodes. Jobs use `/scratch/$USER/$SLURM_JOB_ID` for temporary files only
  (PGN chunks, `SPARK_LOCAL_DIRS`) and delete it on exit with a trap. Results always go to home.
- Submit jobs from a login shell (`bash -l`, or a normal SSH/VS Code terminal): `cluster/env.sh` needs the
  `module` command, which non-login shells (e.g. `ssh rust 'sbatch ...'`) don't define.

## Conventions

- Python 3.11, type hints, short docstrings, `logging` (never `print` in `src/`).
- Each step is a module with a `main()` and a `--config` CLI argument.
- **Idempotent:** rerunning a step for a month overwrites that month's partition (`spark.sql.sources.partitionOverwriteMode=dynamic`) or upserts in PostgreSQL. Never duplicates rows.
- Column names snake_case, all timestamps UTC.
- **Parquet timestamps are UTC.** Spark shows them in UTC (`get_spark` sets the session time zone), but plain Python `datetime` objects from `.collect()`/`.first()` are shown in the Mac's local time (Madrid). In pandas (`pd.read_parquet` or `toPandas()`, which uses Arrow via `get_spark`) the columns arrive tz-naive with UTC values: mark them with `df[col] = df[col].dt.tz_localize("UTC")`. Use `.dt.tz_convert("UTC")` only on columns that are already tz-aware; on tz-naive columns it raises a TypeError.
- SQL lives in `.sql` files, not long strings in Python.
- Spark for anything on full data; pandas only on small aggregates inside notebooks.
- Column names, types and constraints must match `docs/data_dictionary.md`. If code needs a change, update the dictionary in the same commit.

## Privacy rules (hard rules)

- `player_id = HMAC-SHA256(key=LICHESS_HASH_KEY, msg=username.lower())`, hex digest truncated to the first 16 characters. Implemented once in `src/common/ids.py`.
- Raw usernames may exist ONLY in: `data/lake/restricted/`, the PostgreSQL `restricted` schema, and `data/raw/profiles/*.ndjson`.
- Never in `core.*`, never in the CSV, never in logs, never in committed notebook outputs.
- The quarantine `raw_excerpt` must have the `[White ...]` and `[Black ...]` header lines removed before storing.
- `.env` and `data/` are never committed.

## Reliability (Solid level)

- Logging to console and `logs/<step>_<UTC timestamp>.log`: start/end, input/output row counts, duration, quarantined count.
- `src/quality/checks.py` runs after each step. **Hard checks** fail the step (PK uniqueness, required fields not null, `result` in the allowed set). **Soft checks** log warnings (Elo outside 400–3500, negative move times, games with 0 moves, BOT-title players).
- **Quarantine:** a game that fails parsing or a hard check goes to `quarantine/` with `error_stage`, `error_code`, `error_detail`. One bad game never crashes the job.
- **Lichess API:** one request at a time; on HTTP 429 wait 60 s, then retry, max 3 tries; set a descriptive `User-Agent`. **Cache:** skip usernames already in `restricted.user_profiles_raw` unless `--refresh`.

## Domain notes

- **Splitting PGN in Spark:** read with Hadoop `textinputformat.record.delimiter = "\n\n[Event "` and prepend `[Event ` back to every record except the first. Parse each game inside `mapPartitions` with `chess.pgn.read_game(io.StringIO(text))`.
- **game_id** = last path segment of the `Site` header (`https://lichess.org/abcd1234` → `abcd1234`).
- **Clock and eval comments:** after each move, e.g. `{ [%eval 0.17] [%clk 0:03:00] }`. In python-chess: `node.clock()` → seconds; `node.eval()` → PovScore, so use `.white().score()` for centipawns and `.white().mate()` for mate. Measured on the local sample (`data/raw/lichess_2026-09_sample.pgn`, 124,914 games): 124,516 have clocks, 13,003 have evals (~10%). The proposal's ~6% eval figure refers to the full Lichess database, not this sample.
- **time_class from TimeControl `base+inc`:** estimated duration = base + 40 × inc. Thresholds: ultrabullet < 30 s, bullet < 180, blitz < 480, rapid < 1500, classical ≥ 1500. `TimeControl "-"` → correspondence.
- **move_time_s** for ply p = clock_after(p − 2) − clock_after(p) + increment. For each side's first move, use base_seconds as the previous clock. First moves are noisy; that's acceptable and should be documented.
- **Termination** values include `Normal`, `Time forfeit`, `Abandoned` and `Rules infraction`. Keep as-is; Rules infraction may be a weak game-level signal later.
- **BOT accounts** (title `BOT`) can play rated games. Keep them, flag them, exclude them from human baselines.

## Part 1 build order (done; kept for reference)

1. Scaffolding: `requirements.txt`, `.gitignore`, `.env.example`, `config/local.yaml`, `src/common/*`
2. `sql/schema.sql`, applied to the local `chess` database
3. `parse_games.py` → Parquet `games`, `moves`, `quarantine`, `restricted/identity`
4. `quality/checks.py`, run against the Parquet output
5. `load_postgres.py` → `core.games`, `core.moves` (flag `postgres.load_moves`: true locally, false on cluster), `restricted.identity`
6. `collect_labels.py` → NDJSON files + `restricted.user_profiles_raw`
7. `build_labels.py` → `player_labels.csv`; `load_labels.py` → `core.player_labels` via COPY
8. `generate_reference.sql`, `generate_derived.sql`
9. `sql/validation/*.sql` + `notebooks/02_validation.ipynb`
10. `notebooks/01_dataset_analysis.ipynb` (charts for the report: games per time class, Elo distribution, eval/clock availability, move-time distribution by band, label distribution)
11. draw.io diagrams in `docs/diagrams/`

## Definition of done for Part 1 (delivered)

- A single `run_local.sh` runs the whole pipeline on the sample from a clean database
- Every table in `docs/data_dictionary.md` exists with matching columns and constraints
- All validation queries pass, and their results are saved for the report
- Rerunning the pipeline produces identical row counts (idempotency)
- No raw usernames outside the allowed locations
