# Chess Cheat Detector — Big Data Technologies, Group Assessment 1

## What this project is

A distributed pipeline to detect engine assistance and rating manipulation in online chess, using Lichess open data. The long-term goal (Part 2) is a **move-by-move cheating risk score for live Lichess games**.

**This deliverable (Part 1) is ONLY data generation + ingestion.** Due **Tue 3 Nov 2026, 15:00**. It requires:
- a dataset including at least one CSV, one JSON and one SQL-generated dataset
- a data dictionary (`docs/data_dictionary.md` — the source of truth for every table and column)
- an architecture design with justification for every component
- an ingestion pipeline (extract from files → load into databases) that is reliable
- validation queries proving the data landed correctly and is queryable

Do **not** build models, Kafka, Stockfish analysis or a dashboard in Part 1. Design for them (they appear dashed in the architecture diagram), don't implement them.

## Decisions already made (don't change without asking)

| Topic | Decision |
|---|---|
| Source | Lichess monthly standard rated dumps (`database.lichess.org`). Chess.com was dropped. |
| Months | Local: `2026-09`, first ~100k games only. Cluster: 3 most recent complete months (configurable list). |
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

**Planned for Part 2 (dashed lines in diagram):** Kafka topic `lichess.moves` (one message per half-move, key = `game_id`); live producer from Lichess game-stream endpoints; replay producer reading `moves/` Parquet with original move timing (load testing); streaming scorer keeping a running game-level risk score; Stockfish workers for games without evals; dashboard.

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
│   └── cluster.yaml        # same keys, cluster values (TBD — cluster type unknown)
├── src/
│   ├── common/             # config.py, spark.py (session factory), logging_setup.py, ids.py (HMAC)
│   ├── ingestion/          # parse_games.py, collect_labels.py, build_labels.py
│   ├── db/                 # load_postgres.py, load_labels.py
│   └── quality/            # checks.py
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

- **Dev machine:** MacBook Apple Silicon, **8 GB RAM**. Spark `local[4]`, `spark.driver.memory=3g`. Keep memory use low: no `collect()` / `toPandas()` on full tables, never cache large DataFrames.
- PostgreSQL 16 via Homebrew. **No Docker locally** (not enough RAM).
- Java 17, Python 3.11, PySpark 3.5.x (the most common version on university clusters).
- **Target cluster type is unknown.** Everything environment-specific (paths, Spark master, memory, JDBC URL, months) lives in `config/*.yaml`. Code never hardcodes paths or URLs. Every step runs with `spark-submit src/... --config config/local.yaml`.
- PostgreSQL JDBC driver via `spark.jars.packages=org.postgresql:postgresql:42.7.4`.
- Cluster note: Hadoop may not read `.zst` natively. Plan: decompress once into HDFS (`zstd -dc file.zst | hdfs dfs -put - path.pgn`). Document this; don't solve it now.

## Conventions

- Python 3.11, type hints, short docstrings, `logging` (never `print` in `src/`).
- Each step is a module with a `main()` and a `--config` CLI argument.
- **Idempotent:** rerunning a step for a month overwrites that month's partition (`spark.sql.sources.partitionOverwriteMode=dynamic`) or upserts in PostgreSQL. Never duplicates rows.
- Column names snake_case, all timestamps UTC.
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
- **Clock and eval comments:** after each move, e.g. `{ [%eval 0.17] [%clk 0:03:00] }`. In python-chess: `node.clock()` → seconds; `node.eval()` → PovScore, so use `.white().score()` for centipawns and `.white().mate()` for mate. About 6% of games have evals; most recent games have clocks.
- **time_class from TimeControl `base+inc`:** estimated duration = base + 40 × inc. Thresholds: ultrabullet < 30 s, bullet < 180, blitz < 480, rapid < 1500, classical ≥ 1500. `TimeControl "-"` → correspondence.
- **move_time_s** for ply p = clock_after(p − 2) − clock_after(p) + increment. For each side's first move, use base_seconds as the previous clock. First moves are noisy; that's acceptable and should be documented.
- **Termination** values include `Normal`, `Time forfeit`, `Abandoned` and `Rules infraction`. Keep as-is; Rules infraction may be a weak game-level signal later.
- **BOT accounts** (title `BOT`) can play rated games. Keep them, flag them, exclude them from human baselines.

## Build order (do these in order, one at a time; test each on the sample before moving on)

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

## Definition of done for Part 1

- A single `run_local.sh` runs the whole pipeline on the sample from a clean database
- Every table in `docs/data_dictionary.md` exists with matching columns and constraints
- All validation queries pass, and their results are saved for the report
- Rerunning the pipeline produces identical row counts (idempotency)
- No raw usernames outside the allowed locations
