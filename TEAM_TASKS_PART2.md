# Team tasks — Part 2 (Group Assessment 2)

Read this together with `CLAUDE.md`, `cluster/README.md` and `docs/data_dictionary.md` before starting.

---

## Where we are

- **Part 1 is delivered** (ingestion: PGN → Parquet lake → PostgreSQL, labels, SQL-generated tables).
- **The pipeline runs on the university Slurm cluster.** The first cluster run on the Part 1 sample reproduced Part 1 exactly: 124,913 games, 8,302,882 moves, 69,392 identity rows, and identical counts in all 14 data-quality checks.
- **Still waiting on the cluster admins** for where large data should live (one full month is about 30 GB compressed, about 210 GB decompressed PGN and about 30 GB of Parquet). Until then, everyone develops on the Part 1 sample.

## What Part 2 asks for (assessment brief)

The brief asks us to: state the insights and value behind the topic, show there is enough data, process it at scale, discuss the legal implications of using and combining the datasets, validate and explain the veracity and integrity of the data, show the solution can scale and uses suitable data structures, and deliver the results of the whole pipeline.

**Due: end of November 2026.** That gives us about three weeks, so the timeline and scope below are tight on purpose.

**Deliverables:** a ~40-page report · datasets and databases in native format · diagrams and figures · all source code · a 10–15 minute presentation at the end of the semester. We'll also be evaluated individually as jury members for other groups' presentations.

---

## Decisions

### Already decided

| Topic | Decision |
|---|---|
| Heavy processing | **Spark + Parquet on the cluster** for everything heavy: parsing, features, baselines, labels logic, scoring. |
| PostgreSQL | **Serving layer only.** Small result tables (baselines, labels summary, scores) are loaded into PostgreSQL at the end for the dashboard and the database deliverable. No PostgreSQL server on the cluster. |
| Porting | The label and baseline logic that is SQL in Part 1 (`build_labels`, `generate_derived.sql`) is ported to Spark. On the sample, the Spark version must give the same numbers as the Part 1 SQL. |
| Hash key | Everyone uses the **same `LICHESS_HASH_KEY`** so player IDs match everywhere. Santi sends it by private message. Never commit it or paste it in the group chat. |
| Data location | Each person works in their own cluster account on the sample until the admins give us a shared folder. Then `paths.data_dir` in `config/cluster.yaml` changes, and nothing else. |

### To decide this week (group)

1. **Cross-platform objective.** Our proposal says we'll test whether a detector trained on one platform generalises to another (chess.com), but we dropped chess.com in Part 1. Options:
   - (a) collect a small chess.com sample through its Published-Data API for this test only, or
   - (b) reframe the objective as generalisation across time controls and/or months on Lichess, and explain the change in the report.

   **Recommendation given the deadline: (b).** It needs no new data source, API or labels, and we explain the change honestly in the report.
2. **Months to process.** Proposal: at least 1 full recent month (Oct 2026), and up to 3 (Aug–Oct) only if storage and time allow.
3. **Dashboard tool.** Proposal: Streamlit (pure Python, reads PostgreSQL).

---

## Who does what (proposed — swap freely in the group chat)

| Person | Workstream | Why |
|---|---|---|
| **Santi** | A. Scale-up on the cluster + E1. Kafka/streaming infrastructure; overall coordination and PR reviews | Built the ingestion and the cluster setup |
| **Massimo** | B. Unlabelled layer: features and baselines in Spark | Wrote the Part 1 schema and SQL-generated tables |
| **Fede** | C. Labels at scale (+ chess.com sample if we choose option a) | Built the Part 1 labels |
| **Karim** | D. Model and evaluation | Did the Part 1 dataset analysis and figures |
| **Mohamed** | E2. Serving database + dashboard; report editor and presentation | Was report editor in Part 1 |

---

## Working rules (everyone)

- **Cluster rules from `CLAUDE.md`:** nothing heavy on the login node `rust`; quick tests with `srun -p interactive`, real runs with `sbatch`; every shell and job starts with `source cluster/env.sh`.
- **One branch per person** (`name/topic`), Pull Request into `main`, at least one review. Santi merges.
- **Never commit data,** `.env`, logs or anything under `data/`.
- **Privacy:** `restricted/` (usernames) never leaves the cluster and is never read outside the pipeline steps that need it (parser, label collector, identity load). No usernames in any output, figure, dashboard or the report: only hashed `player_id`.
- **Wording:** labels are proxies. In the dashboard and the report, say "flagged" or "high risk", never "cheater".
- **Interfaces:** each workstream writes its outputs to the paths and columns listed below. If you need to change one, tell the person who reads it first, and update `docs/data_dictionary.md` in the same PR.
- **Parse jobs:** don't run two `parse_games.py` jobs that finish at the same time (both rewrite `restricted/identity`).
- **Record every real cluster run:** job ID, cores, memory, elapsed time (`sacct -j <id> --format=JobID,Elapsed,MaxRSS,TotalCPU`). We need these numbers for the scalability section.

---

## Getting started on the cluster (everyone, ~30 min)

1. **Request a cluster account** with the same form Santi used (ask him for the link and the justification text).
2. Follow `cluster/README.md`: SSH config with ProxyJump, SSH key, modules, venv, `pip install -r requirements.txt`.
3. **VS Code:** install *Remote - SSH*, connect to `rust`, then `Git: Clone` this repo into your home folder and open it.
4. **Claude Code:** install it on `rust` (`curl -fsSL https://claude.ai/install.sh | bash`), or use the VS Code extension in the remote window. Set it to ask before acting.
5. Put the `.env` Santi sends you in the repo root and run `chmod 600 .env`.
6. **Build your own copy of the sample lake:** ask your Claude Code to fetch the Part 1 sample with the SETUP.md command inside a Slurm job, then run `cluster/parse.sbatch` on it. It takes about 1 minute and must give 124,913 games and 8,302,882 moves.

**Don't wait for your account.** Accounts can take a few days. Until yours arrives, develop on your laptop on the Part 1 sample with the Part 1 setup (SETUP.md). The code is the same; only the config changes (`config/local.yaml` vs `config/cluster.yaml`).

---

## Workstream A — Scale-up (Santi)

**Goal:** the full lake for the chosen months on the cluster, plus evidence that the pipeline scales.

- [ ] Get the storage answer from the admins; set `paths.data_dir`.
- [ ] Download + decompress job per month (`curl | zstd -dc` straight to disk, inside Slurm).
- [ ] Run `parse.sbatch` per month with 64–128 CPUs and enough `--time`; split a month into chunks if needed.
- [ ] Point `SPARK_LOCAL_DIRS` at fast node-local disk if the admins confirm there is one.
- [ ] **Scaling test:** the same input on 16 / 32 / 64 / 128 cores → chart of time vs cores (strong scaling) for the report.
- [ ] Data-quality results per month in `logs/` → summary table for the veracity/integrity section.

**Outputs:** `lake/games`, `lake/moves`, `lake/quarantine`, `restricted/identity` for all months (existing schemas).
**Done when:** all months parsed, DQ checks pass, runtimes recorded.

---

## Workstream B — Unlabelled layer: features and baselines (Massimo)

**Goal:** population norms per rating band × time class, and how far each player deviates from them. No labels needed.

- [ ] Port `generate_derived.sql` (`core.players`, `core.baselines`) to Spark. On the sample, the numbers must match the Part 1 tables.
- [ ] Per player per game, timing features from `lake/moves`: move-time mean, median, coefficient of variation, p10/p90; behaviour under time pressure (share of moves and time variation when the clock is low); number of moves.
- [ ] Within-session consistency: group a player's games into sessions (e.g. gap < 30 min) and measure how stable their timing is across a session.
- [ ] Accuracy relative to rating, **only on games with evals**: average centipawn loss from the existing `[%eval]` values.
- [ ] Baselines: mean, std and percentiles of every feature per `rating_band × time_class`.
- [ ] Per-player anomaly scores: percentile or z-score of each feature against their band, plus one combined anomaly score.

**Outputs:**
- `lake/features/game_player` — one row per (game_id, player_id): timing features, `has_evals`, accuracy features (nullable)
- `lake/features/player` — features aggregated per player
- `lake/baselines` — one row per (rating_band, time_class, feature)
- `lake/anomaly/player` — per-player deviations and combined anomaly score

**Done when:** it runs on the full months via sbatch and the sample numbers match Part 1.

---

## Workstream C — Labels at scale (Fede)

**Goal:** sanction labels for a large enough sample of accounts, without needing PostgreSQL.

- [ ] Make `collect_labels.py` work without the database: read player lists from Parquet, keep the raw-profile cache in Parquet/NDJSON under `restricted/` (no account is ever queried twice).
- [ ] Run collection as a long, low-resource Slurm job (compute nodes have internet). Keep the batch size of 300 IDs per request and the 60 s wait on HTTP 429.
- [ ] Sampling plan, agreed with Karim: a random sample of active players, plus extra high-anomaly players from B so there are enough positives. Document the sampling, because it affects the evaluation.
- [ ] Port `build_labels` (SQL over JSONB) to Spark. Same rules as Part 1: positive / negative / unknown, with `label_fetched_at`.
- [ ] Label statistics per rating band and time class (class imbalance figures for the report).
- [ ] If the group chooses option (a): a small chess.com sample via its Published-Data API with account status / fair-play fields.

**Outputs:** `lake/labels` — player_id, label, label_fetched_at, source.
**Done when:** labels exist for the agreed sample size and the Spark rules give the same result as Part 1 on the sample.

---

## Workstream D — Model and evaluation (Karim)

**Goal:** a supervised detector and an honest evaluation of it.

- [ ] Join `lake/features/player` (+ anomaly scores) with `lake/labels`; drop `unknown`.
- [ ] Split **by player**, never by game, to avoid leakage; if possible also hold out a later month.
- [ ] Models: logistic regression baseline, then gradient boosting (Spark MLlib, or scikit-learn on the per-player table if it fits in memory). Handle imbalance with class weights.
- [ ] Metrics: **PR-AUC, precision@k, recall at a fixed precision.** No accuracy.
- [ ] Feature-family comparison: timing-only vs engine-eval-only vs both (on the eval subset). This is one of the project's main findings.
- [ ] Generalisation test according to the group decision (chess.com, or across time controls/months).
- [ ] Optional, only if time allows: Stockfish on a small set of games without evals to check what engine features add.

**Outputs:** `lake/scores/player` — player_id, score, model_version; model files under `models/` (not committed if large); figures in `docs/figures/`.
**Done when:** evaluation figures and a results table are ready for the report.

---

## Workstream E — Streaming, serving database and dashboard

### E1. Kafka and streaming (Santi)

- [ ] Wait for the admins' answer on running services; otherwise run a Kafka broker (KRaft mode, Java 17 module) inside a Slurm job, with producers and consumers on the same node.
- [ ] Topic `lichess.moves`: one message per half-move, key = `game_id`, value = game_id, ply, player_id, color, clock, move_time, eval (nullable), timestamp.
- [ ] **Replay producer:** reads `lake/moves` and sends moves with their original timing (with a speed-up factor) for load testing.
- [ ] **Live producer:** reads ongoing games from Lichess's streaming endpoints.
- [ ] **Scorer:** a consumer that keeps a running risk score per game using B's baselines and the timing features; writes to topic `lichess.scores` and to a small table.
- [ ] **Load test:** messages per second and end-to-end latency at increasing replay speeds → chart for the report.

### E2. Serving database and dashboard (Mohamed)

- [ ] Load the small result tables into PostgreSQL (`serving` schema: baselines, labels summary, player scores, live scores), reusing the Part 1 load scripts.
- [ ] Dashboard (Streamlit, reads PostgreSQL): population baselines per band, distribution of anomaly scores, model results, and a live view of game risk scores. Only `player_id`, never usernames.
- [ ] Database deliverable: `pg_dump` + restore check + privacy check, as in Part 1.

**Done when:** the dashboard runs from the serving database, and the replay test shows live scores updating.

---

## Report (~40 pages) and presentation

Mohamed edits and keeps the structure; each person writes the sections for their workstream.

| Section | Owner |
|---|---|
| 1. Context, insights and expected value | Mohamed + Santi |
| 2. Data at scale: volume, sources, veracity and integrity (DQ results per month) | Santi |
| 3. Legal and ethical implications: CC0 data, GDPR and pseudonymisation, API terms, combining datasets, risk of false accusations | Fede + Mohamed |
| 4. Architecture for Part 2: lake + serving layer, cluster, streaming (updated diagram) | Karim + Santi |
| 5. Scale-up and scalability results | Santi |
| 6. Unlabelled layer: features, baselines, anomaly scores | Massimo |
| 7. Labelled layer: labels, sampling, class imbalance | Fede |
| 8. Model and evaluation | Karim |
| 9. Real-time scoring and load test | Santi |
| 10. Dashboard and insights | Mohamed |
| 11. Conclusions, limitations, further work | everyone (Mohamed edits) |

**Presentation (10–15 min):** one slide per section group, a live or recorded dashboard demo, and the scaling and evaluation charts.
**Jury duty:** everyone prepares 1–2 questions or insights for each other group.

---

## Internal timeline (due end of November)

| When | Milestone |
|---|---|
| **Mon 9 Nov** | Everyone has requested a cluster account and read this file. Decisions 1–3 agreed. |
| **Sun 15 Nov** | Every workstream works on the **sample** (cluster or laptop) and has a PR open. A: at least 1 full month parsed if storage is ready. B: features and baselines on the sample. C: label collection job running. E1: Kafka + replay producer working. |
| **Sat 21 Nov** | **Full-data runs done** (B on the full month(s), C labels collected). D: first model results. E1: scorer + load test. E2: serving DB + first dashboard. **Code freeze for new features.** |
| **Wed 25 Nov** | All report sections drafted with final numbers and figures. Presentation slides drafted. |
| **Fri 27 Nov** | Final report and presentation. Code cleaned up, README updated, deliverable package (database dump, datasets, code) built. |
| **Weekend 28–29 Nov** | Rehearsal, final checks, submission. |

## If we run short of time

Do these first:
- full-month run of the batch pipeline (A);
- features, baselines and anomaly scores (B);
- labels (C);
- model with PR-AUC and the timing vs engine comparison (D);
- replay producer → scorer → dashboard (E1/E2);
- the report.

Cut these first, in this order:
1. the optional Stockfish check (D);
2. the live Lichess producer (E1); the replay producer is enough to demonstrate streaming;
3. months 2 and 3 (A); one full month is enough to prove scale;
4. the chess.com sample (C), if the group chose option (a).

---

## Starter prompt for your Claude Code

Open the repo in VS Code (connected to `rust`), switch to your branch, start Claude Code and paste (replace NAME, WORKSTREAM and BRANCH):

> I'm NAME, one of five people on this group project, working on Part 2 on the university Slurm cluster. Read CLAUDE.md, cluster/README.md, docs/data_dictionary.md and TEAM_TASKS_PART2.md in full. My tasks are in "Workstream WORKSTREAM", and I'm working on branch BRANCH.
> Follow the cluster rules in CLAUDE.md: nothing heavy on rust, use srun for quick tests and sbatch for real runs, and ask me before downloads over 1 GB, deleting files, committing or pushing.
> Work through my tasks one at a time. For each one: explain the plan briefly, write the code, test it on the sample lake, then stop so I can review before committing.
> Only edit files that belong to my workstream. If a shared file (CLAUDE.md, the data dictionary, config/, requirements.txt) or another workstream's output format needs a change, tell me instead of editing it.
