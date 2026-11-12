# Team tasks — Part 2 (Group Assessment 2)

Read this together with `CLAUDE.md`, `cluster/README.md` and `docs/data_dictionary.md` before starting.

---

## Where we are

- **Part 1 is delivered** (ingestion: PGN → Parquet lake → PostgreSQL, labels, SQL-generated tables).
- **The pipeline runs on the university Slurm cluster.** The first cluster run on the Part 1 sample reproduced Part 1 exactly (124,913 games, 8,302,882 moves, 69,392 identity rows, identical counts in all 14 data-quality checks).
- **October 2026 is fully parsed on the cluster:** 95,395,628 games and 6,275,657,696 moves in about 2 hours on 64 cores, a 41 GB Parquet lake, all hard data-quality checks passing. August and September are next.
- **Kafka streaming works:** replay producer + consumers on the October lake. At 100× replay speed the p99 latency is 8 ms; the broker handles about 1 million messages per second.
- **Only Santi has cluster access.** So everyone else develops on their laptop on the Part 1 sample, and Santi runs the full-data jobs on the cluster (see "How we work" below).

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
| Hash key | Everyone uses the **same `LICHESS_HASH_KEY`** so player IDs match everywhere. Never commit it or paste it in the group chat. |
| Data location | The full lake lives in Santi's cluster account; temporary files go on the nodes' local `/scratch`. Laptops only hold the Part 1 sample and the small result tables. |
| Who runs what | **Everyone writes and tests code on the sample on their laptop. Santi runs every full-data job on the cluster.** |
| Excluded games | BOT games and games with 0 moves are excluded from baselines, features and the model (October has about 494k BOT games and 407k empty games). |

### To decide this week (group)

1. **Cross-platform objective.** Our proposal says we'll test whether a detector trained on one platform generalises to another (chess.com), but we dropped chess.com in Part 1. Options:
   - (a) collect a small chess.com sample through its Published-Data API for this test only, or
   - (b) reframe the objective as generalisation across time controls and/or months on Lichess, and explain the change in the report.

   **Recommendation given the deadline: (b).** It needs no new data source, API or labels, and we explain the change honestly in the report.
2. **Months to process.** October is done; August and September are being processed now. We use all 3 if they finish cleanly.
3. **Dashboard tool.** Proposal: Streamlit (pure Python, reads PostgreSQL).

---

## Who does what (proposed — swap freely in the group chat)

| Person | Workstream | Why |
|---|---|---|
| **Santi** | A. Scale-up on the cluster + E1. Kafka/streaming; runs all cluster jobs, coordination and PR reviews | Built the ingestion and has cluster access |
| **Massimo** | B. Unlabelled layer: features and baselines in Spark | Wrote the Part 1 schema and SQL-generated tables |
| **Fede** | C. Labels at scale (+ chess.com sample if we choose option a) | Built the Part 1 labels |
| **Karim** | D. Model and evaluation | Did the Part 1 dataset analysis and figures |
| **Mohamed** | E2. Serving database + dashboard; report editor and presentation | Was report editor in Part 1 |

---

## How we work (laptop → PR → cluster run)

1. **Develop on your laptop on the Part 1 sample.** It has the same schemas as the full data, so code that works on the sample runs on the cluster with only a config change (`config/local.yaml` → `config/cluster.yaml`).
2. **Write a cluster job for your step:** `cluster/<your_step>.sbatch`, modelled on `cluster/parse.sbatch` (it starts with `source cluster/env.sh`, uses `spark-submit` with `PYTHONPATH=.`, and requests CPUs, memory and time). You can't run it, but write it so Santi only has to submit it.
3. **Open a PR.** A PR is only merged if your code ran on the sample and the PR says so (row counts, run time, any figures).
4. **Daily cluster run, around 20:00:** Santi pulls `main`, submits the sbatch files merged that day on the full data, and posts the results in the group chat. Small output tables (hashed IDs only, never `restricted/`) go to the shared Drive folder.
5. **If something fails on the full data,** Santi sends you the end of the log, you fix it on your branch, and it goes into the next day's run.

**Merge by 18:00 to make that day's run.** The schedule gives us about 10 full-data runs before the code freeze, so don't wait until the end to open your first PR.

---

## Working rules (everyone)

- **One branch per person** (`name/topic`), Pull Request into `main`, at least one review. Santi merges.
- **Never commit data,** `.env`, logs or anything under `data/`.
- **Privacy:** `restricted/` (usernames) never leaves the cluster and is never read outside the parser and the label collector. No usernames in any output, figure, dashboard or the report: only hashed `player_id`.
- **Wording:** labels are proxies. In the dashboard and the report, say "flagged" or "high risk", never "cheater".
- **Interfaces:** each workstream writes its outputs to the paths and columns listed below. If you need to change one, tell the person who reads it first, and update `docs/data_dictionary.md` in the same PR.
- **Cluster rules (for sbatch files) from `CLAUDE.md`:** every job starts with `source cluster/env.sh`; Python-heavy Spark steps give the Java heap about 50% of `--mem`, DataFrame-only steps about 75%; temporary files on `/scratch/$USER/$SLURM_JOB_ID`, deleted at the end.
- **Record every real cluster run** (Santi): job ID, cores, memory, elapsed time (`sacct -j <id> --format=JobID,Elapsed,MaxRSS,TotalCPU`). We need these numbers for the scalability section.

---

## Getting started (everyone, ~20 min)

1. **Update your laptop repo:** `git switch main && git pull`, then `pip install -r requirements.txt` in your `.venv`. Part 2 added a few packages.
2. **Check you have the Part 1 sample lake** under `data/lake/` (games, moves, quarantine). The schemas of `games` and `moves` didn't change in Part 2. If you don't have it, ask Santi for the zip (it never contains `restricted/`).
3. **Check your `.env`** has the same `LICHESS_HASH_KEY` as in Part 1.
4. **Claude Code:** open the repo in VS Code on your laptop, start Claude Code in ask-before-acting mode, and paste the starter prompt at the end of this file.
5. **Optional:** request a cluster account with the same form Santi used (ask him for the link and the justification text). It isn't needed for the workflow above.

---

## Workstream A — Scale-up (Santi)

**Goal:** the full lake for the chosen months on the cluster, plus evidence that the pipeline scales.

- [x] Storage: Santi's home on the shared drive for the lake; node-local `/scratch` for decompressed chunks and Spark temporary files.
- [x] Chunked download → decompress → parse inside one Slurm job, with cleanup on failure.
- [x] **October 2026:** 95.4M games, 6.28B moves, 2 h on 64 cores, DQ checks pass.
- [ ] August and September 2026 (chained jobs so they never run at the same time).
- [ ] **Scaling test:** the same chunk on 16 / 32 / 64 / 128 cores → chart of time vs cores (strong scaling).
- [ ] Data-quality results per month → summary table for the veracity/integrity section.
- [ ] Run every merged workstream's sbatch on the full data (daily run).

**Outputs:** `lake/games`, `lake/moves`, `lake/quarantine`, `restricted/identity` for all months (existing schemas).

---

## Workstream B — Unlabelled layer: features and baselines (Massimo)

**Goal:** population norms per rating band × time class, and how far each player deviates from them. No labels needed.

- [ ] Port `generate_derived.sql` (`core.players`, `core.baselines`) to Spark. On the sample, the numbers must match the Part 1 tables.
- [ ] Per player per game, timing features from `lake/moves`: move-time mean, median, coefficient of variation, p10/p90; behaviour under time pressure (share of moves and time variation when the clock is low); number of moves. Handle negative move times (the "add time" effect) as in Part 1.
- [ ] Within-session consistency: group a player's games into sessions (e.g. gap < 30 min) and measure how stable their timing is across a session.
- [ ] Accuracy relative to rating, **only on games with evals**: average centipawn loss from the existing `[%eval]` values.
- [ ] Baselines: mean, std and percentiles of every feature per `rating_band × time_class`. Exclude BOT games and games with 0 moves.
- [ ] Per-player anomaly scores: percentile or z-score of each feature against their band, plus one combined anomaly score.
- [ ] `cluster/features.sbatch` for the full months. Plan for billions of moves: aggregate per game first, and avoid collecting anything to the driver.

**Outputs:**
- `lake/features/game_player` — one row per (game_id, player_id): timing features, `has_evals`, accuracy features (nullable)
- `lake/features/player` — features aggregated per player
- `lake/baselines` — one row per (rating_band, time_class, feature)
- `lake/anomaly/player` — per-player deviations and combined anomaly score

**Done when:** the sample numbers match Part 1, and Santi's full-data run succeeds.
**Blocks:** C (sampling of high-anomaly players), D (features), E1 (the scorer needs the baselines). **First PR by Fri 13 Nov.**

---

## Workstream C — Labels at scale (Fede)

**Goal:** sanction labels for a large enough sample of accounts, without needing PostgreSQL.

- [ ] Make `collect_labels.py` work without the database: read player lists from Parquet, keep the raw-profile cache in Parquet/NDJSON under `restricted/` (no account is ever queried twice).
- [ ] Keep the batch size of 300 IDs per request and the 60 s wait on HTTP 429. Make the job resumable: if it stops, rerunning it continues where it left off.
- [ ] Sampling plan, agreed with Karim: a random sample of active players, plus extra high-anomaly players from B so there are enough positives. Document the sampling, because it affects the evaluation.
- [ ] Port `build_labels` (SQL over JSONB) to Spark. Same rules as Part 1: positive / negative / unknown, with `label_fetched_at`.
- [ ] Label statistics per rating band and time class (class imbalance figures for the report).
- [ ] `cluster/labels.sbatch`: a long, low-resource job (a few CPUs, many hours). **Santi runs it,** because the usernames in `restricted/` stay on the cluster. Test locally with your Part 1 data.
- [ ] If the group chooses option (a): a small chess.com sample via its Published-Data API with account status / fair-play fields.

**Outputs:** `lake/labels` — player_id, label, label_fetched_at, source.
**Done when:** labels exist for the agreed sample size and the Spark rules give the same result as Part 1 on the sample.

---

## Workstream D — Model and evaluation (Karim)

**Goal:** a supervised detector and an honest evaluation of it.

- [ ] Develop on the sample now; when B and C have run on the full data, Santi sends you `lake/features/player`, `lake/anomaly/player` and `lake/labels` (hashed IDs only, small) and you train on your laptop.
- [ ] Join the per-player features (+ anomaly scores) with the labels; drop `unknown`.
- [ ] Split **by player**, never by game, to avoid leakage; if possible also hold out a later month.
- [ ] Models: logistic regression baseline, then gradient boosting (scikit-learn on the per-player table). Handle imbalance with class weights.
- [ ] Metrics: **PR-AUC, precision@k, recall at a fixed precision.** No accuracy.
- [ ] Feature-family comparison: timing-only vs engine-eval-only vs both (on the eval subset). This is one of the project's main findings.
- [ ] Generalisation test according to the group decision (chess.com, or across time controls/months).
- [ ] Export the trained model in a form the streaming scorer (E1) can load.
- [ ] Optional, only if time allows: Stockfish on a small set of games without evals to check what engine features add.

**Outputs:** `lake/scores/player` — player_id, score, model_version; model files under `models/` (not committed if large); figures in `docs/figures/`.
**Done when:** evaluation figures and a results table are ready for the report.

---

## Workstream E — Streaming, serving database and dashboard

### E1. Kafka and streaming (Santi)

- [x] Single-node Kafka 4.4.0 (KRaft) inside one Slurm job (`cluster/kafka_job.sh`), data on `/scratch`, cleaned up on any exit.
- [x] Topic `lichess.moves`: one message per half-move, key = `game_id`. The message schema is in `docs/data_dictionary.md` §8 (includes time_class, elo and eval_mate).
- [x] **Replay producer** with original timing ÷ `--speed`; skips BOT games, games with 0 moves and correspondence.
- [x] **Load test:** speed 100 → p99 8 ms; no pacing → ~560k msg/s; broker ceiling ~1.06M rec/s; consumer scaling 1/2/4/8 in `docs/validation_results/consumer_scaling.csv`.
- [ ] **Scorer:** a consumer that keeps a running risk score per game using B's baselines (and D's model if ready); writes to topic `lichess.scores` and to a small table for the dashboard.
- [ ] Optional: live producer from Lichess's streaming endpoints.

### E2. Serving database and dashboard (Mohamed)

- [ ] On your laptop: load the small result tables into PostgreSQL (`serving` schema: baselines, labels summary, player scores, live scores), reusing the Part 1 load scripts. Start with tables built from the sample; switch to the full-data tables Santi sends.
- [ ] Dashboard (Streamlit, reads PostgreSQL): population baselines per band, distribution of anomaly scores, model results, and a view of game risk scores from a scorer export. Only `player_id`, never usernames.
- [ ] Database deliverable: `pg_dump` + restore check + privacy check, as in Part 1.

**Done when:** the dashboard runs from the serving database with the full-data results.

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
| **Fri 13 Nov** | Decisions 1–3 agreed. B's first PR (features on the sample) merged, so it runs on the full data that evening. |
| **Sun 15 Nov** | Every workstream works on the sample and has a PR open with its sbatch file. A: all 3 months parsed. C: label collection job running on the cluster. |
| **Sat 21 Nov** | **Full-data runs done** (B features and baselines, C labels). D: first model results. E1: scorer. E2: serving DB + first dashboard. **Code freeze for new features.** |
| **Wed 25 Nov** | All report sections drafted with final numbers and figures. Presentation slides drafted. |
| **Fri 27 Nov** | Final report and presentation. Code cleaned up, README updated, deliverable package (database dump, datasets, code) built. |
| **Weekend 28–29 Nov** | Rehearsal, final checks, submission. |

## If we run short of time

Do these first:
- full-month run of the batch pipeline (A) ✅;
- features, baselines and anomaly scores (B);
- labels (C);
- model with PR-AUC and the timing vs engine comparison (D);
- replay producer ✅ → scorer → dashboard (E1/E2);
- the report.

Cut these first, in this order:
1. the optional Stockfish check (D);
2. the live Lichess producer (E1); the replay producer is enough to demonstrate streaming;
3. months 2 and 3 (A); one full month is enough to prove scale;
4. the chess.com sample (C), if the group chose option (a).

---

## Starter prompt for your Claude Code

Open the repo in VS Code on your laptop, switch to your branch, start Claude Code in ask-before-acting mode and paste (replace NAME, WORKSTREAM and BRANCH):

> I'm NAME, one of five people on this group project, working on Part 2. Read CLAUDE.md, cluster/README.md, docs/data_dictionary.md and TEAM_TASKS_PART2.md in full. My tasks are in "Workstream WORKSTREAM", and I'm working on branch BRANCH.
> I develop on this laptop on the Part 1 sample (config/local.yaml). Santi runs the full-data jobs on the university Slurm cluster, so for each step also write a `cluster/<step>.sbatch` modelled on cluster/parse.sbatch and the cluster rules in CLAUDE.md. Don't try to run it.
> The full data is about 750 times larger than the sample (95M games and 6.3B moves per month), so write Spark code that scales: no collecting to the driver, no pandas on full tables, aggregate early.
> Work through my tasks one at a time. For each one: explain the plan briefly, write the code, test it on the sample, then stop so I can review before committing. Ask me before deleting files, committing or pushing.
> Only edit files that belong to my workstream. If a shared file (CLAUDE.md, the data dictionary, config/, requirements.txt) or another workstream's output format needs a change, tell me instead of editing it.
