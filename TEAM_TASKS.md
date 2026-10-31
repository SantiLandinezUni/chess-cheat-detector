# Team Tasks — Group Assessment 1

**Deadline: Tuesday 3 November 2026, 15:00.** Internal deadline: everything final by **Tuesday 12:00**.

Read this file, `CLAUDE.md` and `docs/data_dictionary.md` before starting. Claude Code reads all three.

## Who does what

| Person | Role | Branch | Code | Report sections |
|---|---|---|---|---|
| **Santi** | Ingestion lead, repo owner | `santi/ingestion` | build steps 1, 3, 4, 5 + `run_local.sh` | 4.1–4.3, 4.5, 4.7, 4.8 |
| **Fede** | Labels | `fede/labels` | build steps 6, 7 | 2.5, 4.4 |
| **Massimo** | Database & SQL | `massimo/sql` | build steps 2, 8, 9 | 4.6, 5 |
| **Karim** | Architecture & dataset analysis | `karim/architecture` | build steps 10, 11 | 2.1–2.4, 2.6, 3 |
| **Mohamed** | Report editor | `mohamed/report` | none (owns the Google Doc) | Executive summary, 1, 2.7, 6, references, appendices |

Build step numbers refer to "Build order" in `CLAUDE.md`.

## Deadlines between us (handoffs)

| When | What | From → To |
|---|---|---|
| **Sat 12:00** | `sql/schema.sql` merged into main | Massimo → Santi, Fede |
| **Sat 20:00** | Parquet lake + `core.games`, `core.moves`, `restricted.identity` loaded | Santi → Fede, Massimo, Karim |
| **Sun 13:00** | `core.player_labels` loaded from the CSV | Fede → Massimo |
| **Sun 20:00** | Validation results saved in `docs/validation_results/`; analysis charts saved in `docs/figures/` | Massimo, Karim → everyone |
| **Mon 20:00** | Every report section written in the Google Doc | everyone → Mohamed |
| **Mon night** | Clean full rerun of the pipeline from an empty database | Santi |
| **Tue 12:00** | Final report (PDF) + code zip ready | Mohamed, Santi |

If you're going to miss a handoff, say so in the group chat as early as possible.

## Ground rules

1. **Work on your own branch**: `git checkout -b <your-branch>`. When a task works, push and open a Pull Request into `main`. Santi reviews and merges. Run `git pull origin main` at the start of every session.
2. **Only edit your own files.** If a shared file (`CLAUDE.md`, the data dictionary, `config/`, `requirements.txt`) needs a change, say so in the group chat first.
3. **Never commit `.env` or anything in `data/`.**
4. **Everyone uses the same `LICHESS_HASH_KEY`.** Santi sends it to you by direct message. Put it in your own `.env`. Never paste it in the group chat or in GitHub.
5. **Column names come from `docs/data_dictionary.md`.** If your code needs a different column, the dictionary changes in the same PR, and you tell Massimo and Karim.
6. **Numbers in the report come from our pipeline run only**: logs, validation results, notebook outputs. Nothing from memory or from Claude without a source.
7. **GitHub email:** if a push is rejected because of your email, set your noreply address (github.com/settings/emails): `git config user.email "ID+USERNAME@users.noreply.github.com"`.

## Setup per person

Accept the GitHub invite, then: `git clone https://github.com/SantiLandinezUni/chess-cheat-detector.git`

| Person | Needs from `SETUP.md` |
|---|---|
| Santi | everything (done) |
| Fede | Python venv (step 3), PostgreSQL (step 2), `.env` (step 4) |
| Massimo | PostgreSQL (step 2), Python venv (step 3), `.env` (step 4); sample data (step 5) to test his SQL |
| Karim | Java 17 + Python venv + `.env` + sample data (steps 1, 3, 4, 5); draw.io (desktop app or the VS Code extension "Draw.io Integration") |
| Mohamed | nothing to install; reads the repo on GitHub and works in Google Docs |

**On Windows?** `SETUP.md` is written for Mac. Ask Claude Code: "Adapt SETUP.md steps X–Y for Windows and run them with me." Use Java 17, PostgreSQL 16 and Python 3.11 so we all run the same versions.

---

## Santi — Ingestion lead, repo owner

**Code**
- Build step 1: scaffolding (done or in progress)
- Build step 3: `src/ingestion/parse_games.py` → Parquet `games`, `moves`, `quarantine`, `restricted/identity`
- Build step 4: `src/quality/checks.py`
- Build step 5: `src/db/load_postgres.py` (needs Massimo's schema)
- `run_local.sh`: runs the full pipeline end to end from an empty database
- Review and merge everyone's PRs; final clean rerun on Monday night

**Report:** 4.1 pipeline overview, 4.2 PGN parsing, 4.3 derived fields, 4.5 PostgreSQL loading, 4.7 reliability, 4.8 performance (runtimes from the logs)

## Fede — Labels

**Code**
- Build step 6: `src/ingestion/collect_labels.py`. Calls `POST https://lichess.org/api/users` with up to 300 usernames per request, one request at a time; on HTTP 429 waits 60 s and retries (max 3); uses a descriptive `User-Agent`; skips usernames already fetched unless `--refresh`. Writes raw responses to `data/raw/profiles/*.ndjson` and to `restricted.user_profiles_raw` (JSONB).
- **Before Santi's identity table exists:** support a `--usernames user1,user2,…` test mode and test it on ~10 real usernames (e.g. take names from the first games in the sample PGN). Until the schema is merged, write only the NDJSON files.
- Build step 7: `src/ingestion/build_labels.py` → `data/processed/player_labels.csv` (no usernames, `player_id` only, label rules from the data dictionary §6), and `src/db/load_labels.py` → `core.player_labels` via `COPY`.

**Report:** 2.5 labels and their limitations, 4.4 label collection

## Massimo — Database & SQL

**Code**
- Build step 2: `sql/schema.sql`. Creates schemas `core`, `ref`, `restricted` and every table in the data dictionary with types, primary keys, foreign keys and CHECK constraints. It must run twice on the same database without errors (use `IF NOT EXISTS`). **Merge by Saturday 12:00.**
- Build step 8: `sql/generate_reference.sql` (`ref.rating_bands` with `generate_series`, `ref.time_classes`) and `sql/generate_derived.sql` (`core.players`, `core.baselines`)
- Build step 9: `sql/validation/*.sql` (one file per check: row counts Parquet vs PostgreSQL, primary key uniqueness, `n_plies` vs move rows, ranges and null rates, label distribution, every player has a label) + `notebooks/02_validation.ipynb`, which runs them all and saves the results to `docs/validation_results/`
- Also 2–3 example downstream queries for report section 5.3 (e.g. median move time by band and time class)

**Report:** 4.6 SQL-generated datasets, 5 resulting data structures and validation (with the ER diagram from Karim)

## Karim — Architecture & dataset analysis

**Code / docs**
- Build step 11 (start now, it doesn't need data): three draw.io diagrams in `docs/diagrams/`, each also exported as PNG:
  - `architecture.drawio`: full architecture from the "Architecture" section of `CLAUDE.md`; solid lines = built in Part 1, dashed lines = planned for Part 2
  - `pipeline.drawio`: the ingestion steps with inputs, outputs and script names
  - `er_diagram.drawio`: tables, keys and relationships from the data dictionary
- Build step 10 (from Saturday evening, once Parquet exists): `notebooks/01_dataset_analysis.ipynb` with charts saved to `docs/figures/`: games per time class, Elo distribution, clock and eval availability, move-time distribution by rating band, termination types, BOT games, label distribution (once Fede's labels exist)
- Keep `docs/data_dictionary.md` in sync with what the code actually produces

**Report:** 2.1–2.4 dataset sources, PGN structure, metadata analysis, sampling; 2.6 data dictionary summary; 3 data architecture design (component justification table, local vs cluster, roles, real-time design)

## Mohamed — Report editor

- Create the Google Doc from the report skeleton (Santi will share it) and give everyone edit access
- Write now (no results needed): executive summary draft, 1 motivation and expected insights, 2.7 licensing and privacy, references
- Write Monday: 6 conclusions and further work, appendix B (code index), appendix C (team roles), final executive summary
- Monday night: make the report read as one voice, check every number against `docs/validation_results/` and the logs, number every figure and table, check the 20-page limit, export to PDF
- **Ask the professor today:** do the cover, references and appendices count toward the 20 pages? What format should the submission be in (zip, repo link, both)?
- Use Claude.ai for writing help: create a Claude Project, upload `CLAUDE.md`, the data dictionary and the assignment brief, and work in it

---

## Starter prompt for your Claude Code

Open the repo in VS Code, switch to your branch, start Claude Code, and paste (replace NAME and BRANCH):

> I'm NAME, one of five people on this group project. Read CLAUDE.md, docs/data_dictionary.md and TEAM_TASKS.md in full. My tasks are in the "NAME" section of TEAM_TASKS.md, and I'm working on branch BRANCH.
> Work through my code tasks one at a time. For each one: explain the plan briefly, write the code, test it on the local data (or with a small test input if a teammate's output isn't ready yet), then stop so I can review before committing.
> Only edit files that belong to my tasks. If a shared file (CLAUDE.md, the data dictionary, config/, requirements.txt) needs a change, tell me instead of editing it.
