# Part 1 feedback → Part 2 actions

Part 1 grade: **95/100**. This file turns every point of the professor's feedback into an action, an owner and the report chapter where it must appear. **Before submission (14 Dec), every row must be ✅ and visible in the report.** Mohamed checks this list during the final edit.

Legend: 🔧 code/data change · 📝 report only · 🔭 "further work" (design sketch in the report, not built)

---

## 1. Writing and presentation

| # | Feedback | Action | Type | Owner | Report chapter | Status |
|---|---|---|---|---|---|---|
| 1.1 | Prose is sometimes telegraphic, "like release notes" | Add short transitions before and after every table and figure: what it shows and why it matters. One read-through pass per chapter | 📝 | Everyone for their chapters; Mohamed final pass | All | ☐ |
| 1.2 | Informal references ("as said in class") and external systems must be cited | Remove "as said in class"; cite course material properly. Add references for Lichess Irwin, Ken Regan's Intrinsic Performance Rating, Lichess API docs, GDPR articles | 📝 | Everyone; Mohamed checks References | All + References | ☐ |
| 1.3 | Diagrams appear at different scales in the PDF | One width for all diagrams, same font size, consistent captions; check the exported PDF page by page | 📝 | Mohamed (final layout); Karim (diagrams) | All | ☐ |

## 2. Project value

| # | Feedback | Action | Type | Owner | Report chapter | Status |
|---|---|---|---|---|---|---|
| 2.1 | Add a business case: "Integrity-as-a-Service" B2B API (tournament organisers, e-sports, proctoring) | 1–1.5 pages: target customers, what the API returns (risk tiers per game/player), a simple cost estimate from our **real cluster numbers** (core-hours per month from `sacct`: parsing, features, scoring) vs. value protected (prize pools, churn). State the assumptions clearly | 📝 | Mohamed + Santi (Santi provides the cost numbers) | Part 2 objectives and expected value | ☐ |
| 2.2 | Compare explicitly with Lichess Irwin and Ken Regan's IPR | Short comparison table: input data, method, labels, what each catches. Our angle: clock-time behaviour + engine evals, aimed at the "hybrid" cheater who uses an engine only at critical moments | 📝 | Karim | Model and evaluation (+ 1 line in objectives) | ☐ |
| 2.3 | Big Data V's: add **Variability** and **Value** | Value: raw PGN text → features → risk scores → dashboard. Variability: **measure it** — compare the baselines of Aug, Sep and Oct (see 3.3) and discuss rating inflation/deflation and time-control shifts | 📝 + 🔧 | Santi | Part 1 §1.2 (update) + Data at scale | ☐ |

## 3. Pipeline and storage

| # | Feedback | Action | Type | Owner | Report chapter | Status |
|---|---|---|---|---|---|---|
| 3.1 | Explain the use of IE's equipment in more depth | Full description of the cluster and how we used it: gateway → login node → compute nodes, Slurm partitions and limits (128 CPUs/user, 8 GB login-node limit), node-local `/scratch`, shared `/home`, chunked processing, memory rule (50%/75%), incidents (memory failure, 128-core hang, API throttling) and how we solved them, total core-hours used | 📝 | Santi | Scale-up and scalability + Part 1 deployment section (update) | ☐ |
| 3.2 | **Parquet partition skew**: partitioning by `time_class` gives huge blitz files and tiny correspondence files ("small file problem") | **Measure** file count and size per `source_month × time_class` partition on the cluster. Then fix the write so each partition gets files of a target size (e.g. ~128–512 MB: number of files proportional to partition size, `maxRecordsPerFile`), and compact the existing lake. Report before/after table (files, sizes, read time of a typical query) | 🔧 | Santi | Scale-up and scalability (+ advanced data structures) | ☐ |
| 3.3 | (supports 2.3) Variability across months | Run the provisional baselines per month (Aug/Sep/Oct) and compare: move-time percentiles per band, share of fast moves. One figure + one table | 🔧 | Santi (Massimo's baselines when final) | Data at scale | ☐ |
| 3.4 | **PostgreSQL bloat** on reruns; use `TRUNCATE` or `PARTITION BY LIST (source_month)` with detach/drop | Serving loader: partition serving tables by `source_month` (or by `run_id` for live scores) and replace a partition by detach/drop + attach instead of `DELETE`. Mention that Part 1's `load_postgres.py` would use the same pattern | 🔧 | Mohamed (in the PR #21 fixes) | Dashboard and serving layer (+ 1 line in Part 1 PostgreSQL section) | ☐ |

## 4. Model and evaluation

| # | Feedback | Action | Type | Owner | Report chapter | Status |
|---|---|---|---|---|---|---|
| 4.1 | Account-level labels = massive label noise at move level | Keep training at the **player level** (as planned) and explain why in the report; say clearly that labels are weak supervision | 📝 | Karim (+ Fede for the label description) | Labels; Model | ☐ |
| 4.2 | Two-stage architecture: unsupervised anomaly detector (e.g. Isolation Forest) → supervised/sequence model | Add an **Isolation Forest** on the per-player features as an unsupervised baseline and as an input feature to the supervised model; compare PR-AUC with and without it. Sequence model over windows of n moves → 🔭 further work | 🔧 + 🔭 | Karim | Model and evaluation | ☐ |
| 4.3 | Class imbalance: beyond PR-AUC, consider focal loss, SMOTE/undersampling, **calibrated probabilities and risk tiers** | Class weights (done) + undersampling comparison; **calibrate** the final model (isotonic or Platt) and define 3–4 risk tiers from calibrated probabilities; reliability plot. Focal loss/SMOTE → discuss briefly | 🔧 | Karim | Model and evaluation | ☐ |
| 4.4 | "Move-by-move suspicion timelines" | The scorer already emits a running score every N moves → dashboard page showing a game's risk score over its moves (hashed `game_key`, no usernames). Use it in the presentation demo | 🔧 | Mohamed (dashboard) + Santi (scorer export) | Real-time scoring; Dashboard | ☐ |

## 5. Other data sources (mostly further work)

| # | Feedback | Action | Type | Owner | Report chapter | Status |
|---|---|---|---|---|---|---|
| 5.1 | Structured: FIDE/USCF ratings, opening-book statistics (ECO) to separate memorised theory from engine calculation | Design sketch: join ECO statistics to measure "moves out of book"; FIDE link only where players declare it. Note privacy implications | 🔭 | Massimo | Further work | ☐ |
| 5.2 | Semi-structured: tournament crosstables (win-trading rings), rating history (sudden jumps) | Design sketch: NDJSON tournament streams → graph of repeated pairings; rating-history jumps as a feature. Note API limits (we hit them in Part 2) | 🔭 | Fede | Further work | ☐ |
| 5.3 | Unstructured: forum/Reddit/stream transcripts as weak-supervision event markers (NLP) | Design sketch + ethics/legal note (accusations in public text are not evidence; GDPR) | 🔭 | Fede + Mohamed | Further work + Legal | ☐ |

## 6. Already praised — keep and extend

- GDPR / HMAC pseudonymisation → extend with the public `game_id` link and our `game_key` hashing (Legal chapter, Fede + Mohamed).
- API pacing → update with the Part 2 story: throttling of the cluster's address, the API token, the documented authenticated limits (Labels chapter, Fede).
- Idempotency, data quality, quarantine → keep; add the per-month quality table (Santi).

## 7. Professor's showcase proposal

The professor suggested this project could become an IE showcase case study. Santi replies positively and offers to meet after submission.
