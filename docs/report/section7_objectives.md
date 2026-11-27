# 7. Part 2 objectives and expected value

*Draft, Mohamed, 19 Nov 2026. Numbering follows the master report (Part 1 = chapters 1–6, Part 2 = chapters 7–17). Figures from the first scorer export (60,000 October games); update after the full-data run.*

*[Co-written with Santi: Santi to check the scale and streaming objectives against the final runs.]*

Part 2 turns the Part 1 dataset into a system that can rank live games by how unusual the players' timing is. The assessment brief asks us to show the insight and value behind the topic, prove there is enough data, process it at scale, validate its veracity and integrity, discuss the legal implications of combining the datasets, and show that the solution scales and uses suitable data structures. Our objectives map directly onto those requirements.

## 7.1 Objectives

1. **Process the data at scale.** Run the Part 1 pipeline unchanged on the university Slurm cluster over the three most recent complete months (August to October 2026), with the same data-quality checks applied to every month.
2. **Describe normal play without labels.** Build population baselines of move-time behaviour for every rating band and time class, and an anomaly score that measures how far each player sits from their own baseline.
3. **Collect labels at scale and train a model.** Extend the Part 1 label rules to the players of all three months, train a supervised model on account-level labels, and evaluate it with precision and recall rather than accuracy, given the class imbalance found in Part 1 (0.82% positive).
4. **Test whether the model generalises.** Train on some months and time controls and test on others. This replaces the cross-platform test against Chess.com that the Part 1 proposal mentioned (section 7.2).
5. **Score games in real time.** Replay stored games move by move through Kafka and update a risk score for each player after every move, and measure latency and throughput.
6. **Serve the results.** Keep PostgreSQL as a small serving layer and build a dashboard on it for a reviewer who needs to know which games to look at first.
7. **Assess the legal and ethical risks** of combining public game records, account status data and behavioural scores about identifiable people.

## 7.2 Reframing the generalisation objective

The original plan tested generalisation across platforms, training on Lichess and testing on Chess.com. We reframed it as a test across months and time controls within Lichess, for three reasons. First, Chess.com was dropped as a source in Part 1, so adding it now would mean a second ingestion pipeline with a different API, data format and terms of use. Second, the two platforms label accounts differently, so a drop in performance could not be separated from a change in what the label means. Third, the three-week deadline left no room for a second source. The reframed test still answers the practical question a fair-play team would ask: does a model trained on past months keep working on the next one, and on time controls it saw less of?

*[Editor note: confirm the group vote on this decision before final.]*

## 7.3 Expected value

For a platform's fair-play team, the value is a review queue. Manual review cannot cover tens of millions of games a month, and account bans arrive after the damage is done. A ranked list of the most unusual games, available while they are being played, lets reviewers spend their time where it is most likely to matter. Because every score is measured against players of the same rating and time control, the ranking does not simply flag strong or fast players.

For the course, the value is a complete, working data lifecycle: ingestion at full scale, validation, a streaming layer and a serving layer, with each design choice justified and measured.

Table 17 links each objective to the evidence that shows whether it was met.

*Table 17. Part 2 objectives and how they are evaluated*

| Objective | Evidence | Chapter |
|---|---|---|
| 1. Process at scale | Rows per month, data-quality results per month, runtimes and core counts from the job records | 8, 9 |
| 2. Unlabelled layer | Baseline cells per band and time class; anomaly score distribution | 10 |
| 3. Labels and model | Label distribution at scale; PR-AUC against a baseline | 11, 12 |
| 4. Generalisation | Performance on held-out months and time controls | 12 |
| 5. Real-time scoring | Score latency (p99) and messages per second in the load test | 13 |
| 6. Serving and dashboard | Dashboard running from the serving database; restore check of the database dump | 16 |
| 7. Legal assessment | Risks identified and the safeguards applied | 15 |

## 7.4 Business case: Integrity-as-a-Service

**The problem.** Fair play is now an operating cost for every online chess organiser. Chess.com closes more than 100,000 accounts a month for fair-play violations, about 0.3% of its monthly active users, and has a fair-play team of around 30 people (Svensen, 2025). Lichess builds its own detection (Irwin and Kaladin). A national federation, a club league or a company running an online event can afford neither, and falls back on webcams, manual review of a few suspicious games, or nothing.

**The product.** Our pipeline can be offered as a business-to-business API that scores games for organisers without their own fair-play team.

- *Input:* games in PGN with clock times (`%clk`), sent in a batch after an event or streamed move by move during it.
- *Output per game and player:* the risk score (0–5), a tier (*not scored*, *normal*, *high risk* at the dashboard's threshold of 1.0), the confidence, and the contribution of each of the three timing terms, so a reviewer can see why a game was flagged.
- *Output per player:* games scored, number of high-risk games, and the anomaly percentile within the rating band and time class (chapter 10) once the player has enough games.
- *Live mode:* the running score after every move (chapter 13 and the dashboard's suspicion timeline), so an arbiter can watch a board while the round is being played.

The API ranks games for human review and never closes an account by itself. This is a product choice and a legal one: GDPR Article 22 restricts decisions based solely on automated processing, so the organiser's arbiter decides (chapter 15). Players are identified only by hashed IDs on our side; the organiser keeps the link to real accounts.

**Target customers.**

| Segment | Examples | What they need |
|---|---|---|
| Tournament organisers and federations | National federations' online championships, club leagues, school competitions | Post-event screening of every game and a short review list for the arbiter |
| E-sports and prize events | Platforms and streamers running cash-prize online events | Live risk during the event and fast screening before prizes are paid |
| Proctoring and education | Online proctoring providers, chess academies with rated exams | A behavioural signal to combine with camera proctoring |

**Cost from our own cluster runs.** The estimate uses the Slurm accounting of our real jobs, not a guess. The whole project used 1,510.5 core-hours in 91 jobs <!-- source: docs/validation_results/cluster_usage.csv (TOTAL) -->, including test and scaling runs that a product would not repeat. Table 18 keeps only the production steps and divides them by the games they processed.

*Table 18. Compute per million games, by pipeline step (Slurm `sacct`)*

| Step | Core-hours | Games processed | Core-hours per million games |
|---|---|---|---|
| Parsing (PGN → Parquet), three full months | 467.7 | 276.9 M | 1.69 |
| Features (one month, 2026-10) | 35.0 | 95.4 M | 0.37 |
| Baselines (one month, 2026-08) | 36.3 | 91.9 M | 0.39 |
| Streaming scorer, full day 2026-10-22 | 32.7 | 3.06 M | 10.70 |
| **Total** | | | **13.15** |

<!-- source: docs/validation_results/cluster_usage.csv (parse (full month), features, baselines (per month, feedback 3.3), serving_v2 replay); games: docs/validation_results/data_at_scale_by_month.csv (time_class = all); 3,055,849 games: ~/exports/serving_v2/manifest.json -->

Scoring dominates, because the streaming scorer keeps per-game state and handles every move as a message; the batch steps are cheap per game. At an on-demand cloud price of $0.0892 per hour for a 2-vCPU instance (AWS c7i.large, us-east-1; AWS, 2026), one core-hour costs about $0.045. Scoring one million games then costs about **$0.48** in compute, and the full pipeline about **$0.59**. Baselines are refreshed once a month, not per customer, so a customer who only sends its own events pays mainly for the scoring step. Storage of the serving tables is negligible at this volume. The real costs are engineering, support and liability, which we do not price here.

**Value protected.** The value side only needs the order of magnitude:

- *Prize money.* Chess.com's Titled Tuesday alone has a weekly prize fund of $10,000 (Chess.com, 2026), more than $500,000 a year. One undetected cheater taking a top prize in one event costs more than scoring hundreds of millions of games.
- *Churn.* No platform publishes how many players leave because of cheating, so we state an assumption instead of a figure. An organiser with 10,000 paying members at €5 a month that loses an extra 1% of them in a year because of fair-play problems loses €6,000 of revenue. Scoring all of that organiser's games costs a few euros.
- *Reviewer time.* The dashboard reduces 3 million games a day to a ranked list. Arbiters review the top tens or hundreds of games instead of sampling at random.

**Pricing.** A simple price per 1,000 games scored (for example €1 per 1,000 games, with a monthly minimum for live events) would cover compute more than a thousand times. The price would be set by support and liability, not by the cluster.

**Assumptions and limits.**

- Compute cost uses on-demand cloud prices. A university cluster or reserved instances would be cheaper.
- The score is a ranking heuristic, not a probability (chapter 12). The product is sold as *risk tiers for review*, not as "cheat detection".
- Timing features need clock data; games without `%clk` cannot be scored.
- Our baselines are built from Lichess play. A paying customer would need an evaluation on their own player population before relying on the tiers.
- The churn and pricing figures are illustrative assumptions, not market research.

*References to add to the bibliography:*
Svensen, T. J. (2025, July 15). *100,000 Accounts Closed Per Month As Chess.com Eyes New Anti-Cheating Measures.* Chess.com. https://www.chess.com/news/view/100k-accounts-closed-anti-cheating-systems-improve
Chess.com. (2026). *Titled Tuesday Now Bigger Than Ever With $10,000 In Prizes Every Week.* https://www.chess.com/news/view/titled-tuesday-new-10k-prize-fund
Amazon Web Services. (2026). *Amazon EC2 On-Demand Pricing.* https://aws.amazon.com/ec2/pricing/on-demand/
Regulation (EU) 2016/679 (General Data Protection Regulation), Article 22.
