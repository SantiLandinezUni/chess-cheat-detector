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
