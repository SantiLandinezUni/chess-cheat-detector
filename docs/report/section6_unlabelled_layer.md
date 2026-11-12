# 6. Unlabelled layer: features, baselines and anomaly scores

*Draft, Massimo, 12 Nov 2026. Figures in this draft come from the 2026-09 sample (124,914 games). Replace them with the full-data numbers after the cluster run of `cluster/features.sbatch`.*

## 6.1 Purpose

Labels from the Lichess `tosViolation` flag are sparse, noisy proxies (section 7). The unlabelled layer does not need them: it describes how the population of human players behaves in each rating band (200-point bands) × time class. It then measures how far each player sits from their own peer group. Its outputs are used in three places: as model features (section 8), to over-sample high-anomaly accounts for labelling (section 7), and as the reference distributions for the real-time scorer (section 9).

## 6.2 Pipeline

All steps are PySpark jobs over the Parquet lake, run in one Slurm job (`cluster/features.sbatch`). Every step aggregates before it joins, so no step ever moves billions of individual moves through a join or to the driver.

| Step | Output | Grain |
|---|---|---|
| `derived.py` | `derived/players`, `derived/baselines_part1` | player; band × time class |
| `game_player.py` | `features/game_player` | game × player |
| `player.py` | `features/player` | player × time class |
| `baselines.py` | `baselines` | level × band × time class × feature |
| `anomaly.py` | `anomaly/player` | player × time class |

**Validation of the port.** The Part 1 tables `core.players` and `core.baselines` were SQL in PostgreSQL. Their Spark port gives the same results on the sample: 69,392 players, 55 baseline cells and 8,222,392 moves, with every value equal up to float rounding (largest absolute difference 3 × 10⁻⁶). On the full data, percentiles and distinct counts switch to their approximate versions (`percentile_approx`, `approx_count_distinct`), because exact percentiles over billions of moves do not fit in memory.

## 6.3 Population

BOT accounts and games with 0 moves are excluded from every table. On the October dump that removes about 494k BOT games and 407k empty games. Negative move times are excluded from the timing statistics. These are real values in the data, not parse errors: lag compensation produces small negatives, and an opponent adding time produces large ones.

## 6.4 Per-game features

For each player in each game:

- **Move-time distribution:** mean, standard deviation, coefficient of variation (cv = sd / mean), 10th, 50th and 90th percentiles, and the share of moves under 2 s.
- **Clock management under time pressure:** the share of moves played with a *low clock*, defined as a clock after the move below 10% of the starting time. Also the mean and cv of move time on those moves.
- **Accuracy (games with `[%eval]` only):** average centipawn loss (ACPL). A move's loss is the drop in the mover's evaluation versus the previous ply, from the mover's point of view, floored at 0. Evaluations are capped at ±1000 cp before differencing, so the loss of an already decisive position does not dominate the average. Mate scores and moves without a centipawn evaluation on the previous ply (including ply 1) are skipped. About 10% of game-player rows carry evals.

On the sample: 247,547 rows from 123,895 games; average move time 4.5 s; average ACPL 63.9. The baselines behave as expected: ACPL falls from 98 in the lowest blitz band to about 33 in the highest, and the cv of move time rises with rating.

## 6.5 Player features and sessions

Per-game features are averaged per player **within one time class** (mean and sd across the player's games). A player's blitz and bullet habits are never mixed.

**Sessions** capture within-session consistency. A player's games are taken in start order, and a game opens a new session when it starts more than 30 minutes after the previous game's estimated end. The end is estimated as the start time plus twice the player's own time used, because the opponent's clock use is not in the player's row. For each session with two or more games in a time class, we compute the cv of the per-game mean move time across the session. A low value means the player's tempo is the same game after game. On the sample: 76,623 sessions, 43,558 of them with 2+ games.

## 6.6 Baselines

`baselines` holds one row per (level, band, time class, feature) with count, mean, sd and nine percentiles (p01 … p99):

- **Game level:** the 12 per-game features. The real-time scorer compares a live game with these.
- **Player level:** the player averages, spreads and session features. Only players with at least 5 games in the time class are included, because means of one or two games are mostly noise.

The Part 1 move-time table (`baselines_part1`) is kept for continuity.

## 6.7 Anomaly scores

For players with at least 5 games in a time class, each feature gets a **robust z-score** against the player-level baseline of their band and time class: z = (x − median) / (IQR / 1.349). We use the median and IQR instead of the mean and sd because move-time features are strongly right-skewed. Cells with fewer than 30 players produce no z-scores.

The **combined anomaly score** averages only the suspicious direction of five features, each capped at 5: very even move timing overall, very even timing under low clock, very even timing across a session, few moves under 2 s, and ACPL below the band. The averaging uses whichever of these features are available, so a player without eval games is scored on timing only. `anomaly_pct` ranks the score within the band × time class. On the sample, 15,463 player × time-class rows qualify and 15,289 receive a score (mean 0.38, 99th percentile 1.46).

## 6.8 Limitations

- The score is a heuristic ranking, not a probability, and certainly not proof of cheating. Strong, consistent players can score high, and "flagged" is the strongest word we use.
- The direction of the timing signals is an assumption (engine relay produces regular timing). The supervised model (section 8) tests whether these signals actually separate flagged accounts.
- ACPL is available on only about 10% of games, and those games are not a random sample: Lichess analyses them because someone requested it.
- Session boundaries use an estimated game end, and first moves have noisy move times (the starting clock is used as the previous clock).
- Bands are assigned from pre-game ratings, which lag for fast-improving or newly created accounts. That is exactly the population where rating manipulation happens.
