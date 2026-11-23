# 16. Dashboard and insights

*Draft, Mohamed, 19 Nov 2026. Numbering follows the master report (Part 1 = chapters 1–6, Part 2 = chapters 7–17). Figures from the first scorer export (60,000 October games); update after the full-data run.*

The dashboard gives a reviewer one place to see how risk scores are distributed, which games score highest and why, and which players are flagged repeatedly. It reads only the PostgreSQL serving layer, so it never touches the full lake or any username.

## 16.1 Architecture

The heavy work stays on the cluster. The streaming scorer writes its final per-game scores to the Parquet lake (`lake/scores/live_game`). A loader (`src/serving/load_serving.py`) reads those small result files, replaces every game ID with a keyed hash, and loads them into the `serving` schema in one transaction per table, so a rerun replaces the data instead of duplicating it. The dashboard is a Streamlit application (`dashboard/app.py`) that queries only that schema. Results from the other workstreams (baselines, anomaly scores, labels summary and model results) are loaded into the same schema as they become available.

*Table 18. The serving schema*

| Object | Type | One row per | Content |
|---|---|---|---|
| `serving.live_scores` | table | player per game per scorer run | features, z-scores, confidence and risk score; hashed `player_id` and `game_key` only |
| `serving.game_scores` | view | game | highest score of the two players; number of players with a high score |
| `serving.player_scores_live` | view | player | games played, mean and maximum score, number of high-risk games |
| `serving.rating_bands` | table | rating band | band labels for display (12 bands) |
| `serving.baselines`, `player_anomaly`, `labels_summary`, `model_results` | tables | as in the data dictionary | loaded from the other workstreams' outputs |

## 16.2 Privacy by design

A Lichess game ID would let anyone look up both players, so it never enters the database. The loader replaces it with a `game_key`: an HMAC-SHA256 hash of the game ID with the team's secret key, truncated to 12 characters. Without the key, the hash cannot be turned back into a game ID. The loader refuses any input that carries a username or game ID column, and a separate privacy check scans the whole schema before the database is dumped. On the test run, the check passed, and the dump restored into an empty database with identical row counts (119,886 score rows and 12 rating bands).

## 16.3 What the dashboard shows

The dashboard has five views, each filtered by time class, rating band and minimum confidence:

1. **Overview:** headline counts, the distribution of risk scores and the share of high-risk player-games by time class (Figure 8).
2. **Behaviour by band:** median timing features for each rating band and time class, which show why every score must be relative to the player's own band (Figure 9).
3. **High-risk games:** the top games by score, each with its anonymous key, confidence and the three z-scores, and a breakdown of how each feature contributed (Figure 10).
4. **Players:** players flagged in more than one game.
5. **Anomaly and model:** the anomaly scores, labels summary and model results from the other workstreams.

A game counts as high risk when a player's score is 1 or more. The score runs from 0 to 5 and is the mean of the available feature terms multiplied by the confidence (section 13), so a score of 1 means a large, consistent deviation over a reasonable number of moves.

*[Figure 8: Dashboard overview: distribution of risk scores and share of high-risk player-games by time class (60,000 October games). Screenshot in the report.]*


*[Figure 9: Median share of moves under two seconds by rating band and time class. Screenshot in the report.]*


*[Figure 10: Top games by risk score, shown by anonymous game key and hashed player ID. Screenshot in the report.]*


## 16.4 Insights from the first scorer run

*[Editor note: these figures come from the first scorer export (60,000 October games, scoring rule timing-z-v1 on the provisional baselines). Update them when the full-data run and the final baselines are loaded. Source: docs/validation_results/dashboard_*.csv.]*

The first run covers 60,000 October games: 119,886 player-games by 41,695 players. Five findings stand out.

**Most player-games can be scored.** 96.4% received a score (115,620). The remaining 4,266 had no feature with enough timed moves, typically because the game was very short, and are shown as "not scored" rather than as zero.

**High scores are rare.** The median score is 0.146 and the 99th percentile is 0.927. Only 733 player-games (0.63% of those scored) reach 1 or more, in 693 games. This is the right shape for a review queue: a reviewer would start from a few hundred games out of 60,000.

**Fast time controls are flagged more often.** The share of high-risk player-games is 1.60% in ultrabullet and 1.33% in bullet, but 0.28% in blitz, 0.09% in rapid and none of the 603 classical player-games. Scores are measured against each time control's own baseline, so this gap should not exist if the rule were neutral. It suggests that the timing features separate players more sharply when there is little time per move.

**Higher-rated players are flagged more often.** No player-game in the lowest band (under 800) scores 1 or more, against 1.0–2.5% in every band from 2000 upwards. Strong players move faster and more evenly (Figure 9), and the baselines adjust for that on average, but the tail of very consistent strong players still scores high. Combined with the previous finding, this is a fairness warning: without further calibration, the rule would send strong bullet players to review more often than others.

**One feature drives most high scores.** Among high-risk player-games, the average term for "few instant moves" is 2.54, against 0.39 across all scored player-games. The terms for even move timing (1.22) and even timing under low clock (1.12) also rise, but less. High-risk players therefore play fewer instant moves than their peers, which fits engine relay, where even obvious moves take time to copy.

Finally, repeated flags are more informative than single ones. 556 players have at least one high-risk game, but only 114 have two or more. The Players view lists these first, because a single high score can be chance while repeated high scores are a stronger reason to review an account.

## 16.5 Limitations

The figures above come from a first export of 60,000 games scored against provisional baselines, so they show what the dashboard can do rather than final results. The export holds only each player's final score per game; showing a score that updates move by move, as planned for the presentation demo, needs the scorer to also export intermediate scores. The dashboard runs locally from the serving database rather than as a hosted service. And because the rule uses timing only, the dashboard cannot yet show engine-based evidence, which is what a human reviewer would check next.
