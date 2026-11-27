# 16. Dashboard and insights

*Mohamed. Numbering follows the master report (Part 1 = chapters 1–6, Part 2 = chapters 7–17). Figures from the full replayed day of 22 October 2026 (run `38028-day2026-10-22-nopacing-c4`, rule `timing-z-v1`, final baselines). Numbers: `docs/validation_results/dashboard_*.csv`, written by `src/serving/dashboard_numbers.py` from the serving database.*

The dashboard gives a reviewer one place to see how risk scores are distributed, which games score highest and why, how a score develops during a game, and which players are flagged repeatedly. It reads only the PostgreSQL serving layer, so it never touches the full lake or any username.

## 16.1 Architecture

The heavy work stays on the cluster. The streaming scorer writes its final per-game scores to the Parquet lake (`lake/scores/live_game`), and the export for the dashboard replaces every game ID with a keyed hash before the files leave the cluster. A loader (`src/serving/load_serving.py`) reads these files in batches and loads them into the `serving` schema. The dashboard is a Streamlit application (`dashboard/app.py`) that queries only that schema. Every chart is aggregated inside PostgreSQL (counts, histogram buckets, medians, top-N lists), so only small result sets reach the browser and the full day runs on a laptop: the first page loads in about 18 seconds and is cached afterwards.

The two scorer tables are partitioned by scorer run. A rerun loads the new data into a staging table and swaps it in for the old partition in one transaction, instead of deleting rows. The dashboard sees either the old run or the new one, never a half-loaded state, and no clean-up of deleted rows is needed afterwards (FEEDBACK_ACTIONS 3.4).

*Table 19. The serving schema*

| Object | Type | One row per | Content |
|---|---|---|---|
| `serving.live_scores` | table, partitioned by run | player per game per scorer run | features, z-scores, confidence and risk score; hashed `player_id` and `game_key` only |
| `serving.running_scores` | table, partitioned by run | ply per player per game (1,000 sampled games) | running risk score after every move, for the suspicion timeline |
| `serving.game_scores` | view | game | highest score of the two players; number of players with a high score |
| `serving.player_scores_live` | view | player | games played, mean and maximum score, number of high-risk games |
| `serving.settings`, `serving.rating_bands` | tables | — | the high-risk threshold used by the views and the dashboard; band labels (12 bands) |
| `serving.baselines`, `player_anomaly`, `labels_summary`, `model_results` | tables | as in the data dictionary | loaded from the other workstreams' outputs |

## 16.2 Privacy by design

A Lichess game ID would let anyone look up both players, so it never enters the database. Games are identified by a `game_key`: an HMAC-SHA256 hash of the game ID with the team's secret key, truncated to 16 characters, the same method as the player IDs. Without the key, the hash cannot be turned back into a game ID. The database itself rejects any key that is not a 16-character hash, the loader refuses any input that carries a username or game ID column, and a separate privacy check scans the whole schema before the database is dumped. On the full day the check passed (9 tables and views, 100 columns), and the dump restored into an empty database with identical row counts: 6,107,430 score rows, 67,275 timeline rows and 12 rating bands.

## 16.3 What the dashboard shows

The dashboard has six views, each filtered by time class, rating band and minimum confidence:

1. **Overview:** headline counts, the distribution of risk scores and the share of high-risk player-games by time class (Figure 8).
2. **Behaviour by band:** median timing features for each rating band and time class, which show why every score must be relative to the player's own band (Figure 9).
3. **High-risk games:** the top games by score, each with its anonymous key, confidence and the three z-scores, and a breakdown of how each feature contributed (Figure 10).
4. **Suspicion timeline:** the running risk score of both players after every move of one game, with the high-risk line and the move at which it was first crossed (Figure 11).
5. **Players:** players flagged in more than one game.
6. **Anomaly and model:** the anomaly scores, labels summary and model results from the other workstreams.

A game counts as high risk when a player's score is 1 or more. The score runs from 0 to 5 and is the mean of the available feature terms multiplied by the confidence (chapter 13), so a score of 1 means a large, consistent deviation over a reasonable number of moves.

*[Figure 8: Dashboard overview: distribution of risk scores and share of high-risk player-games by time class (full day, 3,055,849 games).]*


*[Figure 9: Median share of moves under two seconds by rating band and time class.]*


*[Figure 10: Top games by risk score, shown by anonymous game key and hashed player ID.]*


*[Figure 11: Suspicion timeline: running risk score of both players, move by move, for one sampled game.]*


## 16.4 Insights from one full day

The replayed day covers every rated game played on Lichess on 22 October 2026: 3,055,849 games, 6,107,430 player-games by 701,069 players. Six findings stand out.

**Almost every player-game can be scored.** 96.1% received a score (5,870,305). The remaining 237,125 had no feature with enough timed moves, typically because the game was very short, and are shown as "not scored" rather than as zero.

**High scores are rare.** The median score is 0.146 and the 99th percentile is 0.934. Only 39,607 player-games (0.67% of those scored) reach 1 or more, in 36,737 games. This is the right shape for a review queue: about 1.2% of the day's games, ranked, instead of three million.

**Fast time controls are flagged more often.** The share of high-risk player-games is 1.48% in bullet and 1.31% in ultrabullet, but 0.28% in blitz, 0.09% in rapid and 0.01% in classical. Scores are measured against each time control's own baseline, so this gap should not exist if the rule were neutral. It suggests that the timing features separate players more sharply when there is little time per move.

**Higher-rated players are flagged more often.** In the lowest band (under 800) 0.02% of player-games score 1 or more, against 1.1–2.2% in every band from 2000 upwards, with the peak at 2400–2599. Strong players move faster and more evenly (Figure 9), and the baselines adjust for that on average, but the tail of very consistent strong players still scores high. Combined with the previous finding, this is a fairness warning: without further calibration, the rule would send strong bullet players to review more often than others.

**One feature drives most high scores.** Among high-risk player-games, the average term for "few instant moves" is 2.60, against 0.39 across all scored player-games. The terms for even move timing (1.20) and even timing under low clock (1.08) also rise, but less. High-risk players therefore play fewer instant moves than their peers, which fits engine relay, where even obvious moves take time to copy.

**A live alert should wait.** In the 1,000 games logged move by move, 37 of the 1,998 player timelines crossed the high-risk line at some point, but only 20 ended above it; the other 17 crossed and fell back. For the 20 that ended high, the line was first crossed at the median after 70% of the game. A live alert on the first crossing would therefore raise almost as many false alarms as real ones. Requiring the score to stay above the line for several moves, or alerting only near the end of the game, would make the live mode more reliable.

Finally, repeated flags are more informative than single ones. 18,954 players have at least one high-risk game during the day, but 6,618 have two or more. The Players view lists these first, because a single high score can be chance while repeated high scores are a stronger reason to review an account.

## 16.5 Limitations

The figures cover one day, scored with a timing-only rule; they show what the dashboard can do at full scale, not a validated detection rate. The move-by-move timeline exists for a random sample of 1,000 games, not for every game. The dashboard runs locally from the serving database rather than as a hosted service. And because the rule uses timing only, the dashboard cannot yet show engine-based evidence, which is what a human reviewer would check next.
