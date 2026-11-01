-- Example 3 (report section 5.3): per-player behaviour against the population baseline, the seed of the
-- unlabelled anomaly layer. For blitz players with at least 500 clocked moves, compare each player's share of
-- moves under 2 s with the baseline of their rating band, and summarise the gap over all such players.
-- Joins core.moves, core.games, core.players, ref.rating_bands and core.baselines. Aggregate only: no player is listed.
--   psql chess -f sql/examples/03_fast_players_vs_band.sql
WITH player_blitz AS (
    SELECT m.player_id,
           count(*)                           AS n_moves,
           avg((m.move_time_s < 2)::int)      AS share_under_2s
    FROM core.moves m
    JOIN core.games g USING (game_id)
    WHERE g.time_class = 'blitz' AND NOT g.has_bot AND g.has_clock AND m.move_time_s >= 0
    GROUP BY m.player_id
    HAVING count(*) >= 500
),
gap AS (
    SELECT pb.player_id,
           pb.share_under_2s - bl.share_moves_under_2s AS diff_vs_band
    FROM player_blitz pb
    JOIN core.players p USING (player_id)
    JOIN ref.rating_bands rb
      ON p.mean_elo >= rb.min_elo AND (rb.max_elo IS NULL OR p.mean_elo <= rb.max_elo)
    JOIN core.baselines bl
      ON bl.band_id = rb.band_id AND bl.time_class = 'blitz'
)
SELECT count(*)                                                              AS n_players,
       round((percentile_cont(0.50) WITHIN GROUP (ORDER BY diff_vs_band))::numeric, 3) AS median_gap,
       round((percentile_cont(0.90) WITHIN GROUP (ORDER BY diff_vs_band))::numeric, 3) AS p90_gap,
       round((percentile_cont(0.99) WITHIN GROUP (ORDER BY diff_vs_band))::numeric, 3) AS p99_gap,
       count(*) FILTER (WHERE diff_vs_band > 0.20)                           AS n_players_gap_above_0_20
FROM gap;
