-- Example 1 (report section 5.3): how long do players think per move, by rating band and time class?
-- Uses the SQL-generated tables core.baselines, ref.rating_bands and ref.time_classes.
-- Baselines cover human-vs-human games with clocks and exclude moves with move_time_s < 0.
--   psql chess -f sql/examples/01_median_move_time_by_band.sql
SELECT tc.time_class,
       rb.band_label,
       bl.n_players,
       bl.n_moves,
       round(bl.p10_move_time_s::numeric, 2)    AS p10_s,
       round(bl.median_move_time_s::numeric, 2) AS median_s,
       round(bl.p90_move_time_s::numeric, 2)    AS p90_s,
       round(100 * bl.share_moves_under_2s::numeric, 1) AS pct_moves_under_2s
FROM core.baselines bl
JOIN ref.rating_bands rb USING (band_id)
JOIN ref.time_classes tc USING (time_class)
ORDER BY tc.sort_order, rb.band_id;
