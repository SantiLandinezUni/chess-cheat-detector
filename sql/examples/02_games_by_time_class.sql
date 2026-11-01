-- Example 2 (report section 5.3): profile of the games per time class.
-- Joins core.games with ref.time_classes; shares of time-forfeit endings, tournament games and games with a BOT.
--   psql chess -f sql/examples/02_games_by_time_class.sql
SELECT tc.time_class,
       count(*)                                              AS n_games,
       round(avg(g.n_plies)::numeric, 1)                     AS avg_plies,
       round(100 * avg((g.termination = 'Time forfeit')::int)::numeric, 2) AS pct_time_forfeit,
       round(100 * avg(g.is_tournament::int)::numeric, 2)    AS pct_tournament,
       round(100 * avg(g.has_bot::int)::numeric, 2)          AS pct_with_bot,
       round(100 * avg(g.has_eval::int)::numeric, 2)         AS pct_with_eval
FROM core.games g
JOIN ref.time_classes tc USING (time_class)
GROUP BY tc.time_class, tc.sort_order
ORDER BY tc.sort_order;
