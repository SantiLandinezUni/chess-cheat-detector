-- Validation 04: value ranges, derived-field consistency and null rates (data dictionary §6 and §7).
-- Percent rows are reported as info. Soft checks warn instead of failing.
--   psql chess -f sql/validation/04_ranges_nulls.sql
WITH checks (check_name, level, observed, max_allowed) AS (
    SELECT 'games: Elo outside 400-3500', 'soft',
           (SELECT count(*) FROM core.games
             WHERE white_elo NOT BETWEEN 400 AND 3500 OR black_elo NOT BETWEEN 400 AND 3500)::numeric, 0::numeric
    UNION ALL SELECT 'moves: move_time_s below -1 s', 'soft',
           (SELECT count(*) FROM core.moves WHERE move_time_s < -1)::numeric, 0
    UNION ALL SELECT 'games: percent without clocks (has_clock = false), warn above 5', 'soft',
           (SELECT round(100.0 * avg((NOT has_clock)::int), 2) FROM core.games), 5
    UNION ALL SELECT 'games: started_at outside its source_month', 'hard',
           (SELECT count(*) FROM core.games WHERE to_char(started_at, 'YYYY-MM') <> source_month)::numeric, 0
    UNION ALL SELECT 'games: has_bot inconsistent with the titles', 'hard',
           (SELECT count(*) FROM core.games
             WHERE has_bot <> (coalesce(white_title, '') = 'BOT' OR coalesce(black_title, '') = 'BOT'))::numeric, 0
    UNION ALL SELECT 'games: time_class inconsistent with time_control', 'hard',
           (SELECT count(*) FROM core.games
             WHERE time_class <> CASE
                     WHEN time_control = '-' THEN 'correspondence'
                     WHEN base_seconds + 40 * increment_seconds < 30   THEN 'ultrabullet'
                     WHEN base_seconds + 40 * increment_seconds < 180  THEN 'bullet'
                     WHEN base_seconds + 40 * increment_seconds < 480  THEN 'blitz'
                     WHEN base_seconds + 40 * increment_seconds < 1500 THEN 'rapid'
                     ELSE 'classical' END)::numeric, 0
    UNION ALL SELECT 'moves: has_clock game but clock_after_s is null', 'hard',
           (SELECT count(*) FROM core.moves m JOIN core.games g USING (game_id)
             WHERE g.has_clock AND m.clock_after_s IS NULL)::numeric, 0
    UNION ALL SELECT 'moves: eval_cp and eval_mate both set', 'hard',
           (SELECT count(*) FROM core.moves WHERE eval_cp IS NOT NULL AND eval_mate IS NOT NULL)::numeric, 0
    UNION ALL SELECT 'moves: negative move_time_s (any), count', 'info',
           (SELECT count(*) FROM core.moves WHERE move_time_s < 0)::numeric, NULL
    UNION ALL SELECT 'games: percent with has_eval', 'info',
           (SELECT round(100.0 * avg(has_eval::int), 2) FROM core.games), NULL
    UNION ALL SELECT 'games: percent with a BOT player', 'info',
           (SELECT round(100.0 * avg(has_bot::int), 2) FROM core.games), NULL
    UNION ALL SELECT 'games: null rate % of eco', 'info',
           (SELECT round(100.0 * avg((eco IS NULL)::int), 2) FROM core.games), NULL
    UNION ALL SELECT 'games: null rate % of white_rating_diff', 'info',
           (SELECT round(100.0 * avg((white_rating_diff IS NULL)::int), 2) FROM core.games), NULL
    UNION ALL SELECT 'moves: null rate % of clock_after_s', 'info',
           (SELECT round(100.0 * avg((clock_after_s IS NULL)::int), 2) FROM core.moves), NULL
    UNION ALL SELECT 'moves: null rate % of move_time_s', 'info',
           (SELECT round(100.0 * avg((move_time_s IS NULL)::int), 2) FROM core.moves), NULL
    UNION ALL SELECT 'moves: null rate % of eval (cp and mate both null)', 'info',
           (SELECT round(100.0 * avg((eval_cp IS NULL AND eval_mate IS NULL)::int), 2) FROM core.moves), NULL
)
SELECT check_name, level, observed, max_allowed,
       CASE WHEN level = 'info' THEN 'INFO'
            WHEN observed <= max_allowed THEN 'PASS'
            WHEN level = 'hard' THEN 'FAIL' ELSE 'WARN' END AS status
FROM checks;
