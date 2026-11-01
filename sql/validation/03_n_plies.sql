-- Validation 03: games.n_plies against the number of rows in core.moves (data dictionary §7).
--   psql chess -f sql/validation/03_n_plies.sql
WITH per_game AS (
    SELECT g.game_id, g.n_plies, count(m.ply) AS n_moves, max(m.ply) AS max_ply
    FROM core.games g
    LEFT JOIN core.moves m USING (game_id)
    GROUP BY g.game_id, g.n_plies
),
checks (check_name, level, observed, max_allowed) AS (
    SELECT 'games: n_plies differs from the number of move rows', 'hard',
           (SELECT count(*) FROM per_game WHERE n_plies <> n_moves)::numeric, 0::numeric
    UNION ALL SELECT 'games: ply numbers not contiguous (max ply <> count of moves)', 'hard',
           (SELECT count(*) FROM per_game WHERE n_moves > 0 AND max_ply <> n_moves)::numeric, 0
    UNION ALL SELECT 'games: n_plies = 0 (no moves)', 'soft',
           (SELECT count(*) FROM per_game WHERE n_plies = 0)::numeric, 0
    UNION ALL SELECT 'games checked', 'info', (SELECT count(*) FROM per_game)::numeric, NULL
    UNION ALL SELECT 'total plies in games.n_plies', 'info', (SELECT sum(n_plies) FROM per_game)::numeric, NULL
    UNION ALL SELECT 'total rows in core.moves', 'info', (SELECT sum(n_moves) FROM per_game)::numeric, NULL
)
SELECT check_name, level, observed, max_allowed,
       CASE WHEN level = 'info' THEN 'INFO'
            WHEN observed <= max_allowed THEN 'PASS'
            WHEN level = 'hard' THEN 'FAIL' ELSE 'WARN' END AS status
FROM checks;
