-- Validation 02: primary keys, foreign keys and identity consistency (data dictionary §7, hard checks).
-- Every "observed" value should be 0.
--   psql chess -f sql/validation/02_keys.sql
WITH checks (check_name, level, observed, max_allowed) AS (
    SELECT 'games: duplicate game_id', 'hard',
           (SELECT count(*) - count(DISTINCT game_id) FROM core.games)::numeric, 0::numeric
    UNION ALL SELECT 'games: null game_id', 'hard',
           (SELECT count(*) FROM core.games WHERE game_id IS NULL)::numeric, 0
    UNION ALL SELECT 'moves: duplicate (game_id, ply)', 'hard',
           (SELECT count(*) FROM (SELECT game_id, ply FROM core.moves GROUP BY game_id, ply HAVING count(*) > 1) d)::numeric, 0
    UNION ALL SELECT 'identity: duplicate player_id', 'hard',
           (SELECT count(*) - count(DISTINCT player_id) FROM restricted.identity)::numeric, 0
    UNION ALL SELECT 'identity: duplicate username', 'hard',
           (SELECT count(*) - count(DISTINCT username) FROM restricted.identity)::numeric, 0
    UNION ALL SELECT 'moves: game_id missing from games (orphans)', 'hard',
           (SELECT count(*) FROM core.moves m LEFT JOIN core.games g USING (game_id) WHERE g.game_id IS NULL)::numeric, 0
    UNION ALL SELECT 'games: white_id = black_id', 'hard',
           (SELECT count(*) FROM core.games WHERE white_id = black_id)::numeric, 0
    UNION ALL SELECT 'games: player missing from restricted.identity', 'hard',
           (SELECT count(*) FROM core.games g
             WHERE NOT EXISTS (SELECT 1 FROM restricted.identity i WHERE i.player_id = g.white_id)
                OR NOT EXISTS (SELECT 1 FROM restricted.identity i WHERE i.player_id = g.black_id))::numeric, 0
    UNION ALL SELECT 'moves: player_id does not match the game''s player of that colour', 'hard',
           (SELECT count(*) FROM core.moves m JOIN core.games g USING (game_id)
             WHERE m.player_id <> CASE m.color WHEN 'w' THEN g.white_id ELSE g.black_id END)::numeric, 0
    UNION ALL SELECT 'moves: colour does not match ply parity (odd ply = white)', 'hard',
           (SELECT count(*) FROM core.moves WHERE (ply % 2 = 1) <> (color = 'w'))::numeric, 0
)
SELECT check_name, level, observed, max_allowed,
       CASE WHEN level = 'info' THEN 'INFO'
            WHEN observed <= max_allowed THEN 'PASS'
            WHEN level = 'hard' THEN 'FAIL' ELSE 'WARN' END AS status
FROM checks;
