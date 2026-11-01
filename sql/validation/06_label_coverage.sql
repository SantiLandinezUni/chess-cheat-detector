-- Validation 06: every player in the games has a label, and every label belongs to a player (data dictionary §7).
--   psql chess -f sql/validation/06_label_coverage.sql
WITH checks (check_name, level, observed, max_allowed) AS (
    SELECT 'players without a label in core.player_labels', 'soft',
           (SELECT count(*) FROM core.players p
             LEFT JOIN core.player_labels l USING (player_id) WHERE l.player_id IS NULL)::numeric, 0::numeric
    UNION ALL SELECT 'labels for players that are not in core.players', 'soft',
           (SELECT count(*) FROM core.player_labels l
             LEFT JOIN core.players p USING (player_id) WHERE p.player_id IS NULL)::numeric, 0
    UNION ALL SELECT 'players with a label (percent of core.players)', 'info',
           (SELECT round(100.0 * count(l.player_id) / nullif(count(*), 0), 2) FROM core.players p
             LEFT JOIN core.player_labels l USING (player_id)), NULL
)
SELECT check_name, level, observed, max_allowed,
       CASE WHEN level = 'info' THEN 'INFO'
            WHEN observed <= max_allowed THEN 'PASS'
            WHEN level = 'hard' THEN 'FAIL' ELSE 'WARN' END AS status
FROM checks;
