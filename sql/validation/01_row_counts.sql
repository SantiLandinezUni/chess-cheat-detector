-- Validation 01: row counts of every table, plus consistency between tables.
-- Output: check_name, level (hard | soft | info), observed, max_allowed, status (PASS | FAIL | WARN | INFO).
-- Parquet vs PostgreSQL counts for games, moves and identity are compared in notebooks/02_validation.ipynb
-- (SQL cannot read Parquet); the PostgreSQL side of that comparison is the first three rows here.
--   psql chess -f sql/validation/01_row_counts.sql
WITH checks (check_name, level, observed, max_allowed) AS (
    SELECT 'core.games rows (compare with Parquet games)', 'info', (SELECT count(*) FROM core.games)::numeric, NULL::numeric
    UNION ALL SELECT 'core.moves rows (compare with Parquet moves)', 'info', (SELECT count(*) FROM core.moves)::numeric, NULL
    UNION ALL SELECT 'restricted.identity rows (compare with Parquet identity)', 'info', (SELECT count(*) FROM restricted.identity)::numeric, NULL
    UNION ALL SELECT 'core.players rows', 'info', (SELECT count(*) FROM core.players)::numeric, NULL
    UNION ALL SELECT 'core.baselines rows', 'info', (SELECT count(*) FROM core.baselines)::numeric, NULL
    UNION ALL SELECT 'core.player_labels rows', 'info', (SELECT count(*) FROM core.player_labels)::numeric, NULL
    UNION ALL SELECT 'ref.rating_bands rows', 'info', (SELECT count(*) FROM ref.rating_bands)::numeric, NULL
    UNION ALL SELECT 'ref.time_classes rows', 'info', (SELECT count(*) FROM ref.time_classes)::numeric, NULL
    UNION ALL SELECT 'core.players vs restricted.identity: row difference', 'hard',
           abs((SELECT count(*) FROM core.players) - (SELECT count(*) FROM restricted.identity))::numeric, 0
    UNION ALL SELECT 'core.players vs distinct players in games: row difference', 'hard',
           abs((SELECT count(*) FROM core.players)
             - (SELECT count(*) FROM (SELECT white_id FROM core.games UNION SELECT black_id FROM core.games) d))::numeric, 0
    UNION ALL SELECT 'ref.rating_bands: difference from the 12 expected rows', 'hard',
           abs((SELECT count(*) FROM ref.rating_bands) - 12)::numeric, 0
    UNION ALL SELECT 'ref.time_classes: difference from the 6 expected rows', 'hard',
           abs((SELECT count(*) FROM ref.time_classes) - 6)::numeric, 0
    UNION ALL SELECT 'core.baselines: rows above 12 bands x 6 time classes', 'hard',
           greatest((SELECT count(*) FROM core.baselines) - 72, 0)::numeric, 0
)
SELECT check_name, level, observed, max_allowed,
       CASE WHEN level = 'info' THEN 'INFO'
            WHEN observed <= max_allowed THEN 'PASS'
            WHEN level = 'hard' THEN 'FAIL' ELSE 'WARN' END AS status
FROM checks;
