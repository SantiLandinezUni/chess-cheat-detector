-- Validation 05: label distribution in core.player_labels (data dictionary §4.3 and §6).
-- Until Fede's labels are loaded the table is empty and the first row warns.
--   psql chess -f sql/validation/05_label_distribution.sql
WITH checks (check_name, level, observed, max_allowed) AS (
    SELECT 'player_labels: table is empty (labels not loaded yet)', 'soft',
           (SELECT (count(*) = 0)::int FROM core.player_labels)::numeric, 0::numeric
    UNION ALL SELECT 'label = positive (players)', 'info',
           (SELECT count(*) FROM core.player_labels WHERE label = 'positive')::numeric, NULL
    UNION ALL SELECT 'label = negative (players)', 'info',
           (SELECT count(*) FROM core.player_labels WHERE label = 'negative')::numeric, NULL
    UNION ALL SELECT 'label = unknown (players)', 'info',
           (SELECT count(*) FROM core.player_labels WHERE label = 'unknown')::numeric, NULL
    UNION ALL SELECT 'label = positive (percent of labelled players)', 'info',
           (SELECT round(100.0 * avg((label = 'positive')::int), 3) FROM core.player_labels), NULL
    UNION ALL SELECT 'found = false (players the API did not return)', 'info',
           (SELECT count(*) FROM core.player_labels WHERE NOT found)::numeric, NULL
    UNION ALL SELECT 'player_labels: label inconsistent with the rules in data dictionary §6', 'hard',
           (SELECT count(*) FROM core.player_labels
             WHERE label <> CASE
                     WHEN tos_violation IS TRUE THEN 'positive'
                     WHEN found AND disabled IS NOT TRUE AND tos_violation IS NOT TRUE THEN 'negative'
                     ELSE 'unknown' END)::numeric, 0
)
SELECT check_name, level, observed, max_allowed,
       CASE WHEN level = 'info' THEN 'INFO'
            WHEN observed <= max_allowed THEN 'PASS'
            WHEN level = 'hard' THEN 'FAIL' ELSE 'WARN' END AS status
FROM checks;
