-- One row per player in restricted.identity that has a fetched profile, with the label rules of
-- data dictionary §6. Columns match core.player_labels (§4.3); no usernames leave this query.
-- Players never fetched (e.g. a failed API batch) have no row; build_labels.py logs how many.
-- Timestamps are UTC, as 'YYYY-MM-DD HH24:MI:SS'.
SELECT
    i.player_id,
    CASE
        WHEN (p.response ->> 'tosViolation')::boolean IS TRUE THEN 'positive'
        WHEN p.found AND (p.response ->> 'disabled')::boolean IS NOT TRUE THEN 'negative'
        ELSE 'unknown'  -- disabled without tosViolation, or not found
    END AS label,
    -- An absent flag on a returned account means false; null only when the account wasn't returned
    CASE WHEN p.found THEN coalesce((p.response ->> 'tosViolation')::boolean, false) END::text AS tos_violation,
    CASE WHEN p.found THEN coalesce((p.response ->> 'disabled')::boolean, false) END::text AS disabled,
    p.found::text AS found,
    to_char(to_timestamp((p.response ->> 'createdAt')::bigint / 1000.0) AT TIME ZONE 'UTC',
            'YYYY-MM-DD HH24:MI:SS') AS account_created_at,
    to_char(p.fetched_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') AS label_fetched_at
FROM restricted.identity i
JOIN restricted.user_profiles_raw p ON p.username = lower(i.username)
ORDER BY i.player_id
