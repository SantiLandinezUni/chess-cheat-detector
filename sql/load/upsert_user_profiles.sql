-- Upsert one batch of Lichess API results; collect_labels.py fills the VALUES placeholder with
-- execute_values (the file must contain exactly one placeholder, comments included).
-- A refetch (--refresh) replaces the stored response and fetch time.
INSERT INTO restricted.user_profiles_raw (username, found, response, fetched_at, batch_id)
VALUES %s
ON CONFLICT (username) DO UPDATE
SET found      = EXCLUDED.found,
    response   = EXCLUDED.response,
    fetched_at = EXCLUDED.fetched_at,
    batch_id   = EXCLUDED.batch_id;
