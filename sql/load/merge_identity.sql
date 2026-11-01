-- Upsert players; keep the earliest month a player was seen in.
INSERT INTO restricted.identity (player_id, username, first_seen_month)
SELECT player_id, username, first_seen_month FROM restricted.stg_identity
ON CONFLICT (player_id) DO UPDATE
SET username         = EXCLUDED.username,
    first_seen_month = LEAST(restricted.identity.first_seen_month, EXCLUDED.first_seen_month);
