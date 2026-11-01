-- SQL-generated reference tables (data dictionary §5). Run after schema.sql and before loading
-- core.games (its time_class column references ref.time_classes). Idempotent: upserts by primary key.
--   psql chess -f sql/generate_reference.sql

-- Time classes: estimated duration E = base + 40 x increment (data dictionary §6)
INSERT INTO ref.time_classes (time_class, min_est_seconds, max_est_seconds, sort_order)
VALUES ('ultrabullet',    0,    30,   1),
       ('bullet',         30,   180,  2),
       ('blitz',          180,  480,  3),
       ('rapid',          480,  1500, 4),
       ('classical',      1500, NULL, 5),
       ('correspondence', NULL, NULL, 6)
ON CONFLICT (time_class) DO UPDATE
SET min_est_seconds = EXCLUDED.min_est_seconds,
    max_est_seconds = EXCLUDED.max_est_seconds,
    sort_order      = EXCLUDED.sort_order;

-- Rating bands: below 800, then 200-point bands from 800 to 2799, then 2800+
INSERT INTO ref.rating_bands (band_id, min_elo, max_elo, band_label)
SELECT 1, 0, 799, '<800'
UNION ALL
SELECT (2 + (lo - 800) / 200)::SMALLINT, lo, lo + 199, lo || '-' || (lo + 199)
FROM generate_series(800, 2600, 200) AS lo
UNION ALL
SELECT 12, 2800, NULL, '2800+'
ON CONFLICT (band_id) DO UPDATE
SET min_elo    = EXCLUDED.min_elo,
    max_elo    = EXCLUDED.max_elo,
    band_label = EXCLUDED.band_label;
