-- SQL-generated derived tables (data dictionary §4.4 and §4.5): core.players and core.baselines.
-- Run after schema.sql, generate_reference.sql and the load of core.games / core.moves.
-- Idempotent: each table is emptied and refilled inside one transaction, so a rerun gives the same rows.
--   psql chess -f sql/generate_derived.sql

BEGIN;

-- ---------- core.players: one row per player, from both colours ----------
TRUNCATE core.players;

INSERT INTO core.players
    (player_id, n_games, n_games_white, first_game_at, last_game_at, mean_elo, max_elo, title, is_bot)
WITH sides AS (
    SELECT white_id AS player_id, TRUE  AS is_white, started_at, white_elo AS elo, white_title AS title
    FROM core.games
    UNION ALL
    SELECT black_id AS player_id, FALSE AS is_white, started_at, black_elo AS elo, black_title AS title
    FROM core.games
)
SELECT player_id,
       count(*)::INTEGER                                   AS n_games,
       (count(*) FILTER (WHERE is_white))::INTEGER         AS n_games_white,
       min(started_at)                                     AS first_game_at,
       max(started_at)                                     AS last_game_at,
       avg(elo)::REAL                                      AS mean_elo,
       max(elo)::SMALLINT                                  AS max_elo,
       -- most recent title seen, if any
       (array_agg(title ORDER BY started_at DESC) FILTER (WHERE title IS NOT NULL))[1] AS title,
       coalesce(bool_or(title = 'BOT'), FALSE)             AS is_bot
FROM sides
GROUP BY player_id;

-- ---------- core.baselines: move-time norms per rating band x time class ----------
-- Population: human-vs-human games (has_bot = false) that carry clocks (has_clock = true).
-- Moves with move_time_s < 0 are excluded: lag compensation, or the opponent adding +15 s
-- (data dictionary §6). The band is the band of the mover's own pre-game Elo.
TRUNCATE core.baselines;

INSERT INTO core.baselines
    (band_id, time_class, n_players, n_games, n_moves,
     mean_move_time_s, sd_move_time_s, p10_move_time_s, median_move_time_s, p90_move_time_s,
     share_moves_under_2s, share_games_with_eval, computed_at)
WITH mv AS (
    SELECT g.game_id, g.time_class, g.has_eval, m.player_id, m.move_time_s,
           CASE m.color WHEN 'w' THEN g.white_elo ELSE g.black_elo END AS elo
    FROM core.moves m
    JOIN core.games g USING (game_id)
    WHERE NOT g.has_bot
      AND g.has_clock
      AND m.move_time_s >= 0
),
banded AS (
    SELECT r.band_id, mv.game_id, mv.time_class, mv.has_eval, mv.player_id, mv.move_time_s
    FROM mv
    JOIN ref.rating_bands r
      ON mv.elo >= r.min_elo AND (r.max_elo IS NULL OR mv.elo <= r.max_elo)
),
move_cell AS (
    SELECT band_id, time_class,
           count(DISTINCT player_id)                                   AS n_players,
           count(DISTINCT game_id)                                     AS n_games,
           count(*)                                                    AS n_moves,
           avg(move_time_s)                                            AS mean_move_time_s,
           stddev_samp(move_time_s)                                    AS sd_move_time_s,
           percentile_cont(0.10) WITHIN GROUP (ORDER BY move_time_s)   AS p10_move_time_s,
           percentile_cont(0.50) WITHIN GROUP (ORDER BY move_time_s)   AS median_move_time_s,
           percentile_cont(0.90) WITHIN GROUP (ORDER BY move_time_s)   AS p90_move_time_s,
           avg((move_time_s < 2)::INTEGER)                             AS share_moves_under_2s
    FROM banded
    GROUP BY band_id, time_class
),
game_cell AS (
    -- one row per (game, band, time class) so a game is counted once per cell
    SELECT band_id, time_class, avg(has_eval::INTEGER) AS share_games_with_eval
    FROM (SELECT DISTINCT band_id, time_class, game_id, has_eval FROM banded) d
    GROUP BY band_id, time_class
)
SELECT mc.band_id, mc.time_class,
       mc.n_players::INTEGER, mc.n_games::INTEGER, mc.n_moves::INTEGER,
       mc.mean_move_time_s::REAL, mc.sd_move_time_s::REAL,
       mc.p10_move_time_s::REAL, mc.median_move_time_s::REAL, mc.p90_move_time_s::REAL,
       mc.share_moves_under_2s::REAL, gc.share_games_with_eval::REAL,
       (now() AT TIME ZONE 'UTC')
FROM move_cell mc
JOIN game_cell gc USING (band_id, time_class);

COMMIT;
