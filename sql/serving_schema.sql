-- Serving layer (Part 2, workstream E2).
-- Small result tables read by the dashboard. Heavy processing stays in Spark + Parquet on the cluster.
-- Privacy: only hashed player_id. No usernames. No Lichess game_id: games are identified by game_key,
-- a keyed hash of game_id (a game_id is public on Lichess and reveals both players' usernames).
-- Safe to run repeatedly.

CREATE SCHEMA IF NOT EXISTS serving;

-- Final per-game, per-player scores of the streaming scorer (lake/scores/live_game, data dictionary L13).
CREATE TABLE IF NOT EXISTS serving.live_scores (
    run_id                    VARCHAR(64)  NOT NULL,
    game_key                  CHAR(12)     NOT NULL,
    player_id                 CHAR(16)     NOT NULL,
    color                     CHAR(1)      NOT NULL CHECK (color IN ('w', 'b')),
    time_class                VARCHAR(16)  NOT NULL,
    elo                       SMALLINT,
    band_id                   SMALLINT,
    n_plies                   SMALLINT     NOT NULL,
    n_moves                   INTEGER      NOT NULL CHECK (n_moves >= 0),
    n_timed_moves             INTEGER      NOT NULL CHECK (n_timed_moves >= 0),
    move_time_mean_s          REAL,
    move_time_cv              REAL,
    share_moves_under_2s      REAL,
    share_low_clock           REAL,
    n_low_clock_moves         INTEGER      NOT NULL,
    move_time_cv_low_clock    REAL,
    z_move_time_cv            REAL,
    z_share_moves_under_2s    REAL,
    z_move_time_cv_low_clock  REAL,
    n_score_features          SMALLINT     NOT NULL CHECK (n_score_features BETWEEN 0 AND 3),
    confidence                REAL         NOT NULL CHECK (confidence >= 0 AND confidence < 1),
    risk_score                REAL         CHECK (risk_score >= 0 AND risk_score <= 5),
    end_reason                VARCHAR(16)  NOT NULL,
    rule_version              VARCHAR(32)  NOT NULL,
    baselines                 VARCHAR(64)  NOT NULL,
    scored_at                 TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (run_id, game_key, player_id),
    CHECK ((risk_score IS NULL) = (n_score_features = 0))
);

CREATE INDEX IF NOT EXISTS live_scores_risk_idx ON serving.live_scores (run_id, risk_score DESC);
CREATE INDEX IF NOT EXISTS live_scores_class_band_idx ON serving.live_scores (time_class, band_id);

-- One row per game: the higher of the two players' scores.
CREATE OR REPLACE VIEW serving.game_scores AS
SELECT run_id,
       game_key,
       MIN(time_class)                         AS time_class,
       MAX(risk_score)                         AS max_risk_score,
       COUNT(*)                                AS n_players_scored,
       SUM(CASE WHEN risk_score >= 1 THEN 1 ELSE 0 END) AS n_players_high
FROM serving.live_scores
GROUP BY run_id, game_key;

-- One row per player across the games in a run.
CREATE OR REPLACE VIEW serving.player_scores_live AS
SELECT run_id,
       player_id,
       COUNT(*)                                 AS n_games,
       COUNT(risk_score)                        AS n_games_scored,
       AVG(risk_score)                          AS mean_risk_score,
       MAX(risk_score)                          AS max_risk_score,
       SUM(CASE WHEN risk_score >= 1 THEN 1 ELSE 0 END) AS n_games_high
FROM serving.live_scores
GROUP BY run_id, player_id;

-- Rating bands, copied from ref.rating_bands so the dashboard can show labels.
CREATE TABLE IF NOT EXISTS serving.rating_bands (
    band_id     SMALLINT PRIMARY KEY,
    min_elo     SMALLINT NOT NULL,
    max_elo     SMALLINT,
    band_label  VARCHAR(12) NOT NULL
);

INSERT INTO serving.rating_bands (band_id, min_elo, max_elo, band_label)
SELECT 1, 0, 799, '<800'
UNION ALL
SELECT g + 1, 800 + 200 * (g - 1), 999 + 200 * (g - 1),
       (800 + 200 * (g - 1))::text || '-' || (999 + 200 * (g - 1))::text
FROM generate_series(1, 10) AS g
UNION ALL
SELECT 12, 2800, NULL, '2800+'
ON CONFLICT (band_id) DO NOTHING;

-- The other serving tables (baselines, player anomaly scores, labels summary, model results) are
-- created by src/serving/load_serving.py from the Parquet/CSV files their workstreams produce,
-- with the same column names as the data dictionary. The loader refuses any file that carries a
-- username or game_id column.
