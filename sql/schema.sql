-- Chess Cheat Detector: schemas and tables from docs/data_dictionary.md §4-5.
-- Safe to run more than once (IF NOT EXISTS). It does not alter tables that already exist:
-- after changing a definition here, drop the table (or the database) and rerun.
--   psql chess -f sql/schema.sql

CREATE SCHEMA IF NOT EXISTS core;        -- analytical, pseudonymized
CREATE SCHEMA IF NOT EXISTS ref;         -- reference, SQL-generated
CREATE SCHEMA IF NOT EXISTS restricted;  -- usernames and raw API responses

-- ---------- ref ----------

CREATE TABLE IF NOT EXISTS ref.time_classes (
    time_class      VARCHAR(16) PRIMARY KEY
                    CHECK (time_class IN ('ultrabullet', 'bullet', 'blitz', 'rapid', 'classical', 'correspondence')),
    min_est_seconds INTEGER,   -- inclusive
    max_est_seconds INTEGER,   -- exclusive; null for classical and correspondence
    sort_order      SMALLINT
);

CREATE TABLE IF NOT EXISTS ref.rating_bands (
    band_id    SMALLINT PRIMARY KEY,  -- 1 = lowest
    min_elo    SMALLINT,              -- inclusive
    max_elo    SMALLINT,              -- inclusive; null for the open top band
    band_label VARCHAR(12)
);

-- ---------- core ----------

CREATE TABLE IF NOT EXISTS core.games (
    game_id           VARCHAR(8)  PRIMARY KEY,
    source_month      CHAR(7)     NOT NULL,
    event             VARCHAR(120) NOT NULL,
    is_tournament     BOOLEAN     NOT NULL,
    time_control      VARCHAR(16) NOT NULL,
    base_seconds      INTEGER     CHECK (base_seconds >= 0),
    increment_seconds INTEGER     CHECK (increment_seconds >= 0),
    time_class        VARCHAR(16) NOT NULL REFERENCES ref.time_classes (time_class),
    started_at        TIMESTAMP   NOT NULL,
    white_id          CHAR(16)    NOT NULL,
    black_id          CHAR(16)    NOT NULL,
    white_elo         SMALLINT    NOT NULL,
    black_elo         SMALLINT    NOT NULL,
    white_rating_diff SMALLINT,
    black_rating_diff SMALLINT,
    white_title       VARCHAR(4),
    black_title       VARCHAR(4),
    has_bot           BOOLEAN     NOT NULL,
    result            VARCHAR(7)  NOT NULL CHECK (result IN ('1-0', '0-1', '1/2-1/2', '*')),
    termination       VARCHAR(32) NOT NULL,
    eco               CHAR(3),
    opening           VARCHAR(160),
    n_plies           SMALLINT    NOT NULL CHECK (n_plies >= 0),
    has_clock         BOOLEAN     NOT NULL,
    has_eval          BOOLEAN     NOT NULL,
    ingested_at       TIMESTAMP   NOT NULL,
    -- base/increment are null exactly for correspondence games
    CHECK ((time_class = 'correspondence') = (base_seconds IS NULL AND increment_seconds IS NULL))
);

CREATE TABLE IF NOT EXISTS core.moves (
    game_id       VARCHAR(8) NOT NULL REFERENCES core.games (game_id),
    ply           SMALLINT   NOT NULL CHECK (ply >= 1),
    color         CHAR(1)    NOT NULL CHECK (color IN ('w', 'b')),
    player_id     CHAR(16)   NOT NULL,
    san           VARCHAR(10) NOT NULL,
    uci           VARCHAR(5) NOT NULL,
    clock_after_s REAL       CHECK (clock_after_s >= 0),
    move_time_s   REAL,      -- can be negative, see data dictionary §6
    eval_cp       INTEGER,
    eval_mate     SMALLINT,
    PRIMARY KEY (game_id, ply)
);

CREATE TABLE IF NOT EXISTS core.player_labels (
    player_id          CHAR(16)   PRIMARY KEY,
    label              VARCHAR(8) NOT NULL CHECK (label IN ('positive', 'negative', 'unknown')),
    tos_violation      BOOLEAN,
    disabled           BOOLEAN,
    found              BOOLEAN    NOT NULL,
    account_created_at TIMESTAMP,
    label_fetched_at   TIMESTAMP  NOT NULL
);

CREATE TABLE IF NOT EXISTS core.players (
    player_id     CHAR(16) PRIMARY KEY,
    n_games       INTEGER,
    n_games_white INTEGER,
    first_game_at TIMESTAMP,
    last_game_at  TIMESTAMP,
    mean_elo      REAL,
    max_elo       SMALLINT,
    title         VARCHAR(4),
    is_bot        BOOLEAN
);

CREATE TABLE IF NOT EXISTS core.baselines (
    band_id               SMALLINT    REFERENCES ref.rating_bands (band_id),
    time_class            VARCHAR(16) REFERENCES ref.time_classes (time_class),
    n_players             INTEGER,
    n_games               INTEGER,
    n_moves               INTEGER,
    mean_move_time_s      REAL,
    sd_move_time_s        REAL,
    p10_move_time_s       REAL,
    median_move_time_s    REAL,
    p90_move_time_s       REAL,
    share_moves_under_2s  REAL,
    share_games_with_eval REAL,
    computed_at           TIMESTAMP,
    PRIMARY KEY (band_id, time_class)
);

-- ---------- restricted ----------

CREATE TABLE IF NOT EXISTS restricted.identity (
    player_id        CHAR(16)    PRIMARY KEY,
    username         VARCHAR(30) NOT NULL UNIQUE,
    first_seen_month CHAR(7)     NOT NULL
);

CREATE TABLE IF NOT EXISTS restricted.user_profiles_raw (
    username   VARCHAR(30) PRIMARY KEY,  -- as queried, lowercase
    found      BOOLEAN     NOT NULL,
    response   JSONB,                    -- full API object, null if not found
    fetched_at TIMESTAMPTZ NOT NULL,
    batch_id   INTEGER     NOT NULL
);
