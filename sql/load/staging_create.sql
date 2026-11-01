-- Empty unlogged staging tables for load_postgres.py (same columns as the targets, no indexes or FKs).
-- Staging for identity lives in `restricted` because it holds usernames.
CREATE UNLOGGED TABLE IF NOT EXISTS core.stg_games (LIKE core.games INCLUDING DEFAULTS);
CREATE UNLOGGED TABLE IF NOT EXISTS core.stg_moves (LIKE core.moves INCLUDING DEFAULTS);
CREATE UNLOGGED TABLE IF NOT EXISTS restricted.stg_identity (LIKE restricted.identity INCLUDING DEFAULTS);
TRUNCATE core.stg_games, core.stg_moves, restricted.stg_identity;
