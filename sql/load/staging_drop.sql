-- Remove the staging tables after a successful load and refresh planner statistics.
DROP TABLE IF EXISTS core.stg_games, core.stg_moves, restricted.stg_identity;
ANALYZE core.games;
ANALYZE core.moves;
ANALYZE restricted.identity;
