-- Replace every source_month present in staging: delete its moves and games, insert the new games.
-- Runs in the same transaction as merge_moves.sql, so a failed load leaves the old data untouched.
DELETE FROM core.moves m
USING core.games g
WHERE m.game_id = g.game_id
  AND g.source_month IN (SELECT DISTINCT source_month FROM core.stg_games);

DELETE FROM core.games
WHERE source_month IN (SELECT DISTINCT source_month FROM core.stg_games);

INSERT INTO core.games SELECT * FROM core.stg_games;
