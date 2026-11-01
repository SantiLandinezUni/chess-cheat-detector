-- Insert the staged moves (their months were cleared by merge_games.sql in the same transaction).
INSERT INTO core.moves SELECT * FROM core.stg_moves;
