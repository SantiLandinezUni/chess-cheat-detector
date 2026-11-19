# Serving database and dashboard (Part 2, workstream E2)

Owner: Mohamed. Runs on a laptop; reads only small result tables, never the full lake.

## Files

| File | Purpose |
|---|---|
| `sql/serving_schema.sql` | `serving` schema: `live_scores`, views `game_scores` and `player_scores_live`, `rating_bands` |
| `src/serving/load_serving.py` | Loads the scorer export (L13) and the optional result tables into `serving`, one transaction per table |
| `src/serving/privacy_check.py` | Fails if any serving column could identify a player or a game |
| `scripts/dump_serving.sh` | Privacy check → `pg_dump` of `serving` → restore into a scratch database → row-count comparison |
| `dashboard/app.py` | Streamlit dashboard, reads PostgreSQL only |
| `config/serving.yaml` | Database URL, input paths, high-risk threshold |

## Run

```bash
pip install streamlit plotly sqlalchemy psycopg2-binary pyyaml pyarrow
# put the scorer export where config/serving.yaml points (data/ is git-ignored)
python -m src.serving.load_serving --config config/serving.yaml
streamlit run dashboard/app.py
scripts/dump_serving.sh chess      # database deliverable
```

`SERVING_DB_URL` overrides the database URL in the config. `LICHESS_HASH_KEY` must be set (from `.env`).

## Privacy

- `game_id` never reaches the database. The loader replaces it with `game_key` = HMAC-SHA256(`LICHESS_HASH_KEY`, `game_id`), first 12 hex characters. A Lichess `game_id` is public and reveals both usernames; the keyed hash cannot be reversed without the key.
- Only the 16-character hashed `player_id` is stored.
- The loader refuses any input with a `username`, `game_id`, `white`, `black` or `site` column, and `privacy_check.py` re-checks the whole schema before every dump.

## Tested on

`scores_60k_games.parquet` (60,000 October games, scorer run with `timing-z-v1` on `baselines_provisional`):
119,886 rows loaded; rerun gives the same count; privacy check passed; dump restored with identical row counts
(`docs/validation_results/serving_restore_check.csv`). Dashboard screenshots are in the report (Figures 8–10).
Numbers quoted in the report: `docs/validation_results/dashboard_*.csv`.
