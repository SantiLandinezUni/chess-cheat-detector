# Serving database and dashboard (Part 2, workstream E2)

Owner: Mohamed. Runs on a laptop; reads only small result tables, never the full lake.

## Files

| File | Purpose |
|---|---|
| `sql/serving_schema.sql` | `serving` schema: `live_scores` and `running_scores` (partitioned by `run_id`), views `game_scores` and `player_scores_live`, `rating_bands` |
| `src/serving/load_serving.py` | Loads the scorer export (L13), the running scores and the optional result tables into `serving` |
| `src/serving/privacy_check.py` | Fails if any serving column could identify a player or a game |
| `scripts/dump_serving.sh` | Privacy check → `pg_dump` of `serving` → restore into a scratch database → row-count comparison |
| `dashboard/app.py` | Streamlit dashboard, reads PostgreSQL only; every chart is aggregated in SQL |
| `config/serving.yaml` | Database URL, input paths, high-risk threshold |

## Run

```bash
pip install streamlit plotly sqlalchemy psycopg2-binary pyyaml pyarrow
# unzip serving_v2.zip into data/serving/serving_v2/ (data/ is git-ignored)
python -m src.serving.load_serving --config config/serving.yaml
streamlit run dashboard/app.py
scripts/dump_serving.sh chess      # database deliverable
```

`SERVING_DB_URL` overrides the database URL in the config. `LICHESS_HASH_KEY` is only needed if an
input file still carries the Lichess `game_id` (the serving_v2 export does not).

## Loading a run: partition swap instead of DELETE (FEEDBACK_ACTIONS 3.4)

`serving.live_scores` and `serving.running_scores` are `PARTITION BY LIST (run_id)`, one partition per
scorer run. The loader reads the input in batches of 200,000 rows and, for each run:

1. creates a staging table `LIKE` the parent, with a `CHECK (run_id = '<run>')` constraint;
2. `COPY`s the batches into it and checks the row count against the input;
3. detaches and drops the old partition of that run (if any) and attaches the staging table as the new one.

All three steps run in one transaction. The dashboard keeps seeing the old run until the commit, and then
the new one, never a half-loaded state. Dropping a partition is instant and leaves no dead rows, so no
`VACUUM` is needed after a reload (a `DELETE` of 6M rows would leave 6M dead tuples). The `CHECK`
constraint lets `ATTACH PARTITION` skip the validation scan. The same pattern applies to the Part 1
loader (`load_postgres.py`) with `source_month` as the partition key: a month is reloaded by swapping
its partition.

## Privacy

- Games are identified only by `game_key`, a 16-hex-character keyed hash computed on the cluster.
  A Lichess `game_id` is public and reveals both usernames; the keyed hash cannot be reversed without the key.
  If an input still has `game_id`, the loader hashes it the same way and drops it.
- Only the 16-character hashed `player_id` is stored. Both keys are enforced by `CHECK` constraints.
- The loader refuses any input with a `username`, `game_id`, `white`, `black` or `site` column, and
  `privacy_check.py` re-checks the whole schema (column names, `player_id` and `game_key` format) before every dump.

## Dashboard pages

| Page | Content | Query |
|---|---|---|
| Overview | KPIs, risk-score histogram, high-risk share by time class | `COUNT`, `width_bucket`, `percentile_cont` |
| Behaviour by band | Median timing features per band × time class, high-risk share per band | `GROUP BY band_id, time_class` |
| High-risk games | Top 50 games, score terms of a selected game | `ORDER BY risk_score DESC LIMIT 100` (index) |
| Suspicion timeline | Running risk score per ply for one logged game (FEEDBACK_ACTIONS 4.4) | one game from `running_scores` |
| Players | Players with two or more high-risk games | `GROUP BY player_id` + `FILTER` |
| Anomaly & model | Tables from workstreams B, C, D when loaded | `SELECT … LIMIT` |

## Tested on

- `scores_60k_games.parquet` (v1, 60,000 October games): 119,886 rows, privacy check and restore check passed.
- Scale test before serving_v2 arrived: the v1 file replicated 50× with new hashed keys (5,994,300 player-games,
  3,000,000 games) plus 70,403 running-score rows for 997 games. Load 2 min 40 s on a laptop-sized container,
  rerun replaced the partition with the same counts; dashboard first load about 22 s, then cached.
