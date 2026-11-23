"""Workstream C, step 1: choose which players get a Lichess label (lake/labels_sample).

Run from the repo root with the venv active, after src/features/anomaly.py:
    PYTHONPATH=. spark-submit --driver-memory 3g src/ingestion/sample_labels.py --config config/local.yaml \
        --n-random 80000 --n-high-anomaly 20000

Population: the players in lake/anomaly/player, i.e. players with at least 5 games in some time
class (BOT and 0-move games excluded upstream), so every labelled player also has features.
One row per player: the highest anomaly_pct over their time classes.

Two parts, recorded in `sample_source` (the evaluation must use only the random part, which keeps
the population's base rate; the high-anomaly part adds positives for training):
  - random:        the first --n-random players ordered by xxhash64(player_id, --seed). Hash order
                   instead of rand(), so the sample doesn't depend on Spark's partitioning.
  - high_anomaly:  the --n-high-anomaly most anomalous of the remaining players (anomaly_pct, then
                   anomaly_score). Players without a score are never in this part.
No usernames here: collect_labels.py joins the sample to restricted/identity.
"""

import logging

from pyspark.sql import functions as F

from src.common.config import build_arg_parser, load_config
from src.common.logging_setup import log_duration, setup_logging
from src.common.spark import get_spark

STEP = "sample_labels"
DEFAULT_N_RANDOM = 80_000
DEFAULT_N_HIGH_ANOMALY = 20_000
DEFAULT_SEED = 2026

log = logging.getLogger(STEP)


def main() -> None:
    parser = build_arg_parser("Choose the players to label: random + high-anomaly sample")
    parser.add_argument("--n-random", type=int, default=DEFAULT_N_RANDOM)
    parser.add_argument("--n-high-anomaly", type=int, default=DEFAULT_N_HIGH_ANOMALY)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args = parser.parse_args()
    cfg = load_config(args.config, args.months)
    setup_logging(STEP, cfg["paths"]["log_dir"])
    spark = get_spark(cfg, STEP)
    lake = cfg["paths"]["lake_dir"]
    out_path = f"{lake}/labels_sample"

    with log_duration(log, STEP):
        players = (spark.read.parquet(f"{lake}/anomaly/player")
                   .groupBy("player_id")
                   .agg(F.max("anomaly_pct").alias("anomaly_pct"), F.max("anomaly_score").alias("anomaly_score")))
        n_population = players.count()
        log.info("population (players in anomaly/player): %d; requested random=%d high_anomaly=%d seed=%d",
                 n_population, args.n_random, args.n_high_anomaly, args.seed)

        random_part = (players.withColumn("_h", F.xxhash64("player_id", F.lit(args.seed)))
                       .orderBy("_h", "player_id").limit(args.n_random)
                       .select("player_id", "anomaly_pct", F.lit("random").alias("sample_source")))
        high_part = (players.join(random_part.select("player_id"), "player_id", "left_anti")
                     .where(F.col("anomaly_score").isNotNull())
                     .orderBy(F.desc("anomaly_pct"), F.desc("anomaly_score"), "player_id")
                     .limit(args.n_high_anomaly)
                     .select("player_id", "anomaly_pct", F.lit("high_anomaly").alias("sample_source")))

        (random_part.unionByName(high_part)
         .withColumn("sampled_at", F.current_timestamp())
         .write.mode("overwrite").parquet(out_path))

        out = spark.read.parquet(out_path)
        counts = {r["sample_source"]: r["n"] for r in out.groupBy("sample_source").agg(F.count("*").alias("n")).collect()}
        n_out, n_distinct = out.count(), out.select("player_id").distinct().count()
        log.info("wrote %s: %d players %s; min anomaly_pct in high_anomaly part: %s",
                 out_path, n_out, counts,
                 out.where("sample_source = 'high_anomaly'").agg(F.min("anomaly_pct")).first()[0])
        if n_out != n_distinct:
            raise RuntimeError(f"duplicate players in the sample: {n_out} rows, {n_distinct} distinct")
        for part, wanted in (("random", args.n_random), ("high_anomaly", args.n_high_anomaly)):
            if counts.get(part, 0) < wanted:
                log.warning("%s part has %d players, fewer than the %d requested (population too small)",
                            part, counts.get(part, 0), wanted)
    spark.stop()


if __name__ == "__main__":
    main()
