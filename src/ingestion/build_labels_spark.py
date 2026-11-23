"""Workstream C, step 3: player labels in Spark from the NDJSON profile cache (lake/labels).

Run from the repo root with the venv active, after collect_labels.py --from-sample:
    PYTHONPATH=. spark-submit --driver-memory 3g src/ingestion/build_labels_spark.py --config config/local.yaml

Spark port of sql/labels/player_labels.sql (Part 1), without PostgreSQL. Same rules (data dictionary §6):
    positive  tosViolation = true
    negative  account returned, not disabled, no tosViolation
    unknown   disabled without tosViolation, or not returned by the API
On a returned account an absent flag means false; flags are null when the account wasn't returned.

Input: data/raw/profiles/profiles_<fetched_at>_b<batch_id>.ndjson (API objects) and notfound_*.ndjson,
with fetched_at and batch_id taken from the file name. A name fetched more than once keeps its
newest fetch, as in the Part 1 reload.

--population sample (default): the players of lake/labels_sample, with their sample_source.
--population identity: every player in restricted/identity (what Part 1 labelled; used to check
that this port gives the Part 1 result). Players without a fetched profile get no row; their
number is logged. The output has no usernames.
"""

import logging

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.common.config import build_arg_parser, load_config
from src.common.logging_setup import log_duration, setup_logging
from src.common.spark import get_spark

STEP = "build_labels_spark"
# Only the fields the label rules use; Spark skips the rest of each (large) API object
PROFILE_SCHEMA = T.StructType([
    T.StructField("id", T.StringType()),
    T.StructField("tosViolation", T.BooleanType()),
    T.StructField("disabled", T.BooleanType()),
    T.StructField("createdAt", T.LongType()),
])
NOTFOUND_SCHEMA = T.StructType([T.StructField("username", T.StringType())])
FILE_NAME_RE = r"(profiles|notfound)_(\d{8}T\d{6}Z)_b(\d+)\.ndjson$"
LABEL_COLUMNS = ["player_id", "label", "tos_violation", "disabled", "found",
                 "account_created_at", "label_fetched_at", "sample_source"]

log = logging.getLogger(STEP)


def with_batch_info(df: DataFrame) -> DataFrame:
    """fetched_at (UTC) and batch_id from the name of the file each row came from."""
    name = F.input_file_name()
    return (df.withColumn("fetched_at", F.to_timestamp(F.regexp_extract(name, FILE_NAME_RE, 2), "yyyyMMdd'T'HHmmss'Z'"))
              .withColumn("batch_id", F.regexp_extract(name, FILE_NAME_RE, 3).cast("int")))


def read_profiles(spark: SparkSession, profiles_dir: str) -> DataFrame:
    """One row per username: its newest fetch, found or not."""
    found = (with_batch_info(spark.read.schema(PROFILE_SCHEMA).json(f"{profiles_dir}/profiles_*.ndjson"))
             .select(F.col("id").alias("username"), F.lit(True).alias("found"), "tosViolation", "disabled",
                     "createdAt", "fetched_at", "batch_id"))
    not_found = (with_batch_info(spark.read.schema(NOTFOUND_SCHEMA).json(f"{profiles_dir}/notfound_*.ndjson"))
                 .select("username", F.lit(False).alias("found"),
                         F.lit(None).cast("boolean").alias("tosViolation"), F.lit(None).cast("boolean").alias("disabled"),
                         F.lit(None).cast("long").alias("createdAt"), "fetched_at", "batch_id"))
    newest = Window.partitionBy("username").orderBy(F.desc("fetched_at"), F.desc("batch_id"))
    return (found.unionByName(not_found)
            .withColumn("_rank", F.row_number().over(newest))
            .where("_rank = 1").drop("_rank"))


def apply_label_rules(df: DataFrame) -> DataFrame:
    """Label rules of data dictionary §6, identical to sql/labels/player_labels.sql."""
    tos = F.when(F.col("found"), F.coalesce("tosViolation", F.lit(False)))
    disabled = F.when(F.col("found"), F.coalesce("disabled", F.lit(False)))
    label = (F.when(F.col("tosViolation") == True, "positive")  # noqa: E712 (null-safe in Spark SQL)
              .when(F.col("found") & ~F.coalesce("disabled", F.lit(False)), "negative")
              .otherwise("unknown"))
    return (df.withColumn("label", label)
              .withColumn("tos_violation", tos)
              .withColumn("disabled", disabled)
              .withColumn("account_created_at", F.timestamp_seconds(F.col("createdAt") / 1000))
              .withColumn("label_fetched_at", F.col("fetched_at")))


def main() -> None:
    parser = build_arg_parser("Player labels in Spark from the NDJSON profile cache")
    parser.add_argument("--population", choices=["sample", "identity"], default="sample",
                        help="sample = lake/labels_sample (Part 2); identity = every player (Part 1 check)")
    args = parser.parse_args()
    cfg = load_config(args.config, args.months)
    setup_logging(STEP, cfg["paths"]["log_dir"])
    spark = get_spark(cfg, STEP)
    lake = cfg["paths"]["lake_dir"]
    out_path = f"{lake}/labels"

    with log_duration(log, STEP):
        identity = spark.read.parquet(f"{lake}/restricted/identity").select(
            "player_id", F.lower("username").alias("username"))
        if args.population == "sample":
            players = (spark.read.parquet(f"{lake}/labels_sample").select("player_id", "sample_source")
                       .join(identity, "player_id", "left"))
        else:
            players = identity.withColumn("sample_source", F.lit(None).cast("string"))
        n_players = players.count()

        labels = (apply_label_rules(players.join(read_profiles(spark, cfg["paths"]["profiles_dir"]), "username", "inner"))
                  .select(*LABEL_COLUMNS))  # username dropped here: lake/labels is pseudonymous
        labels.write.mode("overwrite").parquet(out_path)

        out = spark.read.parquet(out_path)
        n_out, n_distinct = out.count(), out.select("player_id").distinct().count()
        if n_out != n_distinct:
            raise RuntimeError(f"duplicate players in lake/labels: {n_out} rows, {n_distinct} distinct")
        dist = {(r["sample_source"], r["label"]): r["n"]
                for r in out.groupBy("sample_source", "label").agg(F.count("*").alias("n")).collect()}
        log.info("wrote %s: %d labelled players of %d in population=%s; by (sample_source, label): %s",
                 out_path, n_out, n_players, args.population, dict(sorted(dist.items(), key=str)))
        if n_out < n_players:
            log.warning("%d of %d players have no fetched profile and no label; rerun collect_labels.py",
                        n_players - n_out, n_players)
    spark.stop()


if __name__ == "__main__":
    main()
