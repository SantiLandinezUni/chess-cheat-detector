"""Workstream B, step 4: population baselines for every feature (lake/baselines).

Run from the repo root with the venv active, after player.py:
    PYTHONPATH=. spark-submit --driver-memory 3g src/features/baselines.py --config config/local.yaml

One row per (level, band_id, time_class, feature):
  - level "game":   the per-game features of lake/features/game_player (what the streaming scorer
                    compares a live game with)
  - level "player": the per-player features of lake/features/player, over players with at least
                    --min-games games in that time class (means of 1-2 games are mostly noise)
Population: no BOT games, no 0-move games (already excluded upstream). Nulls are skipped per feature.
Percentiles are approximate (percentile_approx, accuracy 10000) so the job scales to billions of rows.
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config import build_arg_parser, load_config
from src.common.logging_setup import log_duration, setup_logging
from src.common.spark import get_spark
from src.features.game_player import GAME_FEATURES
from src.features.player import PLAYER_FEATURES

STEP = "features_baselines"
PERCENTILES = [0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]
PCT_NAMES = ["p01", "p05", "p10", "p25", "p50", "p75", "p90", "p95", "p99"]
DEFAULT_MIN_GAMES = 5
log = logging.getLogger(STEP)


def to_long(df: DataFrame, features: list[str]) -> DataFrame:
    """Unpivot the feature columns into (band_id, time_class, feature, value), dropping nulls."""
    pairs = ", ".join(f"'{f}', CAST(`{f}` AS DOUBLE)" for f in features)
    return (df.select("band_id", "time_class", F.expr(f"stack({len(features)}, {pairs}) AS (feature, value)"))
            .filter(F.col("value").isNotNull() & ~F.isnan("value")))


def summarise(long_df: DataFrame, level: str) -> DataFrame:
    """Count, mean, sd and percentiles of each feature per band x time class."""
    pct = F.percentile_approx("value", PERCENTILES, 10000)
    return (
        long_df.groupBy("band_id", "time_class", "feature").agg(
            F.count("*").alias("n"), F.avg("value").alias("mean"),
            F.stddev_samp("value").alias("sd"), pct.alias("pct"),
        )
        .select(
            F.lit(level).alias("level"), "band_id", "time_class", "feature", F.col("n").cast("long"),
            F.col("mean").cast("double"), F.col("sd").cast("double"),
            *[F.col("pct")[i].cast("double").alias(name) for i, name in enumerate(PCT_NAMES)],
            F.current_timestamp().alias("computed_at"),
        )
    )


def build_baselines(game_player: DataFrame, player: DataFrame, min_games: int) -> DataFrame:
    """Game-level and player-level baselines in one long table."""
    game = summarise(to_long(game_player, GAME_FEATURES), "game")
    plr = summarise(to_long(player.filter(F.col("n_games") >= min_games), PLAYER_FEATURES), "player")
    return game.unionByName(plr)


def main() -> None:
    parser = build_arg_parser("Baselines of every feature per rating band x time class")
    parser.add_argument("--min-games", type=int, default=DEFAULT_MIN_GAMES,
                        help=f"minimum games in a time class for a player to enter the player-level "
                             f"baselines (default {DEFAULT_MIN_GAMES})")
    args = parser.parse_args()
    cfg = load_config(args.config, args.months)
    setup_logging(STEP, cfg["paths"]["log_dir"])
    spark = get_spark(cfg, STEP)

    lake = cfg["paths"]["lake_dir"]
    gp = spark.read.parquet(f"{lake}/features/game_player").filter(F.col("source_month").isin(cfg["months"]))
    player = spark.read.parquet(f"{lake}/features/player")

    out_path = f"{lake}/baselines"
    with log_duration(log, "baselines"):
        build_baselines(gp, player, args.min_games).write.mode("overwrite").parquet(out_path)
        out = spark.read.parquet(out_path)
        for row in out.groupBy("level").agg(F.count("*").alias("rows"),
                                              F.countDistinct("feature").alias("features")).collect():
            log.info("baselines written: level=%s %d rows, %d features", row["level"], row["rows"], row["features"])
        (out.filter((F.col("time_class") == "blitz") & (F.col("feature").isin("move_time_cv", "acpl")))
         .orderBy("level", "feature", "band_id").show(30, truncate=False))

    spark.stop()


if __name__ == "__main__":
    main()
