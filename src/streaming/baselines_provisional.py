"""Provisional baselines for the streaming scorer (Part 2, E1): lake/baselines_provisional.

A stand-in for workstream B's lake/baselines (data dictionary L10) until that runs on the full data.
Same column layout as L10, so switching the scorer to B's table is a config change only
(paths.scorer_baselines_dir in config/*.yaml).

Run from the repo root with the venv active:
    PYTHONPATH=. spark-submit --driver-memory 3g src/streaming/baselines_provisional.py \
        --config config/local.yaml
On the cluster: cluster/baselines_provisional.sbatch <month>[,<month>...]

Population: the months of the run, no BOT games, no games with 0 moves, no correspondence.
Timing statistics use only moves with move_time_s >= 0 (as L7 and L8). Rating bands are those of
ref.rating_bands (src/features/derived.py:band_id), from the mover's pre-game rating.

Rows (one per level x band_id x time_class x feature):
  - level "game": the per-game features the scorer compares a live game with, defined exactly as
    in L8 (src/features/game_player.py): n_moves, move_time_mean_s, move_time_sd_s, move_time_cv,
    share_moves_under_2s, share_low_clock, move_time_mean_low_clock_s, move_time_cv_low_clock.
  - level "move": feature move_time_s, the distribution of single move times (p10 / p50 / p90 ...).
    B's table has no "move" level; the scorer only reads level "game".
Percentiles are approximate (percentile_approx, accuracy 10,000) so the job scales to billions of moves.
The per-game features are aggregated in the same query as the move-level rows, so Spark reads and
shuffles the moves once (the join's exchange is reused).
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config import build_arg_parser, load_config
from src.common.logging_setup import log_duration, setup_logging
from src.common.spark import get_spark
from src.features.baselines import summarise, to_long
from src.features.derived import band_id
from src.features.game_player import LOW_CLOCK_SHARE

STEP = "baselines_provisional"
FAST_MOVE_S = 2.0
EXCLUDED_TIME_CLASSES = ["correspondence"]
# Per-game features of the provisional table (a subset of L8's GAME_FEATURES, same definitions)
PROVISIONAL_GAME_FEATURES = [
    "n_moves", "move_time_mean_s", "move_time_sd_s", "move_time_cv", "share_moves_under_2s",
    "share_low_clock", "move_time_mean_low_clock_s", "move_time_cv_low_clock",
]
log = logging.getLogger(STEP)


def move_rows(games: DataFrame, moves: DataFrame) -> DataFrame:
    """One row per move of the population, with the mover's band, its timed move time and low-clock flag."""
    g = (games.filter(~F.col("has_bot") & (F.col("n_plies") > 0)
                      & ~F.col("time_class").isin(EXCLUDED_TIME_CLASSES))
         .select("game_id", "time_class", "base_seconds", "white_elo", "black_elo"))
    m = (moves.filter(~F.col("time_class").isin(EXCLUDED_TIME_CLASSES))
         .select("game_id", "time_class", "color", "player_id", "clock_after_s", "move_time_s"))
    elo = F.when(F.col("color") == "w", F.col("white_elo")).otherwise(F.col("black_elo"))
    low = F.col("clock_after_s") < LOW_CLOCK_SHARE * F.col("base_seconds")
    return (m.join(g, ["game_id", "time_class"])
            .select("game_id", "time_class", "player_id", band_id(elo).alias("band_id"),
                    F.when(F.col("move_time_s") >= 0, F.col("move_time_s")).alias("mt"),
                    low.alias("low")))


def game_features(mv: DataFrame) -> DataFrame:
    """Per (game, player) timing features, same definitions as L8."""
    mt_low = F.when(F.col("low"), F.col("mt"))
    agg = mv.groupBy("game_id", "player_id", "time_class", "band_id").agg(
        F.count("*").alias("n_moves"),
        F.avg("mt").alias("mean"),
        F.stddev_samp("mt").alias("sd"),
        F.avg(F.when(F.col("mt").isNotNull(), (F.col("mt") < FAST_MOVE_S).cast("int"))).alias("share_fast"),
        F.avg(F.col("low").cast("int")).alias("share_low_clock"),
        F.avg(mt_low).alias("mean_low"),
        F.stddev_samp(mt_low).alias("sd_low"),
    )
    return agg.select(
        "band_id", "time_class", "n_moves",
        F.col("mean").alias("move_time_mean_s"),
        F.col("sd").alias("move_time_sd_s"),
        F.when(F.col("mean") > 0, F.col("sd") / F.col("mean")).alias("move_time_cv"),
        F.col("share_fast").alias("share_moves_under_2s"),
        "share_low_clock",
        F.col("mean_low").alias("move_time_mean_low_clock_s"),
        F.when(F.col("mean_low") > 0, F.col("sd_low") / F.col("mean_low")).alias("move_time_cv_low_clock"),
    )


def build_provisional_baselines(games: DataFrame, moves: DataFrame) -> DataFrame:
    """Game-level and move-level baselines in one long table with the L10 layout."""
    mv = move_rows(games, moves)
    game = summarise(to_long(game_features(mv), PROVISIONAL_GAME_FEATURES), "game")
    move_long = mv.filter(F.col("mt").isNotNull()).select(
        "band_id", "time_class", F.lit("move_time_s").alias("feature"), F.col("mt").cast("double").alias("value"))
    return game.unionByName(summarise(move_long, "move"))


def main() -> None:
    parser = build_arg_parser("Provisional baselines for the streaming scorer (lake/baselines_provisional)")
    args = parser.parse_args()
    cfg = load_config(args.config, args.months)
    setup_logging(STEP, cfg["paths"]["log_dir"])
    spark = get_spark(cfg, STEP)

    lake = cfg["paths"]["lake_dir"]
    months = cfg["months"]
    log.info("months=%s lake=%s", months, lake)
    games = spark.read.parquet(f"{lake}/games").filter(F.col("source_month").isin(months))
    moves = spark.read.parquet(f"{lake}/moves").filter(F.col("source_month").isin(months))

    out_path = f"{lake}/baselines_provisional"
    with log_duration(log, "provisional baselines"):
        build_provisional_baselines(games, moves).write.mode("overwrite").parquet(out_path)

    out = spark.read.parquet(out_path)
    for row in out.groupBy("level").agg(F.count("*").alias("rows"), F.countDistinct("feature").alias("features"),
                                        F.sum("n").alias("values")).orderBy("level").collect():
        log.info("written to %s: level=%s %d rows, %d features, %d values",
                 out_path, row["level"], row["rows"], row["features"], row["values"])
    for row in (out.filter((F.col("level") == "game") & (F.col("feature") == "n_moves"))
                .groupBy("time_class").agg(F.sum("n").alias("games")).orderBy("time_class").collect()):
        log.info("player-games in the population: %s %d", row["time_class"], row["games"])
    (out.filter((F.col("time_class") == "blitz") & F.col("feature").isin("move_time_s", "move_time_cv"))
     .select("level", "feature", "band_id", "n", "mean", "sd", "p10", "p50", "p90")
     .orderBy("level", "feature", "band_id").show(30, truncate=False))
    spark.stop()


if __name__ == "__main__":
    main()
