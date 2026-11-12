"""Workstream B, step 1: Spark port of sql/generate_derived.sql (core.players and core.baselines).

Run from the repo root with the venv active:
    PYTHONPATH=. spark-submit --driver-memory 3g src/features/derived.py --config config/local.yaml

Outputs (Parquet, under paths.lake_dir):
    derived/players            one row per player (data dictionary 4.4)
    derived/baselines_part1    one row per rating band x time class (data dictionary 4.5)

On the sample the numbers must match the Part 1 PostgreSQL tables. On the full data pass
--approx-percentiles: exact percentiles over billions of moves do not fit in memory.
"""

import logging

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from src.common.config import build_arg_parser, load_config
from src.common.logging_setup import log_duration, setup_logging
from src.common.spark import get_spark

STEP = "features_derived"
log = logging.getLogger(STEP)


def band_id(elo: Column) -> Column:
    """Rating band of ref.rating_bands: 1 = <800, 2..11 = 200-point bands from 800, 12 = 2800+."""
    return (
        F.when(elo < 800, 1)
        .when(elo >= 2800, 12)
        .otherwise(F.floor((elo - 800) / 200).cast("int") + 2)
        .cast("smallint")
    )


def build_players(games: DataFrame) -> DataFrame:
    """One row per player, counting both colours (same rules as core.players)."""
    cols = ["is_white", "started_at", "elo", "title"]
    white = games.select(F.col("white_id").alias("player_id"), F.lit(True).alias("is_white"),
                         "started_at", F.col("white_elo").alias("elo"), F.col("white_title").alias("title"))
    black = games.select(F.col("black_id").alias("player_id"), F.lit(False).alias("is_white"),
                         "started_at", F.col("black_elo").alias("elo"), F.col("black_title").alias("title"))
    sides = white.unionByName(black).select("player_id", *cols)
    # most recent non-null title: max over (started_at, title) structs, ignoring rows without a title
    latest_title = F.max(F.when(F.col("title").isNotNull(), F.struct("started_at", "title")))
    return sides.groupBy("player_id").agg(
        F.count("*").cast("int").alias("n_games"),
        F.sum(F.col("is_white").cast("int")).cast("int").alias("n_games_white"),
        F.min("started_at").alias("first_game_at"),
        F.max("started_at").alias("last_game_at"),
        F.avg("elo").cast("float").alias("mean_elo"),
        F.max("elo").cast("smallint").alias("max_elo"),
        latest_title.getField("title").alias("title"),
        F.coalesce(F.max(F.col("title") == "BOT"), F.lit(False)).alias("is_bot"),
    )


def build_baselines(games: DataFrame, moves: DataFrame, approx: bool) -> DataFrame:
    """Move-time norms per rating band x time class over human-vs-human games with clocks.

    Same population as core.baselines: no BOT games, games with clocks, moves with move_time_s >= 0.
    The band is the band of the mover's own pre-game Elo.
    """
    g = (games.filter(~F.col("has_bot") & F.col("has_clock"))
         .select("game_id", "time_class", "has_eval", "white_elo", "black_elo"))
    m = moves.filter(F.col("move_time_s") >= 0).select("game_id", "color", "player_id", "move_time_s")
    elo = F.when(F.col("color") == "w", F.col("white_elo")).otherwise(F.col("black_elo"))
    mv = m.join(g, "game_id").withColumn("band_id", band_id(elo))

    def count_distinct(col: str) -> Column:
        return F.approx_count_distinct(col) if approx else F.countDistinct(col)

    def pct(p: float) -> Column:
        if approx:
            return F.percentile_approx("move_time_s", p, 10000)
        return F.expr(f"percentile(move_time_s, {p})")  # exact, linearly interpolated = percentile_cont

    move_cell = mv.groupBy("band_id", "time_class").agg(
        count_distinct("player_id").alias("n_players"),
        count_distinct("game_id").alias("n_games"),
        F.count("*").alias("n_moves"),
        F.avg("move_time_s").alias("mean_move_time_s"),
        F.stddev_samp("move_time_s").alias("sd_move_time_s"),
        pct(0.10).alias("p10_move_time_s"),
        pct(0.50).alias("median_move_time_s"),
        pct(0.90).alias("p90_move_time_s"),
        F.avg((F.col("move_time_s") < 2).cast("int")).alias("share_moves_under_2s"),
    )
    # one row per (game, band, time class), so a game counts once per cell
    game_cell = (
        mv.select("band_id", "time_class", "game_id", "has_eval").distinct()
        .groupBy("band_id", "time_class")
        .agg(F.avg(F.col("has_eval").cast("int")).alias("share_games_with_eval"))
    )
    return (
        move_cell.join(game_cell, ["band_id", "time_class"])
        .select(
            "band_id", "time_class",
            F.col("n_players").cast("int"), F.col("n_games").cast("int"),
            F.col("n_moves").cast("long"),  # a full-data cell can pass 2^31 moves
            F.col("mean_move_time_s").cast("float"), F.col("sd_move_time_s").cast("float"),
            F.col("p10_move_time_s").cast("float"), F.col("median_move_time_s").cast("float"),
            F.col("p90_move_time_s").cast("float"),
            F.col("share_moves_under_2s").cast("float"), F.col("share_games_with_eval").cast("float"),
            F.current_timestamp().alias("computed_at"),
        )
    )


def main() -> None:
    parser = build_arg_parser("Spark port of core.players and core.baselines")
    parser.add_argument("--approx-percentiles", action="store_true",
                        help="approximate percentiles and distinct counts (use on the full data)")
    args = parser.parse_args()
    cfg = load_config(args.config, args.months)
    setup_logging(STEP, cfg["paths"]["log_dir"])
    spark = get_spark(cfg, STEP)

    lake = cfg["paths"]["lake_dir"]
    months = cfg["months"]
    games = spark.read.parquet(f"{lake}/games").filter(F.col("source_month").isin(months))
    moves = spark.read.parquet(f"{lake}/moves").filter(F.col("source_month").isin(months))

    with log_duration(log, "players"):
        players = build_players(games)
        players.write.mode("overwrite").parquet(f"{lake}/derived/players")
        log.info("players written: %d rows", spark.read.parquet(f"{lake}/derived/players").count())

    with log_duration(log, "baselines"):
        baselines = build_baselines(games, moves, args.approx_percentiles)
        baselines.write.mode("overwrite").parquet(f"{lake}/derived/baselines_part1")
        out = spark.read.parquet(f"{lake}/derived/baselines_part1")
        totals = out.agg(F.count("*").alias("cells"), F.sum("n_moves").alias("moves")).first()
        log.info("baselines written: %d cells, %d moves", totals["cells"], totals["moves"])
        out.orderBy("band_id", "time_class").show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()