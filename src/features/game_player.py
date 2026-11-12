"""Workstream B, step 2: per-game, per-player features (lake/features/game_player).

Run from the repo root with the venv active:
    PYTHONPATH=. spark-submit --driver-memory 3g src/features/game_player.py --config config/local.yaml

One row per (game_id, player_id). BOT games and games with 0 moves are excluded.
Timing features ignore negative move times (lag compensation / "add time"), as in the Part 1 baselines.
Accuracy features (acpl) exist only for games with [%eval] values; they are null otherwise.

Modelling choices (write them in the report):
  - low clock = clock after the move < 10% of the starting time (base_seconds)
  - centipawn loss of a move = how much the mover's evaluation dropped versus the previous ply,
    from the mover's point of view, floored at 0; evals are capped at +/-1000 cp
  - mate evals (eval_cp is null) and ply 1 (no previous eval) are skipped
"""

import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.common.config import build_arg_parser, load_config
from src.common.logging_setup import log_duration, setup_logging
from src.common.spark import get_spark
from src.features.derived import band_id

STEP = "features_game_player"
LOW_CLOCK_SHARE = 0.10   # low clock: clock_after_s < 10% of base_seconds
CP_CAP = 1000            # evals are capped at +/- 1000 centipawns before differencing
log = logging.getLogger(STEP)

# Numeric per-game features: baselined per band x time class (src/features/baselines.py)
# and averaged per player (src/features/player.py)
GAME_FEATURES = [
    "n_moves", "move_time_mean_s", "move_time_sd_s", "move_time_cv",
    "move_time_p10_s", "move_time_median_s", "move_time_p90_s", "share_moves_under_2s",
    "share_low_clock", "move_time_mean_low_clock_s", "move_time_cv_low_clock", "acpl",
]


def build_accuracy(moves: DataFrame) -> DataFrame:
    """Average centipawn loss (acpl) per (game_id, player_id), from consecutive [%eval] values."""
    cp = F.greatest(F.lit(-CP_CAP), F.least(F.lit(CP_CAP), F.col("eval_cp")))
    sign = F.when(F.col("color") == "w", 1).otherwise(-1)  # evals are from White's point of view
    w = Window.partitionBy("game_id").orderBy("ply")
    ev = (
        moves.filter(F.col("eval_cp").isNotNull())  # only games with evals; mates are skipped
        .select("game_id", "ply", "player_id", "color", cp.alias("cp"))
        .withColumn("prev_cp", F.lag("cp").over(w))
        .withColumn("prev_ply", F.lag("ply").over(w))
        .withColumn(
            "cp_loss",
            F.when(F.col("ply") - F.col("prev_ply") == 1,
                   F.greatest(F.lit(0), sign * (F.col("prev_cp") - F.col("cp")))),
        )
    )
    return ev.groupBy("game_id", "player_id").agg(
        F.avg("cp_loss").cast("float").alias("acpl"),
        F.count("cp_loss").cast("int").alias("n_eval_moves"),
    )


def build_game_player(games: DataFrame, moves: DataFrame) -> DataFrame:
    """Timing + accuracy features, one row per (game_id, player_id)."""
    g = (games.filter(~F.col("has_bot") & (F.col("n_plies") > 0))
         .select("game_id", "source_month", "time_class", "started_at", "base_seconds",
                 "white_elo", "black_elo"))
    m = moves.select("game_id", "color", "player_id", "clock_after_s", "move_time_s")
    elo = F.when(F.col("color") == "w", F.col("white_elo")).otherwise(F.col("black_elo"))

    mv = (
        m.join(g, "game_id")
        .withColumn("elo", elo)
        .withColumn("band_id", band_id(F.col("elo")))
        .withColumn("mt", F.when(F.col("move_time_s") >= 0, F.col("move_time_s")))
        .withColumn("low", F.col("clock_after_s") < LOW_CLOCK_SHARE * F.col("base_seconds"))
        .withColumn("mt_low", F.when(F.col("low"), F.col("mt")))
    )
    keys = ["game_id", "player_id", "color", "source_month", "time_class", "started_at", "elo", "band_id"]
    timing = (
        mv.groupBy(*keys).agg(
            F.count("*").cast("int").alias("n_moves"),
            F.count("mt").cast("int").alias("n_timed_moves"),
            F.sum("mt").alias("time_used"),
            F.avg("mt").alias("mean"),
            F.stddev_samp("mt").alias("sd"),
            F.percentile_approx("mt", [0.10, 0.50, 0.90], 10000).alias("pct"),
            F.avg(F.when(F.col("mt").isNotNull(), (F.col("mt") < 2).cast("int"))).alias("share_under_2s"),
            F.avg(F.col("low").cast("int")).alias("share_low_clock"),
            F.count("mt_low").cast("int").alias("n_low_clock_moves"),
            F.avg("mt_low").alias("mean_low"),
            F.stddev_samp("mt_low").alias("sd_low"),
        )
        .select(
            *keys, "n_moves", "n_timed_moves",
            F.col("time_used").cast("float").alias("time_used_s"),
            F.col("mean").cast("float").alias("move_time_mean_s"),
            F.col("sd").cast("float").alias("move_time_sd_s"),
            F.when(F.col("mean") > 0, F.col("sd") / F.col("mean")).cast("float").alias("move_time_cv"),
            F.col("pct")[0].cast("float").alias("move_time_p10_s"),
            F.col("pct")[1].cast("float").alias("move_time_median_s"),
            F.col("pct")[2].cast("float").alias("move_time_p90_s"),
            F.col("share_under_2s").cast("float").alias("share_moves_under_2s"),
            F.col("share_low_clock").cast("float"),
            "n_low_clock_moves",
            F.col("mean_low").cast("float").alias("move_time_mean_low_clock_s"),
            F.when(F.col("mean_low") > 0, F.col("sd_low") / F.col("mean_low"))
            .cast("float").alias("move_time_cv_low_clock"),
        )
    )
    acc = build_accuracy(moves)
    return (
        timing.join(acc, ["game_id", "player_id"], "left")
        .withColumn("n_eval_moves", F.coalesce(F.col("n_eval_moves"), F.lit(0)))
        .withColumn("has_evals", F.col("n_eval_moves") > 0)
    )


def main() -> None:
    parser = build_arg_parser("Per-game, per-player timing and accuracy features")
    args = parser.parse_args()
    cfg = load_config(args.config, args.months)
    setup_logging(STEP, cfg["paths"]["log_dir"])
    spark = get_spark(cfg, STEP)

    lake = cfg["paths"]["lake_dir"]
    months = cfg["months"]
    games = spark.read.parquet(f"{lake}/games").filter(F.col("source_month").isin(months))
    moves = spark.read.parquet(f"{lake}/moves").filter(F.col("source_month").isin(months))

    out_path = f"{lake}/features/game_player"
    with log_duration(log, "game_player features"):
        df = build_game_player(games, moves)
        df.write.mode("overwrite").partitionBy("source_month", "time_class").parquet(out_path)
        out = spark.read.parquet(out_path)
        stats = out.agg(
            F.count("*").alias("rows"),
            F.countDistinct("game_id").alias("games"),
            F.sum(F.col("has_evals").cast("int")).alias("rows_with_evals"),
            F.avg("move_time_mean_s").alias("avg_mean_move_time"),
            F.avg("acpl").alias("avg_acpl"),
        ).first()
        log.info("game_player written: %d rows, %d games, %d rows with evals, avg move time %.3f s, avg acpl %.1f",
                 stats["rows"], stats["games"], stats["rows_with_evals"],
                 stats["avg_mean_move_time"], stats["avg_acpl"])
        out.drop("game_id", "player_id").show(5, truncate=False, vertical=True)

    spark.stop()


if __name__ == "__main__":
    main()