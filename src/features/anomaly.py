"""Workstream B, step 5: per-player anomaly scores against the player's band (lake/anomaly/player).

Run from the repo root with the venv active, after baselines.py:
    PYTHONPATH=. spark-submit --driver-memory 3g src/features/anomaly.py --config config/local.yaml

Grain: one row per (player_id, time_class), for players with at least --min-games games in that time
class (the same players as the player-level baselines). BOT and 0-move games are excluded upstream.

Modelling choices (write them in the report):
  - per feature, a robust z-score against the player-level baseline of the player's band x time class:
        z = (x - median) / (IQR / 1.349)        (falls back to the sd when the IQR is 0)
    The timing features are skewed, so median/IQR is less distorted by outliers than mean/sd.
    Cells with fewer than MIN_CELL_PLAYERS players get no z (the baseline is too thin).
  - combined anomaly_score = mean over the available "suspicious-direction" features of
    max(0, direction x z), each term capped at Z_CAP. Direction -1 = low values are suspicious
    (very even timing, low centipawn loss). It is a heuristic ranking, not a probability.
  - anomaly_pct = percent rank of anomaly_score within band x time class (1 = most anomalous).
"""

import logging

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from src.common.config import build_arg_parser, load_config
from src.common.logging_setup import log_duration, setup_logging
from src.common.spark import get_spark
from src.features.baselines import DEFAULT_MIN_GAMES

STEP = "features_anomaly"
MIN_CELL_PLAYERS = 30
Z_CAP = 5.0
IQR_TO_SD = 1.349  # IQR of a normal distribution in sd units
log = logging.getLogger(STEP)

# Features scored with a z-score. direction: -1 = low is suspicious, +1 = high is suspicious,
# 0 = reported only, not part of the combined score.
SCORED = {
    "avg_move_time_cv": -1,             # engine relay gives evenly spaced moves
    "avg_move_time_cv_low_clock": -1,   # stays even when the clock is low
    "avg_share_moves_under_2s": -1,     # few instant moves, even obvious recaptures take time
    "avg_session_move_time_cv": -1,     # same timing pattern game after game in a session
    "avg_acpl": -1,                     # accuracy above the band (only games with evals)
    "avg_move_time_mean_s": 0,
    "avg_move_time_median_s": 0,
    "avg_share_low_clock": 0,
    "avg_n_moves": 0,
}


def robust_z(x: Column, p25: Column, p50: Column, p75: Column, sd: Column) -> Column:
    """(x - median) / scale, with scale = IQR / 1.349, or the sd when the IQR is 0; null if both are 0."""
    iqr_scale = (p75 - p25) / IQR_TO_SD
    scale = F.when(iqr_scale > 0, iqr_scale).when(sd > 0, sd)
    return (x - p50) / scale


def build_anomaly(player: DataFrame, baselines: DataFrame, min_games: int) -> DataFrame:
    """z-score per scored feature, combined anomaly score and its percent rank within the cell."""
    b = baselines.filter((F.col("level") == "player") & (F.col("n") >= MIN_CELL_PLAYERS))
    out = player.filter(F.col("n_games") >= min_games).select(
        "player_id", "time_class", "band_id", "n_games", "n_games_with_evals", *SCORED)
    for feat in SCORED:
        cell = b.filter(F.col("feature") == feat).select(
            "band_id", "time_class", F.col("p25").alias("_p25"), F.col("p50").alias("_p50"),
            F.col("p75").alias("_p75"), F.col("sd").alias("_sd"))
        out = (out.join(F.broadcast(cell), ["band_id", "time_class"], "left")
               .withColumn(f"z_{feat}", robust_z(F.col(feat), F.col("_p25"), F.col("_p50"),
                                                  F.col("_p75"), F.col("_sd")).cast("float"))
               .drop("_p25", "_p50", "_p75", "_sd"))

    terms = [F.least(F.lit(Z_CAP), F.greatest(F.lit(0.0), d * F.col(f"z_{f}")))
             for f, d in SCORED.items() if d != 0]
    present = [F.col(f"z_{f}").isNotNull().cast("int") for f, d in SCORED.items() if d != 0]
    n_terms = sum(present[1:], present[0])
    total = sum((F.coalesce(t, F.lit(0.0)) for t in terms[1:]), F.coalesce(terms[0], F.lit(0.0)))
    out = (out.withColumn("n_score_features", n_terms.cast("int"))
           .withColumn("anomaly_score", F.when(F.col("n_score_features") > 0,
                                               total / F.col("n_score_features")).cast("float")))
    w = Window.partitionBy("band_id", "time_class").orderBy("anomaly_score")
    out = out.withColumn("anomaly_pct",
                         F.when(F.col("anomaly_score").isNotNull(), F.percent_rank().over(w)).cast("float"))
    return out.select("player_id", "time_class", "band_id", "n_games", "n_games_with_evals",
                      *[f"z_{f}" for f in SCORED], "n_score_features", "anomaly_score", "anomaly_pct",
                      F.current_timestamp().alias("computed_at"))


def main() -> None:
    parser = build_arg_parser("Per-player anomaly scores against the band x time class baselines")
    parser.add_argument("--min-games", type=int, default=DEFAULT_MIN_GAMES,
                        help=f"minimum games in a time class for a player to be scored (default {DEFAULT_MIN_GAMES})")
    args = parser.parse_args()
    cfg = load_config(args.config, args.months)
    setup_logging(STEP, cfg["paths"]["log_dir"])
    spark = get_spark(cfg, STEP)

    lake = cfg["paths"]["lake_dir"]
    player = spark.read.parquet(f"{lake}/features/player")
    baselines = spark.read.parquet(f"{lake}/baselines")

    out_path = f"{lake}/anomaly/player"
    with log_duration(log, "anomaly scores"):
        build_anomaly(player, baselines, args.min_games).write.mode("overwrite").parquet(out_path)
        out = spark.read.parquet(out_path)
        stats = out.agg(
            F.count("*").alias("rows"),
            F.count("anomaly_score").alias("scored"),
            F.avg("anomaly_score").alias("mean_score"),
            F.expr("percentile_approx(anomaly_score, 0.99)").alias("p99_score"),
        ).first()
        log.info("anomaly scores written: %d rows, %d with a score, mean score %.3f, p99 %.3f",
                 stats["rows"], stats["scored"], stats["mean_score"] or 0.0, stats["p99_score"] or 0.0)
        out.groupBy("time_class").agg(F.count("*").alias("players"),
                                      F.avg("anomaly_score").alias("mean_score")).orderBy("time_class").show()

    spark.stop()


if __name__ == "__main__":
    main()
