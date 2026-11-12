"""Workstream B, step 3: player-level features and sessions (lake/features/player).

Run from the repo root with the venv active, after game_player.py:
    PYTHONPATH=. spark-submit --driver-memory 3g src/features/player.py --config config/local.yaml

Grain: one row per (player_id, time_class). Timing habits differ completely between time
classes, and anomaly is measured against band x time class, so a player's blitz and bullet games
are never averaged together. Built from all months of the run; not partitioned, overwritten.

Sessions (modelling choices, write them in the report):
  - a player's games across all time classes, in start order; a game starts a new session when it
    starts more than 30 min after the previous game's estimated end
  - estimated end = started_at + 2 x the player's own time used (sum of move times): the opponent's
    clock usage is not in the row, and the two sides use similar time
  - within-session consistency = cv of the per-game mean move time across the games of a session in
    one time class, for sessions with at least 2 games in that class; averaged per player
"""

import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.common.config import build_arg_parser, load_config
from src.common.logging_setup import log_duration, setup_logging
from src.common.spark import get_spark
from src.features.derived import band_id
from src.features.game_player import GAME_FEATURES

STEP = "features_player"
SESSION_GAP_S = 30 * 60
log = logging.getLogger(STEP)

# Numeric player features, baselined per band x time class in src/features/baselines.py
PLAYER_FEATURES = (
    [f"avg_{f}" for f in GAME_FEATURES] + [f"sd_{f}" for f in GAME_FEATURES]
    + ["avg_games_per_session", "avg_session_move_time_cv"]
)


def build_sessions(gp: DataFrame) -> DataFrame:
    """Session statistics per (player_id, time_class)."""
    w = Window.partitionBy("player_id").orderBy("started_at", "game_id")
    end = F.col("started_at").cast("double") + 2 * F.coalesce(F.col("time_used_s"), F.lit(0.0))
    g = (
        gp.select("player_id", "game_id", "time_class", "started_at", "time_used_s", "move_time_mean_s")
        .withColumn("prev_end", F.lag(end).over(w))
        .withColumn("new_session",
                    (F.col("prev_end").isNull()
                     | (F.col("started_at").cast("double") - F.col("prev_end") > SESSION_GAP_S)).cast("int"))
        .withColumn("session_no", F.sum("new_session").over(w.rowsBetween(Window.unboundedPreceding, 0)))
    )
    per_session = g.groupBy("player_id", "time_class", "session_no").agg(
        F.count("*").alias("games"),
        F.avg("move_time_mean_s").alias("m"),
        F.stddev_samp("move_time_mean_s").alias("s"),
    )
    cv = F.when((F.col("games") >= 2) & (F.col("m") > 0), F.col("s") / F.col("m"))
    return per_session.groupBy("player_id", "time_class").agg(
        F.count("*").cast("int").alias("n_sessions"),
        F.sum((F.col("games") >= 2).cast("int")).cast("int").alias("n_multi_game_sessions"),
        F.avg("games").cast("float").alias("avg_games_per_session"),
        F.avg(cv).cast("float").alias("avg_session_move_time_cv"),
    )


def build_player(gp: DataFrame) -> DataFrame:
    """Aggregate game_player per (player_id, time_class) and add the session features."""
    aggs = [
        F.count("*").cast("int").alias("n_games"),
        F.sum(F.col("has_evals").cast("int")).cast("int").alias("n_games_with_evals"),
        F.sum("n_moves").cast("long").alias("total_moves"),
        F.avg("elo").cast("float").alias("mean_elo"),
        F.min("started_at").alias("first_game_at"),
        F.max("started_at").alias("last_game_at"),
    ]
    aggs += [F.avg(f).cast("float").alias(f"avg_{f}") for f in GAME_FEATURES]
    aggs += [F.stddev_samp(f).cast("float").alias(f"sd_{f}") for f in GAME_FEATURES]
    players = (
        gp.groupBy("player_id", "time_class").agg(*aggs)
        .withColumn("band_id", band_id(F.round("mean_elo")))
    )
    return players.join(build_sessions(gp), ["player_id", "time_class"], "left")


def main() -> None:
    parser = build_arg_parser("Player-level features and sessions")
    args = parser.parse_args()
    cfg = load_config(args.config, args.months)
    setup_logging(STEP, cfg["paths"]["log_dir"])
    spark = get_spark(cfg, STEP)

    lake = cfg["paths"]["lake_dir"]
    gp = spark.read.parquet(f"{lake}/features/game_player").filter(F.col("source_month").isin(cfg["months"]))

    out_path = f"{lake}/features/player"
    with log_duration(log, "player features"):
        build_player(gp).write.mode("overwrite").parquet(out_path)
        out = spark.read.parquet(out_path)
        stats = out.agg(
            F.count("*").alias("rows"),
            F.countDistinct("player_id").alias("players"),
            F.sum("n_games").alias("games"),
            F.sum("n_sessions").alias("sessions"),
            F.sum("n_multi_game_sessions").alias("multi"),
        ).first()
        log.info("player features written: %d rows (player x time class), %d players, %d player-games, "
                 "%d sessions (%d with 2+ games)",
                 stats["rows"], stats["players"], stats["games"], stats["sessions"], stats["multi"])

    spark.stop()


if __name__ == "__main__":
    main()
