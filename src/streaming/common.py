"""Shared settings for the Kafka producers and consumers (Part 2, workstream E1).

The broker address is not fixed: cluster/kafka_job.sh starts a broker inside a Slurm job on free
localhost ports and exports KAFKA_BOOTSTRAP (and KAFKA_TOPIC) for the commands it runs.
"""

import os

TOPIC_MOVES = "lichess.moves"
TOPIC_SCORES = "lichess.scores"

# Value of a lichess.moves message (JSON, UTF-8); key = game_id. See docs/data_dictionary.md §8.
# `timestamp` must stay last: the replay producer appends it to a pre-encoded prefix at send time.
MOVE_FIELDS = ("game_id", "ply", "player_id", "color", "time_class", "elo",
               "clock", "move_time", "eval", "eval_mate", "base_seconds", "is_last", "timestamp")


def default_bootstrap() -> str | None:
    """Broker address from the environment (set by cluster/kafka_job.sh)."""
    return os.getenv("KAFKA_BOOTSTRAP")


def default_topic() -> str:
    """Topic from the environment, else lichess.moves."""
    return os.getenv("KAFKA_TOPIC", TOPIC_MOVES)
