"""Offset commits and partition handoff of the streaming scorer (fix for job 32798), without a broker.

A partition handed over at any offset, to one or two later owners, must give the same final scores as one
scorer reading it all: every game scored exactly once, and no move read twice (the open games' state moves
with the partition as snapshots, which go through JSON here as they do through lichess.scorer_state).

Run from the repo root on the login node: PYTHONPATH=. pytest tests
"""

import json
import random
from typing import Any

import pytest

from src.streaming.scorer import SCORED, Scorer, parse_state_metadata, state_metadata

BASELINES = {(b, tc, f): (0.5, 0.2) for b in range(1, 13) for tc in ("blitz", "bullet") for f in SCORED}
VOLATILE = {"scored_at"}


class FakeProducer:
    """Stands in for confluent_kafka.Producer: acknowledges every score at once."""

    def __init__(self) -> None:
        self.sent: list[bytes] = []

    def produce(self, _topic: str, key: bytes, value: bytes, timestamp: int, on_delivery: Any) -> None:
        self.sent.append(value)
        on_delivery(None, None)

    def poll(self, _t: float) -> int:
        return 0

    def flush(self, _t: float = 0) -> int:
        return 0


def interleaved_moves(n_games: int = 40, seed: int = 7) -> list[dict[str, Any]]:
    """The moves of n_games games on one partition, interleaved as a paced replay sends them."""
    rng = random.Random(seed)
    games = []
    for i in range(n_games):
        n_plies = rng.randint(1, 70)
        tc = "blitz" if i % 2 else "bullet"
        games.append([{"game_id": f"g{i:03d}", "ply": ply, "player_id": f"p{i}{ply % 2}",
                       "color": "w" if ply % 2 else "b", "time_class": tc, "elo": 1500 + 10 * i,
                       "clock": max(0.0, 180 - ply * rng.uniform(0.5, 3)),
                       "move_time": rng.choice([None, -0.1, rng.uniform(0, 8), rng.uniform(0, 2)]),
                       "eval": None, "eval_mate": None, "base_seconds": 180, "is_last": ply == n_plies,
                       "timestamp": 1_700_000_000_000 + ply} for ply in range(1, n_plies + 1)])
    out = []
    while any(games):  # the next move of a random game still running: games overlap in time
        out.append(rng.choice([g for g in games if g]).pop(0))
    return out


def new_scorer() -> Scorer:
    return Scorer(BASELINES, "test", 10, FakeProducer(), "lichess.scores")


def feed(s: Scorer, moves: list[dict[str, Any]], start: int, end: int, partition: int = 0) -> None:
    for offset in range(start, end):
        s.on_move(moves[offset], partition, offset, now=0.0)


def via_kafka(snapshots: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Snapshots as the next owner reads them from lichess.scorer_state (JSON round trip)."""
    return [json.loads(json.dumps(st, separators=(",", ":"))) for st in snapshots]


def rows(*scorers: Scorer) -> list[tuple]:
    """Final rows without the volatile fields, as sorted tuples (duplicates kept)."""
    return sorted(tuple(sorted((k, v) for k, v in r.items() if k not in VOLATILE))
                  for s in scorers for r in s.final_rows)


def scores(*scorers: Scorer) -> list[dict[str, Any]]:
    """Every lichess.scores message without its emit time, sorted."""
    msgs = [json.loads(v) for s in scorers for v in s.producer.sent]
    for m in msgs:
        m.pop("timestamp")
    return sorted(msgs, key=lambda m: json.dumps(m, sort_keys=True))


@pytest.fixture(scope="module")
def moves() -> list[dict[str, Any]]:
    return interleaved_moves()


@pytest.fixture(scope="module")
def reference(moves) -> tuple[list[tuple], list[dict[str, Any]]]:
    s = new_scorer()
    s.assign(0)
    feed(s, moves, 0, len(moves))
    assert not s.games and s.final_games()["last_move"] == 40
    return rows(s), scores(s)


def test_commits_the_position_after_each_batch(moves):
    s = new_scorer()
    s.assign(0)
    feed(s, moves, 0, 200)
    assert s.due_commits() == [(0, 200)]
    assert s.due_commits() == []  # nothing new since the last commit
    feed(s, moves, 200, 250)
    assert s.due_commits() == [(0, 250)]


@pytest.mark.parametrize("cut", [0, 1, 37, 150, 400, 777, 1100, -1])
def test_one_handoff_scores_every_game_once(moves, reference, cut):
    cut = cut if cut >= 0 else len(moves) - 1
    a, b = new_scorer(), new_scorer()
    a.assign(0)
    feed(a, moves, 0, cut)
    open_at_cut = len(a.games)
    points, snaps = a.release([0])
    assert points == ([(0, cut)] if cut else []) and len(snaps[0]) == open_at_cut
    assert not a.games and 0 not in a.owned

    b.assign(0, cut if cut else None, via_kafka(snaps[0]))
    feed(b, moves, cut, len(moves))  # continues at the committed position: nothing is read twice
    assert rows(a, b) == reference[0]
    assert scores(a, b) == reference[1]  # also every running score in lichess.scores, once
    assert b.counts["games_restored"] == open_at_cut and b.counts["games_joined_late"] == 0


def test_two_handoffs(moves, reference):
    """A -> B -> C, and C gets the partition back from B before B processed anything new."""
    a, b, c = new_scorer(), new_scorer(), new_scorer()
    a.assign(0)
    feed(a, moves, 0, 900)
    [(_, pos_a)], snaps = a.release([0])
    b.assign(0, pos_a, via_kafka(snaps[0]))
    [(_, pos_b)], snaps = b.release([0])  # nothing processed: B passes A's games on unchanged
    assert pos_b == pos_a
    c.assign(0, pos_b, via_kafka(snaps[0]))
    feed(c, moves, pos_b, 1000)
    [(_, pos_c)], snaps = c.release([0])
    a.assign(0, pos_c, via_kafka(snaps[0]))
    feed(a, moves, pos_c, len(moves))
    assert rows(a, b, c) == reference[0]


def test_final_stop_commits_the_position(moves):
    """Group stop: open games get their final score ("shutdown"), nothing is handed over."""
    s = new_scorer()
    s.assign(0)
    feed(s, moves, 0, 500)
    s.finish_all("shutdown")
    points, snaps = s.release([0])
    assert points == [(0, 500)] and snaps == {0: []}


def test_state_metadata():
    assert parse_state_metadata(state_metadata(17, 42)) == (17, 42)
    assert parse_state_metadata("") is None and parse_state_metadata(None) is None
