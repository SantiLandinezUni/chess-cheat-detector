"""Metrics of src/streaming/compare_scores.py on a hand-made pair of runs.

Run from the repo root on the login node: PYTHONPATH=. pytest tests
"""

import pandas as pd
import pytest

from src.streaming.compare_scores import KEY, compare, game_level


def run(scores: list[float | None]) -> pd.DataFrame:
    """Two players per game, scores in (game, colour) order."""
    return pd.DataFrame({"game_id": [f"g{i // 2}" for i in range(len(scores))],
                         "player_id": [f"p{i}" for i in range(len(scores))],
                         "time_class": "blitz", "risk_score": scores})


def test_identical_runs():
    a = run([float(i) / 100 for i in range(200)])
    r = compare(a, a.copy(), KEY, 1.0)
    assert r["pearson_r"] == pytest.approx(1.0) and r["spearman_r"] == pytest.approx(1.0)
    assert r["tier_changes"] == 0 and r["max_abs_diff"] == 0
    assert r["top1pct_k"] == 2 and r["top1pct_overlap"] == 2


def test_tiers_and_top():
    a = run([0.5, 1.2, None, 0.1] + [0.0] * 196)
    b = run([1.1, 0.9, 0.3, 0.1] + [0.0] * 196)
    r = compare(a, b, KEY, 1.0)
    assert (r["normal_to_high"], r["high_to_normal"], r["unscored_changes"], r["tier_changes"]) == (1, 1, 1, 3)
    assert (r["high_a"], r["high_b"]) == (1, 1)
    assert r["scored_only_b"] == 1 and r["scored_both"] == 199
    assert r["top1pct_k"] == 2 and r["top1pct_overlap"] == 2   # {p1, p0} in both runs


def test_game_level_is_max_of_players():
    s = game_level(run([0.2, 0.7, None, None])).set_index("game_id")["risk_score"]
    assert s["g0"] == 0.7 and pd.isna(s["g1"])
