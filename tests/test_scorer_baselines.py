"""The scorer's baseline loader reads L10 (lake/baselines) and L12 (lake/baselines_provisional) alike.

Run from the repo root on the login node: PYTHONPATH=. pytest tests
"""

from datetime import datetime

import pandas as pd
import pytest

from src.streaming.scorer import IQR_TO_SD, MIN_CELL_GAMES, load_baselines


def row(level: str, band: int, tc: str, feature: str, n: int = 1000, p25: float = 0.4, p50: float = 0.6,
        p75: float = 0.8, sd: float = 0.3) -> dict:
    """One baseline row with the L10/L12 columns."""
    pct = {k: None for k in ("p01", "p05", "p10", "p90", "p95", "p99")}
    return {"level": level, "band_id": band, "time_class": tc, "feature": feature, "n": n, "mean": p50, "sd": sd,
            "p25": p25, "p50": p50, "p75": p75, **pct, "computed_at": datetime(2026, 11, 23)}


def write(tmp_path, rows: list[dict]) -> str:
    df = pd.DataFrame(rows).astype({"band_id": "int16", "n": "int64"})
    df.to_parquet(tmp_path / "part-00000.parquet", index=False)
    return str(tmp_path)


def test_l10_layout(tmp_path):
    """L10: level "player" rows, unscored features and correspondence cells are ignored."""
    path = write(tmp_path, [
        row("game", 5, "blitz", "move_time_cv"),
        row("game", 5, "blitz", "share_moves_under_2s", p25=0.1, p50=0.2, p75=0.1 + 1.349, sd=0.5),
        row("game", 5, "blitz", "move_time_cv_low_clock", p25=0.5, p75=0.5, sd=0.25),   # IQR 0 -> sd
        row("game", 5, "blitz", "acpl"),                                                # not scored
        row("player", 5, "blitz", "avg_move_time_cv"),                                  # player level
        row("game", 5, "correspondence", "move_time_cv"),                               # excluded
        row("game", 6, "blitz", "move_time_cv", n=MIN_CELL_GAMES - 1),                 # too few games
    ])
    b = load_baselines(path)
    assert set(b) == {(5, "blitz", "move_time_cv"), (5, "blitz", "share_moves_under_2s"),
                      (5, "blitz", "move_time_cv_low_clock")}
    assert b[(5, "blitz", "move_time_cv")] == (0.6, (0.8 - 0.4) / IQR_TO_SD)
    assert b[(5, "blitz", "share_moves_under_2s")][1] == pytest.approx(1.0)
    assert b[(5, "blitz", "move_time_cv_low_clock")] == (0.6, 0.25)


def test_l12_layout(tmp_path):
    """L12: the level "move" rows are ignored."""
    path = write(tmp_path, [row("game", 3, "bullet", "move_time_cv"), row("move", 3, "bullet", "move_time_s")])
    assert set(load_baselines(path)) == {(3, "bullet", "move_time_cv")}
