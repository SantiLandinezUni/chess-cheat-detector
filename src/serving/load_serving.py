"""Load the small result tables into the PostgreSQL ``serving`` schema (workstream E2).

Usage:
    python -m src.serving.load_serving --config config/serving.yaml

Privacy rules applied here:
  * the Lichess ``game_id`` never reaches the database: it is replaced by ``game_key``,
    a keyed hash (HMAC-SHA256 with LICHESS_HASH_KEY, first 12 hex characters);
  * any input that carries a ``username`` or ``game_id`` column after this step is refused.

Each table is replaced inside one transaction, so a rerun never duplicates rows.
"""
from __future__ import annotations

import argparse
import hashlib
import hmac
import io
import logging
import os
import sys
from pathlib import Path

import pandas as pd
import yaml
from sqlalchemy import create_engine, text

LOG = logging.getLogger("load_serving")
FORBIDDEN = {"username", "game_id", "white", "black", "site"}
SCHEMA_SQL = Path(__file__).resolve().parents[2] / "sql" / "serving_schema.sql"

LIVE_COLUMNS = [
    "run_id", "game_key", "player_id", "color", "time_class", "elo", "band_id", "n_plies",
    "n_moves", "n_timed_moves", "move_time_mean_s", "move_time_cv", "share_moves_under_2s",
    "share_low_clock", "n_low_clock_moves", "move_time_cv_low_clock", "z_move_time_cv",
    "z_share_moves_under_2s", "z_move_time_cv_low_clock", "n_score_features", "confidence",
    "risk_score", "end_reason", "rule_version", "baselines", "scored_at",
]


def hash_key() -> bytes:
    """Read LICHESS_HASH_KEY from the environment or the local .env file."""
    key = os.environ.get("LICHESS_HASH_KEY")
    if not key and Path(".env").exists():
        for line in Path(".env").read_text().splitlines():
            if line.startswith("LICHESS_HASH_KEY="):
                key = line.split("=", 1)[1].strip().strip('"').strip("'")
    if not key:
        sys.exit("LICHESS_HASH_KEY is not set (environment or .env).")
    return key.encode()


def game_key(game_id: str, key: bytes) -> str:
    return hmac.new(key, game_id.encode(), hashlib.sha256).hexdigest()[:12]


def read_any(path: Path) -> pd.DataFrame:
    if path.suffix == ".csv":
        return pd.read_csv(path)
    return pd.read_parquet(path)  # a file or a folder of parquet files


def check_privacy(df: pd.DataFrame, name: str) -> None:
    bad = FORBIDDEN & {c.lower() for c in df.columns}
    if bad:
        sys.exit(f"{name}: refused, it contains identifying columns {sorted(bad)}")
    if "player_id" in df.columns:
        lengths = df["player_id"].dropna().astype(str).str.len().unique()
        if set(lengths) - {16}:
            sys.exit(f"{name}: player_id values are not all 16-character hashes")


def copy_replace(conn, table: str, df: pd.DataFrame, where: str | None = None,
                 params: dict | None = None) -> None:
    """Delete the old rows and COPY the new ones, inside the caller's transaction."""
    conn.execute(text(f"DELETE FROM {table}" + (f" WHERE {where}" if where else "")), params or {})
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    raw = conn.connection.dbapi_connection.cursor()
    cols = ", ".join(df.columns)
    raw.copy_expert(f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)


def load_live_scores(engine, cfg: dict, key: bytes) -> int:
    path = Path(cfg["path"])
    if not path.exists():
        LOG.warning("live scores: %s not found, skipped", path)
        return 0
    df = read_any(path)
    df["game_key"] = df["game_id"].astype(str).map(lambda g: game_key(g, key))
    df = df.drop(columns=["game_id"])
    df["run_id"] = df["run_id"] if "run_id" in df.columns else cfg["run_id"]
    df = df[LIVE_COLUMNS]
    check_privacy(df, "live_scores")
    with engine.begin() as conn:
        copy_replace(conn, "serving.live_scores", df, "run_id = :r", {"r": cfg["run_id"]})
        n = conn.execute(text("SELECT COUNT(*) FROM serving.live_scores WHERE run_id = :r"),
                         {"r": cfg["run_id"]}).scalar()
    if n != len(df):
        sys.exit(f"live_scores: loaded {n} rows, expected {len(df)}")
    LOG.info("serving.live_scores: %d rows (run %s)", n, cfg["run_id"])
    return n


def load_optional(engine, name: str, path_str: str) -> int:
    path = Path(path_str)
    if not path.exists():
        LOG.warning("%s: %s not found, skipped", name, path)
        return 0
    df = read_any(path)
    check_privacy(df, name)
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS serving.{name}"))
        df.head(0).to_sql(name, conn, schema="serving", index=False)
        copy_replace(conn, f"serving.{name}", df)
    LOG.info("serving.%s: %d rows", name, len(df))
    return len(df)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/serving.yaml")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    cfg = yaml.safe_load(Path(args.config).read_text())
    url = os.environ.get("SERVING_DB_URL", cfg["postgres"]["url"])
    engine = create_engine(url)
    with engine.begin() as conn:
        conn.execute(text(SCHEMA_SQL.read_text()))

    key = hash_key()
    load_live_scores(engine, cfg["live_scores"], key)
    for name, path in (cfg.get("optional_tables") or {}).items():
        load_optional(engine, name, path)


if __name__ == "__main__":
    main()
