"""Load the small result tables into the PostgreSQL ``serving`` schema (workstream E2).

Usage:
    python -m src.serving.load_serving --config config/serving.yaml

Privacy rules applied here:
  * games are identified only by ``game_key`` (16 hex characters). The scorer export from the
    cluster already carries it. If a file still has the Lichess ``game_id``, it is hashed here
    (HMAC-SHA256 with LICHESS_HASH_KEY, first 16 hex characters) and dropped;
  * any input that carries a ``username`` or ``game_id`` column after this step is refused, and
    every player_id / game_key must be a 16-character hex hash.

Scorer tables (live_scores, running_scores) are partitioned by run_id. A run is replaced by a
partition swap: COPY into a staging table, check the row count, then detach + drop the old
partition and attach the new one, all in one transaction (FEEDBACK_ACTIONS 3.4). Input is read
in batches, so the full day (about 6M player-games) loads on a laptop.
"""
from __future__ import annotations

import argparse
import hashlib
import hmac
import io
import logging
import os
import re
import sys
from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds
import yaml
from sqlalchemy import create_engine, text

LOG = logging.getLogger("load_serving")
FORBIDDEN = {"username", "game_id", "white", "black", "site"}
HEX16 = re.compile(r"^[0-9a-f]{16}$")
SCHEMA_SQL = Path(__file__).resolve().parents[2] / "sql" / "serving_schema.sql"
BATCH_ROWS = 200_000

LIVE_COLUMNS = [
    "run_id", "game_key", "player_id", "color", "time_class", "elo", "band_id", "n_plies",
    "n_moves", "n_timed_moves", "move_time_mean_s", "move_time_cv", "share_moves_under_2s",
    "share_low_clock", "n_low_clock_moves", "move_time_cv_low_clock", "z_move_time_cv",
    "z_share_moves_under_2s", "z_move_time_cv_low_clock", "n_score_features", "confidence",
    "risk_score", "end_reason", "rule_version", "baselines", "scored_at",
]
# Nullable columns: filled with NULL (and a warning) if a scorer version does not export them.
LIVE_NULLABLE = {"elo", "band_id", "move_time_mean_s", "move_time_cv", "share_moves_under_2s",
                 "share_low_clock", "move_time_cv_low_clock", "z_move_time_cv",
                 "z_share_moves_under_2s", "z_move_time_cv_low_clock", "risk_score"}
RUNNING_COLUMNS = ["run_id", "game_key", "ply", "color", "player_id", "score"]
RUNNING_NULLABLE = {"color", "player_id"}
RUNNING_RENAME = {"risk_score": "score", "running_score": "score", "running_risk_score": "score"}


# ---------------------------------------------------------------- hashing and privacy
def hash_key() -> bytes | None:
    """LICHESS_HASH_KEY from the environment or .env; only needed if a file has game_id."""
    key = os.environ.get("LICHESS_HASH_KEY")
    if not key and Path(".env").exists():
        for line in Path(".env").read_text().splitlines():
            if line.startswith("LICHESS_HASH_KEY="):
                key = line.split("=", 1)[1].strip().strip('"').strip("'")
    return key.encode() if key else None


def game_key(game_id: str, key: bytes) -> str:
    return hmac.new(key, game_id.encode(), hashlib.sha256).hexdigest()[:16]


def add_game_key(df: pd.DataFrame, name: str) -> pd.DataFrame:
    """Use the pre-hashed game_key; hash game_id only if the file still has it."""
    if "game_id" in df.columns:
        if "game_key" not in df.columns:
            key = hash_key()
            if key is None:
                sys.exit(f"{name}: has game_id but LICHESS_HASH_KEY is not set")
            df["game_key"] = df["game_id"].astype(str).map(lambda g: game_key(g, key))
        df = df.drop(columns=["game_id"])
    if "game_key" not in df.columns:
        sys.exit(f"{name}: no game_key column")
    return df


def check_privacy(df: pd.DataFrame, name: str) -> None:
    bad = FORBIDDEN & {c.lower() for c in df.columns}
    if bad:
        sys.exit(f"{name}: refused, it contains identifying columns {sorted(bad)}")
    for col in ("player_id", "game_key"):
        if col in df.columns:
            vals = df[col].dropna().astype(str)
            if not vals.str.fullmatch(HEX16.pattern).all():
                sys.exit(f"{name}: {col} values are not all 16-character hex hashes")


# ---------------------------------------------------------------- reading
def dataset(path: Path) -> ds.Dataset:
    fmt = "csv" if path.suffix == ".csv" else "parquet"
    return ds.dataset(str(path), format=fmt, partitioning="hive")


def batches(path: Path):
    for batch in dataset(path).to_batches(batch_size=BATCH_ROWS):
        yield batch.to_pandas()


def read_any(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.suffix == ".csv" else pd.read_parquet(path)


def conform(df: pd.DataFrame, columns: list[str], nullable: set[str], run_id: str,
            name: str, warned: set[str]) -> pd.DataFrame:
    if "run_id" not in df.columns:
        df["run_id"] = run_id
    df["run_id"] = df["run_id"].astype(str)
    missing = [c for c in columns if c not in df.columns]
    hard = [c for c in missing if c not in nullable]
    if hard:
        sys.exit(f"{name}: missing required columns {hard}")
    for c in missing:
        if c not in warned:
            LOG.warning("%s: column %s not in the input, loaded as NULL", name, c)
            warned.add(c)
        df[c] = None
    return df[columns]


# ---------------------------------------------------------------- partition swap
def part_name(table: str, run_id: str) -> str:
    return f"{table}__{re.sub(r'[^a-z0-9]+', '_', run_id.lower()).strip('_')}"[:63]


def copy_df(conn, table: str, df: pd.DataFrame) -> None:
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    cur = conn.connection.dbapi_connection.cursor()
    cur.copy_expert(f"COPY {table} ({', '.join(df.columns)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)


def swap_partition(conn, table: str, run_id: str, staging: str) -> None:
    """Replace the run_id partition of serving.<table> with the staging table."""
    final = part_name(table, run_id)
    old = conn.execute(text(
        "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
        "JOIN pg_class p ON p.oid = i.inhparent JOIN pg_namespace n ON n.oid = p.relnamespace "
        "WHERE n.nspname = 'serving' AND p.relname = :t "
        "AND pg_get_expr(c.relpartbound, c.oid) = :bound"),
        {"t": table, "bound": f"FOR VALUES IN ('{run_id}')"}).scalar()
    if old:
        conn.execute(text(f"ALTER TABLE serving.{table} DETACH PARTITION serving.{old}"))
        conn.execute(text(f"DROP TABLE serving.{old}"))
    conn.execute(text(f"ALTER TABLE serving.{staging} RENAME TO {final}"))
    conn.execute(text(f"ALTER TABLE serving.{table} ATTACH PARTITION serving.{final} "
                      f"FOR VALUES IN ('{run_id}')"))
    LOG.info("serving.%s: partition %s %s", table, final, "replaced" if old else "attached")


def load_partitioned(engine, table: str, cfg: dict, columns: list[str], nullable: set[str],
                     rename: dict | None = None) -> dict[str, int]:
    path = Path(cfg["path"])
    if not path.exists():
        LOG.warning("%s: %s not found, skipped", table, path)
        return {}
    counts: dict[str, int] = {}
    warned: set[str] = set()
    with engine.begin() as conn:
        for df in batches(path):
            if rename:
                df = df.rename(columns={k: v for k, v in rename.items()
                                        if k in df.columns and v not in df.columns})
            df = add_game_key(df, table)
            df = conform(df, columns, nullable, cfg["run_id"], table, warned)
            check_privacy(df, table)
            for run_id, part in df.groupby("run_id", sort=False):
                if not re.fullmatch(r"[A-Za-z0-9._-]{1,64}", run_id):
                    sys.exit(f"{table}: run_id {run_id!r} has unexpected characters")
                staging = f"stage_{part_name(table, run_id)}"[:63]
                if run_id not in counts:
                    conn.execute(text(f"DROP TABLE IF EXISTS serving.{staging}"))
                    conn.execute(text(f"CREATE TABLE serving.{staging} (LIKE serving.{table} "
                                      "INCLUDING DEFAULTS INCLUDING CONSTRAINTS)"))
                    # Lets ATTACH skip the validation scan of the new partition.
                    conn.execute(text(f"ALTER TABLE serving.{staging} ADD CONSTRAINT "
                                      f"{staging[:50]}_run CHECK (run_id = '{run_id}')"))
                    counts[run_id] = 0
                copy_df(conn, f"serving.{staging}", part)
                counts[run_id] += len(part)
            LOG.info("%s: %s rows staged", table, f"{sum(counts.values()):,}")
        for run_id, n in counts.items():
            staging = f"stage_{part_name(table, run_id)}"[:63]
            got = conn.execute(text(f"SELECT COUNT(*) FROM serving.{staging}")).scalar()
            if got != n:
                sys.exit(f"{table}: staged {got} rows for {run_id}, expected {n}")  # rolls back
            swap_partition(conn, table, run_id, staging)
    for run_id, n in counts.items():
        LOG.info("serving.%s: %s rows (run %s)", table, f"{n:,}", run_id)
    return counts


# ---------------------------------------------------------------- small tables
def load_optional(engine, name: str, path_str: str) -> int:
    path = Path(path_str)
    if not path.exists():
        LOG.warning("%s: %s not found, skipped", name, path)
        return 0
    df = read_any(path)
    if "game_id" in df.columns or "game_key" in df.columns:
        df = add_game_key(df, name)
    check_privacy(df, name)
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS serving.{name}"))
        df.head(0).to_sql(name, conn, schema="serving", index=False)
        copy_df(conn, f"serving.{name}", df)
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

    load_partitioned(engine, "live_scores", cfg["live_scores"], LIVE_COLUMNS, LIVE_NULLABLE)
    if cfg.get("running_scores"):
        load_partitioned(engine, "running_scores", cfg["running_scores"], RUNNING_COLUMNS,
                         RUNNING_NULLABLE, RUNNING_RENAME)
    for name, path in (cfg.get("optional_tables") or {}).items():
        load_optional(engine, name, path)


if __name__ == "__main__":
    main()
