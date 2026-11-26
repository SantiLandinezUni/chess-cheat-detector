"""Privacy check on the serving schema (workstream E2), run before the database dump.

Fails (exit code 1) if any serving table or view has a column that could identify a player
or a game (username, game_id, ...), or if a player_id or game_key is not a 16-character hex hash.

Usage:
    python -m src.serving.privacy_check --config config/serving.yaml
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import yaml
from sqlalchemy import create_engine, text

FORBIDDEN = ("username", "game_id", "white", "black", "site", "email", "name")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/serving.yaml")
    args = parser.parse_args()
    cfg = yaml.safe_load(Path(args.config).read_text())
    engine = create_engine(os.environ.get("SERVING_DB_URL", cfg["postgres"]["url"]))

    failures = []
    with engine.connect() as conn:
        cols = conn.execute(text(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE table_schema = 'serving' ORDER BY table_name, ordinal_position")).all()
        for table, column in cols:
            if column.lower() in FORBIDDEN or "username" in column.lower():
                failures.append(f"serving.{table}.{column}: identifying column")
        for key in ("player_id", "game_key"):
            for table in sorted({t for t, c in cols if c == key}):
                bad = conn.execute(text(
                    f"SELECT COUNT(*) FROM serving.{table} "
                    f"WHERE {key} IS NOT NULL AND {key} !~ '^[0-9a-f]{{16}}$'")).scalar()
                if bad:
                    failures.append(f"serving.{table}: {bad} {key} values are not 16-hex hashes")
        print(f"Checked {len({t for t, _ in cols})} serving tables/views, {len(cols)} columns.")

    if failures:
        print("PRIVACY CHECK FAILED:\n  " + "\n  ".join(failures))
        sys.exit(1)
    print("Privacy check passed: no usernames or game_ids, all player_id and game_key values are hashes.")


if __name__ == "__main__":
    main()
