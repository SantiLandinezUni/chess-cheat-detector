"""Privacy check on the serving schema (workstream E2), run before the database dump.

Fails (exit code 1) if any serving table or view has a column that could identify a player
or a game (username, game_id, ...), or if a player_id is not a 16-character hash.

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
        tables = {t for t, c in cols if c == "player_id"}
        for table in sorted(tables):
            bad = conn.execute(text(
                f"SELECT COUNT(*) FROM serving.{table} "
                "WHERE player_id IS NOT NULL AND player_id !~ '^[0-9a-f]{16}$'")).scalar()
            if bad:
                failures.append(f"serving.{table}: {bad} player_id values are not 16-hex hashes")
        print(f"Checked {len({t for t, _ in cols})} serving tables/views, {len(cols)} columns.")

    if failures:
        print("PRIVACY CHECK FAILED:\n  " + "\n  ".join(failures))
        sys.exit(1)
    print("Privacy check passed: no usernames or game_ids, all player_id values are hashes.")


if __name__ == "__main__":
    main()
