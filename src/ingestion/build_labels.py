"""Build step 7a: player labels CSV (source S3) from the fetched Lichess profiles.

Run from the repo root with the venv active (after collect_labels.py):
    PYTHONPATH=. spark-submit src/ingestion/build_labels.py --config config/local.yaml

Runs sql/labels/player_labels.sql (restricted.identity joined to restricted.user_profiles_raw,
label rules of data dictionary §6) and streams the result with COPY into
data/processed/player_labels.csv: UTF-8, comma-separated, header row, player_id only, no usernames.
The file is replaced atomically, so a rerun gives the same file for the same profiles.
"""

import csv
import logging
import os
from collections import Counter
from pathlib import Path

from src.common.config import REPO_ROOT, build_arg_parser, load_config
from src.common.logging_setup import log_duration, setup_logging
from src.db.load_postgres import connect

STEP = "build_labels"
LABELS_SQL = REPO_ROOT / "sql" / "labels" / "player_labels.sql"
CSV_NAME = "player_labels.csv"
# core.player_labels columns, data dictionary §4.3
COLUMNS = ["player_id", "label", "tos_violation", "disabled", "found", "account_created_at", "label_fetched_at"]

log = logging.getLogger(STEP)


def main() -> None:
    args = build_arg_parser("Build data/processed/player_labels.csv from the fetched profiles").parse_args()
    cfg = load_config(args.config)
    setup_logging(STEP, cfg["paths"]["log_dir"])
    out_dir = Path(cfg["paths"]["processed_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / CSV_NAME
    tmp_path = out_path.with_suffix(".tmp")

    conn = connect(cfg)
    with log_duration(log, STEP):
        query = LABELS_SQL.read_text(encoding="utf-8").strip().rstrip(";")
        with conn.cursor() as cur, open(tmp_path, "w", encoding="utf-8", newline="") as f:
            cur.copy_expert(f"COPY ({query}) TO STDOUT WITH (FORMAT csv, HEADER true)", f)
            cur.execute("SELECT count(*) FROM restricted.identity")
            n_identity = cur.fetchone()[0]
        conn.close()

        # Check the file before replacing the previous one: header, row count, label distribution
        with open(tmp_path, encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames != COLUMNS:
                raise ValueError(f"unexpected CSV header {reader.fieldnames}, expected {COLUMNS}")
            labels = Counter(row["label"] for row in reader)
        n_rows = sum(labels.values())
        os.replace(tmp_path, out_path)

        log.info("wrote %s: rows=%d %s", out_path, n_rows, dict(sorted(labels.items())))
        missing = n_identity - n_rows
        if missing:
            log.warning("%d of %d players in restricted.identity have no fetched profile and no label; "
                        "rerun collect_labels.py", missing, n_identity)
        else:
            log.info("every player in restricted.identity (%d) has a label", n_identity)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("%s failed", STEP)  # traceback in the log file too, not only on stderr
        raise
