"""Build step 7b: load data/processed/player_labels.csv into core.player_labels with COPY.

Run from the repo root with the venv active (after build_labels.py):
    PYTHONPATH=. spark-submit src/db/load_labels.py --config config/local.yaml

The CSV is the complete label set, so the table is replaced in one transaction (DELETE + COPY):
reruns never duplicate rows, and a bad file rolls back to the previous labels. Ends by comparing
the CSV and table row counts; a mismatch fails the step.
"""

import logging
import sys
from pathlib import Path

from src.common.config import build_arg_parser, load_config
from src.common.logging_setup import log_duration, setup_logging
from src.db.load_postgres import connect
from src.ingestion.build_labels import COLUMNS, CSV_NAME

STEP = "load_labels"

log = logging.getLogger(STEP)


def main() -> None:
    args = build_arg_parser("Load player_labels.csv into core.player_labels").parse_args()
    cfg = load_config(args.config, args.months)
    setup_logging(STEP, cfg["paths"]["log_dir"])
    csv_path = Path(cfg["paths"]["processed_dir"]) / CSV_NAME
    with open(csv_path, encoding="utf-8") as f:
        n_csv = sum(1 for _ in f) - 1  # minus the header; no field contains a newline
    log.info("input %s: %d rows", csv_path, n_csv)

    conn = connect(cfg)
    with log_duration(log, STEP):
        try:
            with conn.cursor() as cur, open(csv_path, encoding="utf-8") as f:
                cur.execute("DELETE FROM core.player_labels")
                cur.copy_expert(f"COPY core.player_labels ({', '.join(COLUMNS)}) "
                                "FROM STDIN WITH (FORMAT csv, HEADER true)", f)
            conn.commit()
        except Exception:
            conn.rollback()
            log.exception("load failed, rolled back; core.player_labels still has the previous load")
            raise

        with conn.cursor() as cur:
            cur.execute("ANALYZE core.player_labels")
            cur.execute("SELECT label, count(*) FROM core.player_labels GROUP BY label ORDER BY label")
            dist = dict(cur.fetchall())
        conn.commit()
    conn.close()

    n_table = sum(dist.values())
    log.info("core.player_labels rows=%d %s", n_table, dist)
    if n_table != n_csv:
        log.error("row counts do not match: csv=%d table=%d", n_csv, n_table)
        sys.exit(1)
    log.info("row counts match")


if __name__ == "__main__":
    main()
