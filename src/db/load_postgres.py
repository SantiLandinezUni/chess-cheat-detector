"""Build step 5: load core.games, core.moves and restricted.identity from the Parquet lake into PostgreSQL.

Run from the repo root with the venv active:
    PYTHONPATH=. spark-submit --driver-memory 3g --packages org.postgresql:postgresql:42.7.4 \
        src/db/load_postgres.py --config config/local.yaml

--packages must be on the command line: spark.jars.packages set in get_spark comes too late
once spark-submit has started the JVM.

Steps: apply sql/schema.sql and sql/generate_reference.sql, write each table with Spark JDBC into
an unlogged staging table, then replace the staged source_months in one transaction
(sql/load/*.sql). Rerunning a month replaces its rows, so counts never grow. Ends by reconciling
Parquet and PostgreSQL row counts; a mismatch fails the step.
"""

import logging
import sys
from pathlib import Path
from typing import Any

import psycopg2
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import REPO_ROOT, build_arg_parser, load_config
from src.common.logging_setup import log_duration, setup_logging
from src.common.spark import get_spark

STEP = "load_postgres"
SQL_DIR = REPO_ROOT / "sql"
JDBC_PARTITIONS = 4
# Columns of core.moves (data dictionary §4.2); the Parquet partition columns are not loaded
MOVES_COLUMNS = ["game_id", "ply", "color", "player_id", "san", "uci",
                 "clock_after_s", "move_time_s", "eval_cp", "eval_mate"]

log = logging.getLogger(STEP)


def connect(cfg: dict[str, Any]) -> psycopg2.extensions.connection:
    """psycopg2 connection from the config (user/password from .env)."""
    pg = cfg["postgres"]
    return psycopg2.connect(host=pg["host"], port=pg["port"], dbname=pg["dbname"],
                            user=pg["user"], password=pg["password"] or None)


def run_sql_file(conn: psycopg2.extensions.connection, path: Path, commit: bool = True) -> None:
    """Execute every statement in a .sql file."""
    with conn.cursor() as cur:
        cur.execute(path.read_text(encoding="utf-8"))
    if commit:
        conn.commit()


def write_staging(df: DataFrame, table: str, cfg: dict[str, Any]) -> None:
    """Append a DataFrame to an (empty) staging table with Spark JDBC."""
    pg = cfg["postgres"]
    (df.write.format("jdbc")
     .option("url", pg["jdbc_url"] + "?reWriteBatchedInserts=true")
     .option("driver", "org.postgresql.Driver")
     .option("dbtable", table)
     .option("user", pg["user"])
     .option("password", pg["password"])
     .option("batchsize", pg["jdbc_batch_size"])
     .option("numPartitions", JDBC_PARTITIONS)
     .mode("append")
     .save())


def pg_count(conn: psycopg2.extensions.connection, query: str, months: list[str]) -> int:
    with conn.cursor() as cur:
        cur.execute(query, {"months": months})
        return cur.fetchone()[0]


def reconcile(conn: psycopg2.extensions.connection, parquet_counts: dict[str, int], months: list[str],
              load_moves: bool) -> bool:
    """Compare Parquet and PostgreSQL row counts for the loaded months."""
    pg_counts = {
        "games": pg_count(conn, "SELECT count(*) FROM core.games WHERE source_month = ANY(%(months)s)", months),
        "moves": pg_count(conn, "SELECT count(*) FROM core.moves m JOIN core.games g USING (game_id) "
                                "WHERE g.source_month = ANY(%(months)s)", months),
        "identity": pg_count(conn, "SELECT count(*) FROM restricted.identity "
                                   "WHERE first_seen_month = ANY(%(months)s)", months),
    }
    ok = True
    for table, n_parquet in parquet_counts.items():
        if table == "moves" and not load_moves:
            log.info("reconcile moves: skipped (postgres.load_moves = false)")
            continue
        match = n_parquet == pg_counts[table]
        ok &= match
        (log.info if match else log.error)("reconcile %s: parquet=%d postgres=%d match=%s",
                                           table, n_parquet, pg_counts[table], match)
    return ok


def main() -> None:
    args = build_arg_parser("Load the Parquet lake into PostgreSQL").parse_args()
    cfg = load_config(args.config, args.months)
    setup_logging(STEP, cfg["paths"]["log_dir"])
    lake, months, load_moves = cfg["paths"]["lake_dir"], cfg["months"], cfg["postgres"]["load_moves"]
    log.info("months=%s lake=%s target=%s load_moves=%s", months, lake, cfg["postgres"]["jdbc_url"], load_moves)

    conn = connect(cfg)
    spark = get_spark(cfg, STEP, use_postgres=True)
    with log_duration(log, STEP):
        with log_duration(log, "schema and reference tables"):
            run_sql_file(conn, SQL_DIR / "schema.sql")
            run_sql_file(conn, SQL_DIR / "generate_reference.sql")
            run_sql_file(conn, SQL_DIR / "load" / "staging_create.sql")

        in_months = F.col("source_month").isin(months)
        games = spark.read.parquet(f"{lake}/games").filter(in_months)
        moves = spark.read.parquet(f"{lake}/moves").filter(in_months).select(*MOVES_COLUMNS)
        identity = spark.read.parquet(f"{lake}/restricted/identity").filter(F.col("first_seen_month").isin(months))
        parquet_counts = {"games": games.count(), "moves": moves.count(), "identity": identity.count()}
        log.info("parquet rows: %s", parquet_counts)

        with log_duration(log, "stage core.games"):
            write_staging(games, "core.stg_games", cfg)
        if load_moves:
            with log_duration(log, "stage core.moves"):
                write_staging(moves, "core.stg_moves", cfg)
        with log_duration(log, "stage restricted.identity"):
            write_staging(identity, "restricted.stg_identity", cfg)

        # One transaction: delete the staged months, insert games (and moves)
        with log_duration(log, "merge core.games + core.moves"):
            try:
                run_sql_file(conn, SQL_DIR / "load" / "merge_games.sql", commit=False)
                if load_moves:
                    run_sql_file(conn, SQL_DIR / "load" / "merge_moves.sql", commit=False)
                conn.commit()
            except Exception:
                conn.rollback()
                log.exception("merge failed, rolled back; PostgreSQL still has the previous load")
                raise
        with log_duration(log, "merge restricted.identity"):
            run_sql_file(conn, SQL_DIR / "load" / "merge_identity.sql")

        with log_duration(log, "drop staging and analyze"):
            run_sql_file(conn, SQL_DIR / "load" / "staging_drop.sql")

        ok = reconcile(conn, parquet_counts, months, load_moves)
    spark.stop()
    conn.close()

    if not ok:
        log.error("row counts do not match")
        sys.exit(1)
    log.info("all row counts match")


if __name__ == "__main__":
    main()
