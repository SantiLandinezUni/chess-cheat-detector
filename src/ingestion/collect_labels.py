"""Build step 6: fetch Lichess profiles for every player in restricted.identity (label source S2).

Run from the repo root with the venv active:
    PYTHONPATH=. spark-submit src/ingestion/collect_labels.py --config config/local.yaml

Test mode, before restricted.identity is loaded (NDJSON files only, no database):
    PYTHONPATH=. python src/ingestion/collect_labels.py --config config/local.yaml \
        --usernames user1,user2 --no-db

Calls POST /api/users with up to lichess.batch_size usernames per request, one request at a time.
HTTP 429 (and 5xx / connection errors): wait lichess.retry_wait_s, retry, at most lichess.max_tries
tries. Each batch is saved in data/raw/profiles/ as
    profiles_<fetched_at>_b<batch_id>.ndjson   raw API objects, one per line
    notfound_<fetched_at>_b<batch_id>.ndjson   {"username": ...} per name the API didn't return
and upserted into restricted.user_profiles_raw in its own transaction, so an interrupted run
resumes where it stopped. Usernames already in the table are skipped unless --refresh.

The NDJSON files are the durable record: run_local.sh drops the table on every clean run, so when
it is empty it is first reloaded from the files (newest fetch wins). Only names missing from the
files go to the API, which keeps labels and label_fetched_at identical between runs.
Usernames are never logged (CLAUDE.md, Privacy rules).
"""

import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
import requests
from psycopg2.extras import Json, execute_values

from src.common.config import REPO_ROOT, build_arg_parser, load_config
from src.common.logging_setup import log_duration, setup_logging
from src.db.load_postgres import connect

STEP = "collect_labels"
UPSERT_SQL = REPO_ROOT / "sql" / "load" / "upsert_user_profiles.sql"
REQUEST_TIMEOUT_S = 60
STAMP_FORMAT = "%Y%m%dT%H%M%SZ"
BATCH_FILE_RE = re.compile(r"^(profiles|notfound)_(\d{8}T\d{6}Z)_b(\d+)\.ndjson$")

log = logging.getLogger(STEP)


def usernames_from_identity(conn: psycopg2.extensions.connection) -> list[str]:
    """All usernames in restricted.identity, lowercase, sorted (stable batches across reruns)."""
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT lower(username) FROM restricted.identity ORDER BY 1")
        return [r[0] for r in cur.fetchall()]


def cached_usernames(conn: psycopg2.extensions.connection) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT username FROM restricted.user_profiles_raw")
        return {r[0] for r in cur.fetchall()}


def next_batch_id(conn: psycopg2.extensions.connection | None) -> int:
    """batch_id continues after the highest one stored, so it stays unique across runs."""
    if conn is None:
        return 1
    with conn.cursor() as cur:
        cur.execute("SELECT coalesce(max(batch_id), 0) + 1 FROM restricted.user_profiles_raw")
        return cur.fetchone()[0]


def read_ndjson(path: Path) -> list[dict[str, Any]]:
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def reload_from_ndjson(conn: psycopg2.extensions.connection, profiles_dir: Path) -> tuple[int, int]:
    """Refill an empty restricted.user_profiles_raw from the saved batch files, in one transaction.

    Files are replayed oldest first (fetched_at, batch_id), so a later --refresh overwrites an
    earlier fetch of the same name. Returns (files read, rows in the table afterwards).
    """
    batches = []
    for path in profiles_dir.glob("*.ndjson"):
        m = BATCH_FILE_RE.match(path.name)
        if not m:
            log.warning("reload: skipping file with unexpected name: %s", path.name)
            continue
        fetched_at = datetime.strptime(m.group(2), STAMP_FORMAT).replace(tzinfo=timezone.utc)
        batches.append((fetched_at, int(m.group(3)), m.group(1), path))
    batches.sort(key=lambda b: (b[0], b[1]))

    sql = UPSERT_SQL.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        for fetched_at, batch_id, kind, path in batches:
            if kind == "profiles":
                rows = [(p["id"], True, Json(p), fetched_at, batch_id) for p in read_ndjson(path)]
            else:
                rows = [(r["username"], False, None, fetched_at, batch_id) for r in read_ndjson(path)]
            if rows:
                execute_values(cur, sql, rows)
        cur.execute("SELECT count(*) FROM restricted.user_profiles_raw")
        n_rows = cur.fetchone()[0]
    conn.commit()
    return len(batches), n_rows


def fetch_batch(session: requests.Session, names: list[str], lichess: dict[str, Any]) -> list[dict[str, Any]]:
    """POST one batch to /api/users; retry on 429, 5xx and connection errors. Raises after max_tries."""
    max_tries, wait_s = lichess["max_tries"], lichess["retry_wait_s"]
    for attempt in range(1, max_tries + 1):
        try:
            resp = session.post(lichess["users_url"], data=",".join(names).encode("utf-8"),
                                headers={"Content-Type": "text/plain"}, timeout=REQUEST_TIMEOUT_S)
            if resp.status_code == 200:
                return resp.json()
            reason = f"HTTP {resp.status_code}"
            if resp.status_code != 429 and resp.status_code < 500:
                resp.raise_for_status()  # 4xx other than 429: retrying won't help
        except requests.ConnectionError as e:
            reason = f"connection error: {type(e).__name__}"
        except requests.Timeout:
            reason = "timeout"
        if attempt < max_tries:
            log.warning("%s (try %d/%d), waiting %d s", reason, attempt, max_tries, wait_s)
            time.sleep(wait_s)
    raise RuntimeError(f"giving up after {max_tries} tries: {reason}")


def write_ndjson(path: Path, records: list[dict[str, Any]]) -> None:
    """One JSON object per line; written to a temp name first so a crash leaves no partial file."""
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    tmp.rename(path)


def save_batch(out_dir: Path, batch_id: int, fetched_at: datetime, profiles: list[dict[str, Any]],
               missing: list[str]) -> None:
    """Write the batch files; the file name carries fetched_at and batch_id for reloading."""
    suffix = f"{fetched_at.strftime(STAMP_FORMAT)}_b{batch_id:05d}.ndjson"
    write_ndjson(out_dir / f"profiles_{suffix}", profiles)
    if missing:
        write_ndjson(out_dir / f"notfound_{suffix}", [{"username": n} for n in missing])


def upsert_profiles(conn: psycopg2.extensions.connection, names: list[str],
                    by_id: dict[str, dict[str, Any]], fetched_at: datetime, batch_id: int) -> None:
    """One row per queried username; found = false (response null) when the API didn't return it."""
    rows = [(n, n in by_id, Json(by_id[n]) if n in by_id else None, fetched_at, batch_id) for n in names]
    with conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL.read_text(encoding="utf-8"), rows)
    conn.commit()


def main() -> None:
    parser = build_arg_parser("Fetch Lichess user profiles (label source)")
    parser.add_argument("--usernames", help="comma-separated usernames to fetch instead of restricted.identity")
    parser.add_argument("--refresh", action="store_true", help="refetch usernames already fetched")
    parser.add_argument("--no-db", action="store_true", help="write NDJSON only (no cache, no PostgreSQL)")
    args = parser.parse_args()
    if args.no_db and not args.usernames:
        parser.error("--no-db needs --usernames (the default list comes from restricted.identity)")

    cfg = load_config(args.config, args.months)
    setup_logging(STEP, cfg["paths"]["log_dir"])
    lichess = cfg["lichess"]
    out_dir = Path(cfg["paths"]["profiles_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = None if args.no_db else connect(cfg)
    with log_duration(log, STEP):
        if conn is not None and not cached_usernames(conn):
            with log_duration(log, "reload restricted.user_profiles_raw from NDJSON"):
                n_files, n_rows = reload_from_ndjson(conn, out_dir)
            log.info("reloaded %d rows from %d files in %s", n_rows, n_files, out_dir)

        if args.usernames:
            requested = sorted({u.strip().lower() for u in args.usernames.split(",") if u.strip()})
            source = "--usernames"
        else:
            requested = usernames_from_identity(conn)
            source = "restricted.identity"
        cached = set() if (conn is None or args.refresh) else cached_usernames(conn)
        todo = [u for u in requested if u not in cached]
        log.info("source=%s requested=%d cached_skipped=%d to_fetch=%d refresh=%s db=%s",
                 source, len(requested), len(requested) - len(todo), len(todo), args.refresh, conn is not None)

        batch_size = lichess["batch_size"]
        batches = [todo[i:i + batch_size] for i in range(0, len(todo), batch_size)]
        first_id = next_batch_id(conn)
        n_found = n_missing = n_tos = n_disabled = 0
        failed: list[int] = []

        session = requests.Session()
        session.headers["User-Agent"] = lichess["user_agent"]
        for i, names in enumerate(batches):
            batch_id = first_id + i
            # Whole seconds: the value is stored in the file name and must reload identically
            fetched_at = datetime.now(timezone.utc).replace(microsecond=0)
            try:
                profiles = fetch_batch(session, names, lichess)
            except Exception as e:  # one bad batch doesn't stop the run; it isn't saved, so a rerun retries it
                log.error("batch %d (%d names) failed: %s", batch_id, len(names), e)
                failed.append(batch_id)
                continue

            by_id = {p["id"]: p for p in profiles if p.get("id") in names}
            missing = [n for n in names if n not in by_id]
            save_batch(out_dir, batch_id, fetched_at, list(by_id.values()), missing)
            if conn is not None:
                upsert_profiles(conn, names, by_id, fetched_at, batch_id)

            found = len(by_id)
            tos = sum(1 for p in by_id.values() if p.get("tosViolation"))
            disabled = sum(1 for p in by_id.values() if p.get("disabled"))
            n_found, n_missing, n_tos, n_disabled = (n_found + found, n_missing + len(missing),
                                                     n_tos + tos, n_disabled + disabled)
            log.info("batch %d/%d (id %d): queried=%d found=%d not_found=%d tos_violation=%d disabled=%d",
                     i + 1, len(batches), batch_id, len(names), found, len(missing), tos, disabled)

        log.info("totals: fetched=%d found=%d not_found=%d tos_violation=%d disabled=%d failed_batches=%d",
                 n_found + n_missing, n_found, n_missing, n_tos, n_disabled, len(failed))
    if conn is not None:
        conn.close()

    if failed:
        log.error("%d batch(es) failed: %s; rerun to fetch them (saved batches are skipped)", len(failed), failed)
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("%s failed", STEP)  # traceback in the log file too, not only on stderr
        raise
