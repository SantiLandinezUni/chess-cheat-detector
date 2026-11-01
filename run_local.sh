#!/usr/bin/env bash
# Run the whole Part 1 pipeline on the local sample, starting from an empty database.
#
#   ./run_local.sh                      # uses config/local.yaml
#   ./run_local.sh path/to/config.yaml
#
# The terminal shows one line per step; the full output (Spark included) goes to
# logs/run_local_<UTC time>.log. Steps whose file isn't merged yet are skipped.
# WARNING: the first step drops the core, ref and restricted schemas, including the
# cached Lichess API responses in restricted.user_profiles_raw.

# -e: stop at the first failing command, -u: error on unset variables,
# -o pipefail: a pipeline fails if any command in it fails,
# -E: run the error handler (trap) for failures inside functions too
set -Eeuo pipefail

cd "$(dirname "$0")"  # always run from the repo root
CONFIG="${1:-config/local.yaml}"
mkdir -p logs
LOG="logs/run_local_$(date -u +%Y%m%dT%H%M%SZ).log"
touch "$LOG"
CURRENT_STEP="setup"
SUMMARY=""

# On any error: say which step failed and show the end of its output
on_error() {
    echo
    echo "FAILED at step: $CURRENT_STEP"
    echo "---- last 30 lines of $LOG ----"
    tail -n 30 "$LOG"
    echo "---- full output: $LOG ----"
    exit 1
}
trap on_error ERR

# ---------- environment ----------
if [[ ! -f .venv/bin/activate ]]; then echo "No .venv found: see SETUP.md step 3" >&2; exit 1; fi
if [[ ! -f .env ]]; then echo "No .env found: see SETUP.md step 4" >&2; exit 1; fi
if [[ ! -f "$CONFIG" ]]; then echo "Config not found: $CONFIG" >&2; exit 1; fi
# shellcheck disable=SC1091
source .venv/bin/activate
export PYTHONPATH=.

# Same settings for every Python step (see SETUP.md, "Running a pipeline step")
SPARK_SUBMIT=(spark-submit --driver-memory 3g --packages org.postgresql:postgresql:42.7.4)

# PostgreSQL connection for psql, read from the config and .env (psql reads these PG* variables)
IFS=$'\t' read -r PGHOST PGPORT PGDATABASE PGUSER PGPASSWORD < <(python -c "
from src.common.config import load_config
pg = load_config('$CONFIG')['postgres']
print('\t'.join(str(pg[k]) for k in ('host', 'port', 'dbname', 'user', 'password')))") || true
export PGHOST PGPORT PGDATABASE PGUSER PGPASSWORD
if [[ -z "${PGDATABASE:-}" ]]; then echo "Could not read the postgres settings from $CONFIG" >&2; exit 1; fi
PSQL=(psql -X -q -v ON_ERROR_STOP=1)

# ---------- helpers ----------
# run_step "<name>" <file that must exist> <command...>
run_step() {
    local name="$1" file="$2"
    shift 2
    CURRENT_STEP="$name"
    if [[ ! -e "$file" ]]; then
        printf '%-28s SKIPPED (not merged yet)\n' "$name"
        SUMMARY+=$(printf '%-28s skipped' "$name")$'\n'
        return 0
    fi
    printf '%-28s running... ' "$name"
    local start=$SECONDS
    echo "===== $name: $* =====" >> "$LOG"
    "$@" >> "$LOG" 2>&1
    local took=$((SECONDS - start))
    printf 'ok (%ds)\n' "$took"
    SUMMARY+=$(printf '%-28s %5ds' "$name" "$took")$'\n'
}

# Run every .sql file in a folder, in name order
run_sql_dir() {
    local f
    for f in "$1"/*.sql; do
        [[ -e "$f" ]] || continue  # folder exists but has no .sql files yet
        echo "-- $f" >> "$LOG"
        "${PSQL[@]}" -f "$f"
    done
}

# ---------- pipeline ----------
echo "run_local.sh  config=$CONFIG  database=$PGDATABASE  log=$LOG"
echo

run_step "reset database"            sql/drop_schemas.sql           "${PSQL[@]}" -f sql/drop_schemas.sql
run_step "schema"                    sql/schema.sql                 "${PSQL[@]}" -f sql/schema.sql
run_step "reference tables"          sql/generate_reference.sql     "${PSQL[@]}" -f sql/generate_reference.sql
run_step "parse games (step 3)"      src/ingestion/parse_games.py   "${SPARK_SUBMIT[@]}" src/ingestion/parse_games.py --config "$CONFIG"
run_step "quality checks (step 4)"   src/quality/checks.py          "${SPARK_SUBMIT[@]}" src/quality/checks.py --config "$CONFIG"
run_step "load postgres (step 5)"    src/db/load_postgres.py        "${SPARK_SUBMIT[@]}" src/db/load_postgres.py --config "$CONFIG"
run_step "collect labels (step 6)"   src/ingestion/collect_labels.py "${SPARK_SUBMIT[@]}" src/ingestion/collect_labels.py --config "$CONFIG"
run_step "build labels (step 7)"     src/ingestion/build_labels.py  "${SPARK_SUBMIT[@]}" src/ingestion/build_labels.py --config "$CONFIG"
run_step "load labels (step 7)"      src/db/load_labels.py          "${SPARK_SUBMIT[@]}" src/db/load_labels.py --config "$CONFIG"
run_step "derived tables (step 8)"   sql/generate_derived.sql       "${PSQL[@]}" -f sql/generate_derived.sql
run_step "validation queries (step 9)" sql/validation               run_sql_dir sql/validation

CURRENT_STEP="done"
echo
echo "Step runtimes:"
printf '%s' "$SUMMARY"
total=$SECONDS
printf 'TOTAL %dm %02ds  (full output: %s)\n' $((total / 60)) $((total % 60)) "$LOG"
