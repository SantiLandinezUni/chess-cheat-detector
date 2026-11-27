#!/usr/bin/env bash
# Database deliverable for Part 2 (workstream E2): privacy check, pg_dump of the serving schema,
# then a restore into a scratch database to prove the dump is complete.
# The dump goes to deliverables/ (git-ignored), never into a tracked folder.
#
# Usage: scripts/dump_serving.sh [database]   (default: chess; uses the usual PG* env variables)
set -euo pipefail

DB="${1:-chess}"
OUT_DIR="deliverables"
DUMP="$OUT_DIR/serving_$(date -u +%Y%m%d).dump"
CHECK_DB="${DB}_restore_check"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"; dropdb --if-exists "$CHECK_DB" 2>/dev/null || true' EXIT
mkdir -p "$OUT_DIR" docs/validation_results

echo "1/3 Privacy check"
python -m src.serving.privacy_check --config config/serving.yaml

echo "2/3 Dump the serving schema to $DUMP"
pg_dump --format=custom --no-owner --schema=serving "$DB" > "$DUMP"

echo "3/3 Restore check into $CHECK_DB"
dropdb --if-exists "$CHECK_DB"
createdb "$CHECK_DB"
pg_restore --no-owner --dbname="$CHECK_DB" "$DUMP"

QUERY="SELECT 'live_scores', COUNT(*) FROM serving.live_scores
       UNION ALL SELECT 'running_scores', COUNT(*) FROM serving.running_scores
       UNION ALL SELECT 'rating_bands', COUNT(*) FROM serving.rating_bands"
psql -At -F, -d "$DB" -c "$QUERY" | sort > "$TMP_DIR/counts_src.csv"
psql -At -F, -d "$CHECK_DB" -c "$QUERY" | sort > "$TMP_DIR/counts_restored.csv"

if diff -q "$TMP_DIR/counts_src.csv" "$TMP_DIR/counts_restored.csv" > /dev/null; then
    { echo "table,rows"; cat "$TMP_DIR/counts_restored.csv"; } > docs/validation_results/serving_restore_check.csv
    echo "Restore check passed. Row counts in docs/validation_results/serving_restore_check.csv"
else
    echo "Restore check FAILED: row counts differ"; diff "$TMP_DIR/counts_src.csv" "$TMP_DIR/counts_restored.csv"
    exit 1
fi
