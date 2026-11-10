#!/bin/bash
# Download one Lichess monthly dump (.pgn.zst, about 30 GB) into data/raw/, with resume.
# Submit from the repo root (logs/ must exist first):
#     mkdir -p logs
#     sbatch cluster/download.sbatch 2026-10
# The file is downloaded as <name>.part; resubmitting after an interruption continues from where it
# stopped (curl -C -). Only when its SHA-256 matches database.lichess.org/standard/sha256sums.txt is
# it renamed to data/raw/lichess_db_standard_rated_<month>.pgn.zst, the path cluster/parse_chunked.sbatch reads.
# Download only, so 1 CPU; compute nodes have internet access, the login node is not for this.
#SBATCH --job-name=download
#SBATCH --partition=cpu
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=1-00:00:00
#SBATCH --output=logs/%x-%j.out

set -euo pipefail
MONTH="${1:?usage: sbatch cluster/download.sbatch <month, e.g. 2026-10>}"
BASE_URL="https://database.lichess.org/standard"
NAME="lichess_db_standard_rated_${MONTH}.pgn.zst"
DEST_DIR="data/raw"

cd "$SLURM_SUBMIT_DIR"  # the repo root
mkdir -p "$DEST_DIR"
DEST="$DEST_DIR/$NAME"
PART="$DEST.part"

echo "job=$SLURM_JOB_ID node=$(hostname) month=$MONTH start=$(date -u +%FT%TZ)"
EXPECTED="$(curl -fsS "$BASE_URL/sha256sums.txt" | awk -v n="$NAME" '$2 == n {print $1}')"
if [[ -z "$EXPECTED" ]]; then echo "$NAME is not in sha256sums.txt (month not published yet?)" >&2; exit 1; fi

if [[ -f "$DEST" ]]; then
    echo "$DEST already exists: checking it instead of downloading"
else
    REMOTE_BYTES="$(curl -fsSIL "$BASE_URL/$NAME" | awk 'tolower($1) == "content-length:" {n = $2} END {print n + 0}')"
    HAVE_BYTES="$( [[ -f "$PART" ]] && stat -c %s "$PART" || echo 0)"
    echo "remote size $REMOTE_BYTES bytes, already downloaded $HAVE_BYTES bytes"
    if (( HAVE_BYTES < REMOTE_BYTES )); then  # a complete .part would make curl -C - fail with HTTP 416
        t0=$SECONDS
        # -C -: continue from the size of the .part file; --retry: transient network errors
        curl -fL -C - --retry 20 --retry-delay 30 --retry-all-errors --no-progress-meter -o "$PART" "$BASE_URL/$NAME"
        echo "download took $((SECONDS - t0)) s"
    fi
fi

FILE="$DEST"; [[ -f "$DEST" ]] || FILE="$PART"
t0=$SECONDS
ACTUAL="$(sha256sum "$FILE" | cut -d' ' -f1)"
echo "sha256 took $((SECONDS - t0)) s: $ACTUAL"
if [[ "$ACTUAL" != "$EXPECTED" ]]; then
    echo "checksum mismatch (expected $EXPECTED): delete $FILE and resubmit" >&2
    exit 1
fi
[[ "$FILE" == "$PART" ]] && mv "$PART" "$DEST"
echo "ok $DEST $(du -h "$DEST" | cut -f1)  end=$(date -u +%FT%TZ)"
