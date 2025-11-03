#!/usr/bin/env bash
set -euo pipefail

# Resume-aware, parallel runner for plate tile CSVs.
# - Tracks completed tiles in a .done.d/ directory (one .ok file per tile)
# - Safe to stop and resume; re-runs only missing tiles
# - Parallelism via xargs -P (default PARALLEL=4)
#
# Usage:
#   ./run_plate_tiles_resume_parallel.sh [plate_tiles.csv]
#
# Env vars:
#   PARALLEL   Number of concurrent jobs (default: 4)
#   RETRY_AFTER  Seconds between retries in run.sh (default: 4)
#   RUN_SH     Path to run.sh (default: ./run.sh)
#   DRYRUN     If set to 1, only print planned actions, do not execute
#
# Work files:
#   <CSV>.done.d/       checkpoint dir (contains *.ok files)
#   <CSV>.work.tsv      worklist for this invocation (auto-removed on success)

CSV="${1:-plate_tiles.csv}"
PARALLEL="${PARALLEL:-4}"
RETRY_AFTER="${RETRY_AFTER:-4}"
RUN_SH="${RUN_SH:-./run.sh}"
DRYRUN="${DRYRUN:-0}"

if [[ ! -f "$CSV" ]]; then
  echo "CSV not found: $CSV" >&2
  exit 1
fi

DONE_DIR="${CSV%.csv}.done.d"
WORK_TSV="${CSV%.csv}.work.tsv"

mkdir -p "$DONE_DIR"

# portable hash function (prefers shasum, falls back to md5)
hash_line() {
  local s="$1"
  if command -v shasum >/dev/null 2>&1; then
    printf '%s' "$s" | shasum | awk '{print $1}'
  elif command -v md5 >/dev/null 2>&1; then
    # macOS md5 prints 'MD5 (stdin) = <hash>' unless -q
    printf '%s' "$s" | md5 -q
  else
    # last resort: simple substitution (not cryptographic)
    printf '%s' "$s" | tr -c '[:alnum:]' '_' | cut -c1-64
  fi
}

# Build worklist (plate ra dec size overlap id)
# Skip header, compute id for each line, skip if already done
: > "$WORK_TSV"
awk -F, 'NR==1{next} {print $1","$2","$3","$4","$5}' "$CSV" | while IFS=, read -r plate ra dec size overlap; do
  line="$plate,$ra,$dec,$size,$overlap"
  id="$(hash_line "$line")"
  if [[ -f "$DONE_DIR/$id.ok" ]]; then
    printf "[SKIP] %s RA=%s Dec=%s size=%s\n" "$plate" "$ra" "$dec" "$size"
    continue
  fi
  printf "%s\t%s\t%s\t%s\t%s\t%s\n" "$plate" "$ra" "$dec" "$size" "$overlap" "$id" >> "$WORK_TSV"

done

# Count remaining
remaining=$(wc -l < "$WORK_TSV" | tr -d ' ')
completed=$(find "$DONE_DIR" -type f -name '*.ok' 2>/dev/null | wc -l | tr -d ' ')

echo "Completed: $completed  |  Remaining: $remaining  |  Parallel: $PARALLEL"

if [[ "$remaining" -eq 0 ]]; then
  echo "Nothing to do. All tiles appear complete."
  rm -f "$WORK_TSV"
  exit 0
fi

if [[ "$DRYRUN" == "1" ]]; then
  echo "[DRYRUN] Would run $remaining tiles with PARALLEL=$PARALLEL"
  exit 0
fi

# Run in parallel using xargs
# Each job consumes 6 fields: plate ra dec size overlap id
# We pass DONE_DIR, RETRY_AFTER, RUN_SH as leading arguments to the worker
cat "$WORK_TSV" \
  | xargs -n6 -P "$PARALLEL" bash "$(dirname "$0")/tools/plate_tile_worker.sh" "$DONE_DIR" "$RETRY_AFTER" "$RUN_SH"

# Cleanup worklist after success
rm -f "$WORK_TSV"

echo "All remaining tiles processed." 
