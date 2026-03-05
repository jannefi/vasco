
#!/usr/bin/env bash
# find_missing_closest.sh
# Detect and (optionally) retry only the missing *_closest.csv derived from positions_chunk_*.csv
# Works with the current naming scheme used by tap_async_one.sh:
#   positions_chunk_00029.csv  -> positions00029_closest.csv
# Usage examples:
#   ./scripts/find_missing_closest.sh
#   ./scripts/find_missing_closest.sh --retry
#   ./scripts/find_missing_closest.sh --dir ./data/local-cats/tmp/positions --retry --adql ./scripts/adql_neowise_se_SIMPLE.sql --parallel 8
#   ./scripts/find_missing_closest.sh --symlink-new-style

set -euo pipefail

DIR="./data/local-cats/tmp/positions"
ADQL="./scripts/adql_neowise_se_SIMPLE.sql"
PARALLEL="${PARALLEL:-6}"
DO_RETRY="no"
DO_SYMLINK="no"

# --- tiny arg parser ---
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dir)        DIR="${2:?}"; shift 2 ;;
    --adql)       ADQL="${2:?}"; shift 2 ;;
    --parallel)   PARALLEL="${2:?}"; shift 2 ;;
    --retry)      DO_RETRY="yes"; shift ;;
    --symlink-new-style) DO_SYMLINK="yes"; shift ;;
    -h|--help)
      cat <<EOF
Usage: $(basename "$0") [--dir DIR] [--retry] [--adql FILE] [--parallel N] [--symlink-new-style]
  --dir DIR             Positions directory (default: $DIR)
  --retry               Retry only missing chunks now (runs tap_async_one.sh in parallel)
  --adql FILE           ADQL file for retries (default: $ADQL)
  --parallel N          Parallel retries (default: \$PARALLEL or $PARALLEL)
  --symlink-new-style   Add compatibility symlinks: positions_chunk_XXXX_closest.csv -> positionsXXXX_closest.csv
EOF
      exit 0
      ;;
    *) echo "[ERROR] Unknown option: $1" >&2; exit 2 ;;
  esac
done

# --- sanity checks ---
[[ -d "$DIR" ]] || { echo "[ERROR] DIR not found: $DIR" >&2; exit 2; }
if [[ "$DO_RETRY" == "yes" ]]; then
  [[ -f "$ADQL" ]] || { echo "[ERROR] ADQL not found: $ADQL" >&2; exit 2; }
  [[ -x "./scripts/tap_async_one.sh" ]] || { echo "[ERROR] scripts/tap_async_one.sh not executable or missing" >&2; exit 2; }
fi

# Enable nullglob to avoid literal patterns when no matches
shopt -s nullglob

# --- compute totals and missing list (mapping-aware) ---
chunks=( "$DIR"/positions_chunk_*.csv )
total=${#chunks[@]}
missing_list="$DIR/_missing_closest.lst"
: > "$missing_list"

closest_done=0
raw_done=0
vot_done=0

for f in "${chunks[@]}"; do
  base="$(basename "$f" .csv)"                       # positions_chunk_00029
  expect_closest="$DIR/${base/_chunk_/}_closest.csv" # positions00029_closest.csv
  expect_raw="$DIR/${base/_chunk_/}_raw.csv"         # positions00029_raw.csv
  expect_vot="$DIR/${base}.vot"                      # positions_chunk_00029.vot

  [[ -f "$expect_vot" ]]     && ((vot_done++))
  [[ -f "$expect_raw" ]]     && ((raw_done++))
  if [[ -f "$expect_closest" && -s "$expect_closest" ]]; then
    ((closest_done++))
  else
    echo "$f" >> "$missing_list"
  fi
done

pct() { awk "BEGIN{t=$2; d=$1; if (t>0){printf \"%.1f\", (d*100.0)/t}else{printf \"0.0\"}}"; }

echo "[PROGRESS] DIR=$DIR"
printf "  VOT     %4d / %-4d (%s%%)\n" "$vot_done"     "$total" "$(pct "$vot_done" "$total")"
printf "  RAW     %4d / %-4d (%s%%)\n" "$raw_done"     "$total" "$(pct "$raw_done" "$total")"
printf "  CLOSEST %4d / %-4d (%s%%)\n" "$closest_done" "$total" "$(pct "$closest_done" "$total")"

missing_count=$(( total - closest_done ))
echo "[INFO] Missing closest count: $missing_count"
if (( missing_count > 0 )); then
  echo "[INFO] Missing list: $missing_list (first 15 shown)"
  head -n 15 "$missing_list" || true
else
  echo "[INFO] All chunks have *_closest.csv present and non-empty."
fi

# --- optional: create compatibility symlinks using chunk-style names ---
if [[ "$DO_SYMLINK" == "yes" ]]; then
  echo "[INFO] Creating chunk-style symlinks for existing legacy closest files…"
  created=0
  for f in "$DIR"/positions_chunk_*.csv; do
    base="$(basename "$f" .csv)"
    legacy="$DIR/${base/_chunk_/}_closest.csv"
    new="$DIR/${base}_closest.csv"
    if [[ -f "$legacy" && ! -e "$new" ]]; then
      ( cd "$DIR" && ln -s "$(basename "$legacy")" "$(basename "$new")" )
      ((created++))
    fi
  done
  echo "[OK] Symlinks created: $created"
fi

# --- optional: retry only the missing chunks now ---
if [[ "$DO_RETRY" == "yes" ]]; then
  if (( missing_count == 0 )); then
    echo "[INFO] Nothing to retry."
    exit 0
  fi

  echo "[RETRY] Retrying $missing_count chunk(s) with parallel=$PARALLEL"
  # Use xargs -P to control concurrency; each chunk writes its own .retry.log
  # We keep the pipeline resilient so the loop completes even if some chunks still fail.
  < "$missing_list" xargs -n1 -P "$PARALLEL" -I{} bash -c '
    CHUNK="$1"
    LOG="${CHUNK%.csv}.retry.log"
    echo "[RETRY] ${CHUNK} -> ${LOG}"
    if [[ -f "${CHUNK%.csv/_chunk_/}_closest.csv" && -s "${CHUNK%.csv/_chunk_/}_closest.csv" ]]; then
      echo "[SKIP] already has legacy closest: ${CHUNK}" | tee -a "$LOG"
      exit 0
    fi
    bash ./scripts/tap_async_one.sh "$CHUNK" "'"$ADQL"'" > "$LOG" 2>&1 || {
      echo "[FAIL] $CHUNK (see $LOG)" >&2
      exit 0
    }
  ' _ {}

  echo "[RETRY] Done. Re-run this script without --retry to refresh counts."
fi

