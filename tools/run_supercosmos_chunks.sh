#!/usr/bin/env bash
set -euo pipefail

# --- Config (adjust if paths differ) ---------------------------------------
REPO_ROOT="${REPO_ROOT:-$HOME/code/vasco}"
cd "$REPO_ROOT"

WORK="${WORK:-./work}"
UPLOAD="${UPLOAD:-$WORK/survivors_supercosmos_upload.csv}"   # built in Section C (page)
OUTROOT="${OUTROOT:-./data/local-cats/_master_optical_parquet_flags/flags_supercosmos}"
CHUNK_DIR="${CHUNK_DIR:-$WORK/scos_chunks}"
LOG_DIR="${LOG_DIR:-$REPO_ROOT/logs}"
DONE_LIST="${DONE_LIST:-$LOG_DIR/SCOS_DONE.list}"
RUN_LOG="${RUN_LOG:-$LOG_DIR/SCOS_RUN.log}"
CHUNK_SIZE="${1:-5000}"          # default probe size; can pass 2000 or 1000
MAX_RETRIES=3
BACKOFF_BASE=5                   # seconds

mkdir -p "$OUTROOT" "$CHUNK_DIR" "$LOG_DIR"

# --- Preflight checks -------------------------------------------------------
command -v stilts >/dev/null || { echo "[ERR] STILTS not in PATH"; exit 1; }
[[ -s "$UPLOAD" ]] || { echo "[ERR] Upload CSV not found or empty: $UPLOAD"; exit 1; }

echo "[INFO] Using upload: $UPLOAD"
echo "[INFO] Chunk size: $CHUNK_SIZE"

# Split only if no existing chunks for this size
if ! ls "$CHUNK_DIR"/chunk_*.csv >/dev/null 2>&1 ; then
  echo "[INFO] Splitting upload -> $CHUNK_DIR (size=$CHUNK_SIZE)"
  split -d -l "$CHUNK_SIZE" --additional-suffix=.csv "$UPLOAD" "$CHUNK_DIR/chunk_"
fi

touch "$DONE_LIST" "$RUN_LOG"

# --- Process loop -----------------------------------------------------------
for f in "$CHUNK_DIR"/chunk_*.csv; do
  base="$(basename "$f" .csv)"
  # Skip already done chunks
  if grep -qx "$base" "$DONE_LIST"; then
    echo "[skip] $base already processed" | tee -a "$RUN_LOG"
    continue
  fi

  vot="$CHUNK_DIR/${base}.vot"
  tmpout="$OUTROOT/_tmp/${base}"
  final_parq="$OUTROOT/flags_supercosmos__${base}.parquet"
  final_csv="$OUTROOT/flags_supercosmos__${base}.csv"

  mkdir -p "$tmpout"

  # CSV -> VOT (fetcher expects VOT)
  stilts tcopy in="$f" ifmt=csv out="$vot" ofmt=votable

  # Retry wrapper around the fetcher
  try=1
  until ./scripts/fetch_supercosmos_stilts.sh "$vot" "$tmpout"; do
    echo "[warn] fetch failed for $base (try $try/$MAX_RETRIES)" | tee -a "$RUN_LOG"
    (( try >= MAX_RETRIES )) && { echo "[fail] giving up $base" | tee -a "$RUN_LOG"; rm -rf "$tmpout"; exit 1; }
    sleep "$((BACKOFF_BASE * try))"
    ((try++))
  done

  # The fetcher writes fixed names; rename per chunk to accumulate
  [[ -f "$tmpout/flags_supercosmos.parquet" ]] && mv -f "$tmpout/flags_supercosmos.parquet" "$final_parq"
  [[ -f "$tmpout/flags_supercosmos.csv"     ]] && mv -f "$tmpout/flags_supercosmos.csv"     "$final_csv"
  rm -rf "$tmpout" "$vot"

  echo "$base" >> "$DONE_LIST"
  echo "[ok] $base" | tee -a "$RUN_LOG"
done

# --- Consolidate all per-chunk Parquets into one file (dedup row_id) -------
duckdb -c "
PRAGMA memory_limit='9GB';
PRAGMA temp_directory='$WORK/_duckdb_tmp';
CREATE OR REPLACE TABLE scos_union AS
  SELECT * FROM read_parquet('$OUTROOT/flags_supercosmos__*.parquet');
CREATE OR REPLACE TABLE scos_dedup AS
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY row_id ORDER BY row_id) AS rn
    FROM scos_union
  ) WHERE rn=1;
COPY (SELECT row_id, is_supercosmos_artifact FROM scos_dedup)
TO '$OUTROOT/flags_supercosmos.parquet' (FORMAT PARQUET);
"

echo "[DONE] Wrote consolidated: $OUTROOT/flags_supercosmos.parquet"
