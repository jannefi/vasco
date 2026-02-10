#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$HOME/code/vasco}"
cd "$REPO_ROOT"

WORK="${WORK:-./work}"
CHUNK_DIR="${CHUNK_DIR:-$WORK/scos_chunks}"
OUTROOT="${OUTROOT:-./data/local-cats/_master_optical_parquet_flags/flags_supercosmos}"
LOG_DIR="${LOG_DIR:-$REPO_ROOT/logs}"
DONE_LIST="${DONE_LIST:-$LOG_DIR/SCOS_DONE.list}"
RUN_LOG="${RUN_LOG:-$LOG_DIR/SCOS_RUN.log}"

mkdir -p "$OUTROOT" "$LOG_DIR"
touch "$DONE_LIST" "$RUN_LOG"

shopt -s nullglob
# Prefer VOT chunks if present, else CSV chunks (convert on the fly)
mapfile -t chunks < <(ls -1 "$CHUNK_DIR"/chunk_*.vot "$CHUNK_DIR"/chunk_*.csv 2>/dev/null | sort)

for f in "${chunks[@]}"; do
  base="$(basename "${f%.*}")"   # chunk_XXXXX
  if grep -qx "$base" "$DONE_LIST"; then
    echo "[skip] $base" | tee -a "$RUN_LOG"; continue
  fi

  # If CSV, convert to VOT first (preserves header)
  if [[ "$f" == *.csv ]]; then
    vot="${f%.csv}.vot"
    stilts tcopy in="$f" ifmt=csv out="$vot" ofmt=votable
    src="$vot"
  else
    src="$f"
  fi

  tmpout="$OUTROOT/_tmp/${base}"
  final_parq="$OUTROOT/flags_supercosmos__${base}.parquet"
  mkdir -p "$tmpout"

  # Run your fetcher
  if ./scripts/fetch_supercosmos_stilts.sh "$src" "$tmpout"; then
    [[ -f "$tmpout/flags_supercosmos.parquet" ]] && mv -f "$tmpout/flags_supercosmos.parquet" "$final_parq"
    rm -rf "$tmpout"
    echo "$base" >> "$DONE_LIST"
    echo "[ok] $base" | tee -a "$RUN_LOG"
  else
    echo "[fail] $base" | tee -a "$RUN_LOG"
    rm -rf "$tmpout"
    exit 1
  fi
done

# Consolidate at end (dedup by row_id)
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
echo "[DONE] consolidated -> $OUTROOT/flags_supercosmos.parquet"
