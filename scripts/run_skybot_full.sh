#!/usr/bin/env bash
# SkyBoT full run (Option C), resume-safe, background-friendly
set -euo pipefail

# ---------- Tunables via env ----------
PARALLEL="${PARALLEL:-2}"   # xargs -P; keep small for etiquette
WORK="${WORK:-./work}"
CHUNK_DIR="${CHUNK_DIR:-$WORK/scos_chunks}"
OUTROOT="${OUTROOT:-data/local-cats/_master_optical_parquet_flags/skybot}"
LOGDIR="${LOGDIR:-./logs}"
SURV_DIR="${SURV_DIR:-}"    # optional; if set, do quick counts at end
# -------------------------------------

PARTS="$OUTROOT/parts"
LOCKDIR="$OUTROOT/.locks"
mkdir -p "$PARTS" "$LOCKDIR" "$LOGDIR"

RUN_ID="$(date +%Y%m%d_%H%M%S)"
RUN_LOG="$LOGDIR/skybot_full_${RUN_ID}.log"

# Log everything to a rotating file and stdout if run in foreground
exec > >(tee -a "$RUN_LOG") 2>&1

echo "[start] SkyBoT full run (Option C)  parallel=$PARALLEL   outroot=$OUTROOT"
echo "[info] log: $RUN_LOG"
date -Is

# Graceful stop on signals
trap 'echo "[signal] termination requested; exiting."; exit 143' TERM INT

# Discover chunks
mapfile -t CHUNKS < <(ls -1 "$CHUNK_DIR"/chunk_*.csv | sort)
TOTAL="${#CHUNKS[@]}"
echo "[info] discovered $TOTAL chunks in $CHUNK_DIR"

# Function to process a single chunk (skip-if-output-exists + lock)
process_one() {
  in="$1"
  base="$(basename "$in" .csv)"
  out="$OUTROOT/parts/flags_skybot__${base}.parquet"
  lock="$LOCKDIR/${base}.lock"

  if [[ -s "$out" ]]; then
    echo "[skip] $base (part exists)"
    return 0
  fi

  # Non-blocking lock to avoid any accidental double-processing
  exec 9>"$lock"
  if ! flock -n 9; then
    echo "[skip] $base (held by another process)"
    return 0
  fi

  echo "[run]  $base"
  python ./scripts/skybot_fetch_chunk.py \
    --chunk-csv "$in" \
    --tile-to-plate metadata/tiles/tile_to_plate_lookup.parquet \
    --plate-epoch  metadata/plates/plate_epoch_lookup.parquet \
    --out-root     "$OUTROOT" \
    --field-radius-arcmin 9 \
    --match-arcsec 5 --fallback-wide-arcsec 30 \
    --connect-timeout 5 --read-timeout 5 --max-retries 0 \
    --workers 1 \
    --fallback-per-row true --fallback-per-row-cap 100

  # Unlock by closing fd 9; delete the file for hygiene
  rm -f "$lock"

  # Progress snapshot
  local done_count
  done_count=$(ls -1 "$PARTS"/flags_skybot__*.parquet 2>/dev/null | wc -l || true)
  echo "[ok]   $base   (progress: ${done_count}/${TOTAL})"
}

export -f process_one
export OUTROOT PARTS LOCKDIR TOTAL

# Parallel run (resume-safe); will skip existing parts
printf '%s\0' "${CHUNKS[@]}" \
| xargs -0 -n1 -P "$PARALLEL" bash -c 'process_one "$@"' _

echo "[merge] Aggregating canonicals with DuckDB"
duckdb -batch <<'SQL'
  INSTALL parquet; LOAD parquet;
  CREATE OR REPLACE VIEW parts AS
    SELECT * FROM read_parquet('data/local-cats/_master_optical_parquet_flags/skybot/parts/flags_skybot__*.parquet');
  COPY (
    SELECT row_id,
           BOOL_OR(has_skybot_match)  AS has_skybot_match,
           BOOL_OR(wide_skybot_match) AS wide_skybot_match,
           MIN(best_sep_arcsec)       AS best_sep_arcsec_min
    FROM parts
    GROUP BY 1
  )
  TO 'data/local-cats/_master_optical_parquet_flags/skybot/flags_skybot.parquet'
  (FORMAT PARQUET);

  COPY parts
    TO 'data/local-cats/_master_optical_parquet_flags/skybot/flags_skybot_audit.parquet'
  (FORMAT PARQUET);
SQL
echo "[merge] done"

# Optional quick counts against survivors if SURV_DIR provided
if [[ -n "$SURV_DIR" && -d "$SURV_DIR" ]]; then
  echo "[counts] Joining flags to survivors under $SURV_DIR"
  duckdb -batch <<SQL
    INSTALL parquet; LOAD parquet;

    CREATE OR REPLACE VIEW surv AS
      SELECT tile_id, NUMBER, CONCAT(tile_id, ':', NUMBER) AS row_id
      FROM parquet_scan('${SURV_DIR}/ra_bin=*/dec_bin=*/part-*.parquet');

    CREATE OR REPLACE VIEW flags AS
      SELECT * FROM read_parquet('data/local-cats/_master_optical_parquet_flags/skybot/flags_skybot.parquet');

    SELECT
      COUNT(*) AS survivors_checked,
      SUM(CASE WHEN has_skybot_match  THEN 1 ELSE 0 END) AS matched_5as,
      SUM(CASE WHEN wide_skybot_match THEN 1 ELSE 0 END) AS matched_30as_only
    FROM surv s LEFT JOIN flags f USING(row_id);
SQL
fi

echo "[finish] SkyBoT full run completed at $(date -Is)"

