#!/usr/bin/env bash
# SkyBoT full run (Option C), resume-safe, background-friendly
set -euo pipefail

# ---------- Tunables via env ----------
PARALLEL="${PARALLEL:-2}"        # xargs -P; keep small for etiquette
WORK="${WORK:-./work}"
CHUNK_DIR="${CHUNK_DIR:-$WORK/scos_chunks}"
OUTROOT="${OUTROOT:-data/local-cats/_master_optical_parquet_flags/skybot}"
LOGDIR="${LOGDIR:-./logs}"
SURV_DIR="${SURV_DIR:-}"         # optional; if set, do quick counts at end
FIELD_ARCMIN="${FIELD_ARCMIN:-22}"     # << cover tile footprint
MATCH_ARCSEC="${MATCH_ARCSEC:-5}"
WIDE_ARCSEC="${WIDE_ARCSEC:-60}"       # << MNRAS-like wide
FB_PER_ROW="${FB_PER_ROW:-true}"
FB_CAP="${FB_CAP:-100}"
CTO="${CTO:-5}"    # connect timeout
RTO="${RTO:-5}"    # read timeout
RETRIES="${RETRIES:-0}"
# -------------------------------------

PARTS="$OUTROOT/parts"
LOCKDIR="$OUTROOT/.locks"
mkdir -p "$PARTS" "$LOCKDIR" "$LOGDIR"

RUN_ID="$(date +%Y%m%d_%H%M%S)"
RUN_LOG="$LOGDIR/skybot_full_${RUN_ID}.log"

# Log to file (and stdout if foreground)
exec > >(tee -a "$RUN_LOG") 2>&1
echo "[start] SkyBoT full run (Option C)  parallel=$PARALLEL  outroot=$OUTROOT"
echo "[info]  log: $RUN_LOG"
date -Is

trap 'echo "[signal] termination requested; exiting."; exit 143' TERM INT

# Discover chunks
mapfile -t CHUNKS < <(ls -1 "$CHUNK_DIR"/chunk_*.csv | sort)
TOTAL="${#CHUNKS[@]}"
echo "[info] discovered $TOTAL chunks in $CHUNK_DIR"

process_one() {
  in="$1"
  base="$(basename "$in" .csv)"
  out="$OUTROOT/parts/flags_skybot__${base}.parquet"
  lock="$LOCKDIR/${base}.lock"

  if [[ -s "$out" ]]; then
    echo "[skip] $base (part exists)"
    return 0
  fi

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
    --field-radius-arcmin "$FIELD_ARCMIN" \
    --match-arcsec "$MATCH_ARCSEC" --fallback-wide-arcsec "$WIDE_ARCSEC" \
    --connect-timeout "$CTO" --read-timeout "$RTO" --max-retries "$RETRIES" \
    --workers 1 \
    --fallback-per-row "$FB_PER_ROW" --fallback-per-row-cap "$FB_CAP"

  rm -f "$lock"

  local done_count
  done_count=$(ls -1 "$PARTS"/flags_skybot__*.parquet 2>/dev/null | wc -l || true)
  echo "[ok]   $base   (progress: ${done_count}/${TOTAL})"
}

export -f process_one
export OUTROOT PARTS LOCKDIR TOTAL FIELD_ARCMIN MATCH_ARCSEC WIDE_ARCSEC FB_PER_ROW FB_CAP CTO RTO RETRIES

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
      SUM(CASE WHEN wide_skybot_match THEN 1 ELSE 0 END) AS matched_60as_only
    FROM surv s LEFT JOIN flags f USING(row_id);
SQL
fi

echo "[finish] SkyBoT full run completed at $(date -Is)"
