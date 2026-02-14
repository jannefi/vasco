#!/usr/bin/env bash
# VOSA-equivalent (CatWISE+unWISE+AllWISE+2MASS+GALEX) at 5", survivors-chunked
set -euo pipefail

# --------- Tunables via env ----------
PARALLEL="${PARALLEL:-2}"                 # gentle to TAPVizieR
WORK="${WORK:-./work}"
CHUNK_DIR="${CHUNK_DIR:-$WORK/scos_chunks}"
OUTROOT="${OUTROOT:-data/local-cats/_master_optical_parquet_flags/vosa_like}"
LOGDIR="${LOGDIR:-./logs}"
RADIUS_ARCSEC="${RADIUS_ARCSEC:-5}"

# default catalogue list (space-separated)
CATALOGS_DEFAULT="II/365/catwise II/363/unwise II/328/allwise II/246/out II/335/galex_ais"
CATALOGS="${CATALOGS:-$CATALOGS_DEFAULT}"
# -------------------------------------

PARTS="$OUTROOT/parts"
LOCKDIR="$OUTROOT/.locks"
mkdir -p "$PARTS" "$LOCKDIR" "$LOGDIR"

RUN_ID="$(date +%Y%m%d_%H%M%S)"
RUN_LOG="$LOGDIR/vosa_like_full_${RUN_ID}.log"
exec > >(tee -a "$RUN_LOG") 2>&1
echo "[start] VOSA-like full run  parallel=$PARALLEL  outroot=$OUTROOT  radius=${RADIUS_ARCSEC}\""
echo "[info]  catalogs: $CATALOGS"
echo "[info]  log: $RUN_LOG"
date -Is

mapfile -t CHUNKS < <(ls -1 "$CHUNK_DIR"/chunk_*.csv | sort)
TOTAL="${#CHUNKS[@]}"
echo "[info] discovered $TOTAL chunks in $CHUNK_DIR"

process_one() {
  in="$1"
  base="$(basename "$in" .csv)"
  out="$OUTROOT/parts/flags_vosa_like__${base}.parquet"
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
  python ./scripts/vizier_vosa_like_chunk.py \
    --chunk-csv "$in" \
    --out-root "$OUTROOT" \
    --radius-arcsec "$RADIUS_ARCSEC" \
    --catalogs $CATALOGS

  rm -f "$lock"
  local done_count
  done_count=$(ls -1 "$PARTS"/flags_vosa_like__*.parquet 2>/dev/null | wc -l || true)
  echo "[ok]   $base (progress: ${done_count}/${TOTAL})"
}

export -f process_one
export OUTROOT PARTS LOCKDIR TOTAL RADIUS_ARCSEC CATALOGS

printf '%s\0' "${CHUNKS[@]}" \
| xargs -0 -n1 -P "$PARALLEL" bash -c 'process_one "$@"' _

echo "[merge] Aggregating canonicals with DuckDB"
duckdb -batch <<'SQL'
  INSTALL parquet; LOAD parquet;
  CREATE OR REPLACE VIEW parts AS
    SELECT * FROM read_parquet('data/local-cats/_master_optical_parquet_flags/vosa_like/parts/flags_vosa_like__*.parquet');
  COPY (
    SELECT
      row_id,
      BOOL_OR(has_catwise2020_match) AS has_catwise2020_match,
      BOOL_OR(has_unwise_match)      AS has_unwise_match,
      BOOL_OR(has_allwise_match)     AS has_allwise_match,
      BOOL_OR(has_2mass_match)       AS has_2mass_match,
      BOOL_OR(has_galex_match)       AS has_galex_match,
      BOOL_OR(has_vosa_like_match)   AS has_vosa_like_match
    FROM parts
    GROUP BY 1
  )
  TO 'data/local-cats/_master_optical_parquet_flags/vosa_like/flags_vosa_like.parquet'
  (FORMAT PARQUET);
SQL
echo "[merge] done"

echo "[finish] VOSA-like full run completed at $(date -Is)"
