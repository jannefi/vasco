#!/usr/bin/env bash
# Gaia DR3 + PS1 DR2, survivors-chunked, cdsskymatch-based (TAPVizieR)
set -euo pipefail

# --------- Tunables via env ----------
PARALLEL="${PARALLEL:-2}"                 # gentle to TAPVizieR (same as your VOSA run)
WORK="${WORK:-./work}"
CHUNK_DIR="${CHUNK_DIR:-$WORK/survivor_chunks_20k}"   # directory containing chunk_*.csv
OUTROOT="${OUTROOT:-$WORK/gaia_ps1_flags}"
LOGDIR="${LOGDIR:-./logs}"
RADIUS_ARCSEC="${RADIUS_ARCSEC:-1.0}"
GAIA_TABLE="${GAIA_TABLE:-I/355/gaiadr3}"
PS1_TABLE="${PS1_TABLE:-II/389/ps1_dr2}"
BLOCKSIZE="${BLOCKSIZE:-1000}"
# -------------------------------------

PARTS="$OUTROOT/parts"
LOCKDIR="$OUTROOT/.locks"
mkdir -p "$PARTS" "$LOCKDIR" "$LOGDIR"

RUN_ID="$(date +%Y%m%d_%H%M%S)"
RUN_LOG="$LOGDIR/gaia_ps1_full_${RUN_ID}.log"
exec > >(tee -a "$RUN_LOG") 2>&1

echo "[start] Gaia+PS1 run parallel=$PARALLEL outroot=$OUTROOT radius=${RADIUS_ARCSEC}\""
echo "[info] chunks: $CHUNK_DIR"
echo "[info] tables: gaia=$GAIA_TABLE ps1=$PS1_TABLE"
echo "[info] log: $RUN_LOG"
date -Is

mapfile -t CHUNKS < <(ls -1 "$CHUNK_DIR"/chunk_*.csv | sort)
TOTAL="${#CHUNKS[@]}"
echo "[info] discovered $TOTAL chunks in $CHUNK_DIR"

process_one() {
  in="$1"
  base="$(basename "$in" .csv)"
  out="$OUTROOT/parts/flags_gaia_ps1__${base}.parquet"
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

  echo "[run] $base"
  python ./scripts/vizier_gaia_ps1_chunk.py \
    --chunk-csv "$in" \
    --out-root "$OUTROOT" \
    --radius-arcsec "$RADIUS_ARCSEC" \
    --gaia-table "$GAIA_TABLE" \
    --ps1-table "$PS1_TABLE" \
    --blocksize "$BLOCKSIZE"

  rm -f "$lock" || true
  done_count="$(ls -1 "$PARTS"/flags_gaia_ps1__*.parquet 2>/dev/null | wc -l || true)"
  echo "[ok] $base (progress: ${done_count}/${TOTAL})"
}
export -f process_one
export OUTROOT PARTS LOCKDIR TOTAL RADIUS_ARCSEC GAIA_TABLE PS1_TABLE BLOCKSIZE

printf '%s\0' "${CHUNKS[@]}" | xargs -0 -n1 -P "$PARALLEL" bash -c 'process_one "$@"' _

echo "[finish] Gaia+PS1 run completed at $(date -Is)"
