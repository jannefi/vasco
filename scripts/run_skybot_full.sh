#!/usr/bin/env bash
# SkyBoT full run (run-scoped chunks), resume-safe, background-friendly — NO DuckDB
set -euo pipefail

PARALLEL="${PARALLEL:-2}"
CHUNK_DIR="${CHUNK_DIR:-work/scos_chunks}"
OUTROOT="${OUTROOT:-work/scos_chunks/skybot}"
LOGDIR="${LOGDIR:-./logs}"

FIELD_ARCMIN="${FIELD_ARCMIN:-22}"
MATCH_ARCSEC="${MATCH_ARCSEC:-5}"
WIDE_ARCSEC="${WIDE_ARCSEC:-60}"

FB_PER_ROW="${FB_PER_ROW:-true}"
FB_CAP="${FB_CAP:-100}"

CTO="${CTO:-5}"
RTO="${RTO:-5}"
RETRIES="${RETRIES:-0}"

TILE2PLATE="${TILE2PLATE:-metadata/tiles/tile_to_plate_lookup.parquet}"
PLATEEPOCH="${PLATEEPOCH:-metadata/plates/plate_epoch_lookup.parquet}"

PARTS="$OUTROOT/parts"
LOCKDIR="$OUTROOT/.locks"
mkdir -p "$PARTS" "$LOCKDIR" "$LOGDIR"

RUN_ID="$(date +%Y%m%d_%H%M%S)"
RUN_LOG="$LOGDIR/skybot_full_${RUN_ID}.log"
exec > >(tee -a "$RUN_LOG") 2>&1

echo "[start] SkyBoT full run parallel=$PARALLEL outroot=$OUTROOT chunk_dir=$CHUNK_DIR"
date -Is
trap 'echo "[signal] termination requested; exiting."; exit 143' TERM INT

mapfile -t CHUNKS < <(ls -1 "$CHUNK_DIR"/upload_positional_chunk_*.csv 2>/dev/null | sort)
TOTAL="${#CHUNKS[@]}"
echo "[info] discovered $TOTAL chunks in $CHUNK_DIR (upload_positional_chunk_*.csv)"
if [[ "$TOTAL" -eq 0 ]]; then
  echo "[error] no upload_positional_chunk_*.csv files in $CHUNK_DIR"
  exit 2
fi

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

  echo "[run] $base"
  python ./scripts/skybot_fetch_chunk.py \
    --chunk-csv "$in" \
    --tile-to-plate "$TILE2PLATE" \
    --plate-epoch "$PLATEEPOCH" \
    --out-root "$OUTROOT" \
    --field-radius-arcmin "$FIELD_ARCMIN" \
    --match-arcsec "$MATCH_ARCSEC" --fallback-wide-arcsec "$WIDE_ARCSEC" \
    --connect-timeout "$CTO" --read-timeout "$RTO" --max-retries "$RETRIES" \
    --workers 1 \
    --fallback-per-row "$FB_PER_ROW" --fallback-per-row-cap "$FB_CAP"

  rm -f "$lock"
  local done_count
  done_count=$(ls -1 "$PARTS"/flags_skybot__*.parquet 2>/dev/null | wc -l || true)
  echo "[ok] $base (progress: ${done_count}/${TOTAL})"
}

export -f process_one
export OUTROOT PARTS LOCKDIR TOTAL FIELD_ARCMIN MATCH_ARCSEC WIDE_ARCSEC FB_PER_ROW FB_CAP CTO RTO RETRIES TILE2PLATE PLATEEPOCH

printf '%s\0' "${CHUNKS[@]}" | xargs -0 -n1 -P "$PARALLEL" bash -c 'set -euo pipefail; process_one "$@"' _

echo "[merge] Aggregating canonicals (Python; no DuckDB)"
python ./scripts/skybot/merge_skybot_parts.py --out-root "$OUTROOT"
echo "[merge] done"

echo "[finish] SkyBoT full run completed at $(date -Is)"
