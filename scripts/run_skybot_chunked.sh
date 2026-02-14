#!/usr/bin/env bash
set -euo pipefail

CHUNK_GLOB="${CHUNK_GLOB:-work/scos_chunks/chunk_*.csv}"
OUT_ROOT="${OUT_ROOT:-data/local-cats/_master_optical_parquet_flags/skybot}"
T2P="${T2P:-metadata/tiles/tile_to_plate_lookup.parquet}"
PEP="${PEP:-metadata/plates/plate_epoch_lookup.parquet}"
PY="${PYTHON_BIN:-python}"

FIELD_RS_ARCMIN="${FIELD_RS_ARCMIN:-22}"   # cover tile footprint
MATCH_ARCSEC="${MATCH_ARCSEC:-5}"
WIDE_ARCSEC="${WIDE_ARCSEC:-60}"
TIMEOUT_S="${TIMEOUT_S:-5}"
MAX_RETRIES="${MAX_RETRIES:-0}"
WORKERS="${WORKERS:-1}"
FB_PER_ROW="${FB_PER_ROW:-true}"
FB_CAP="${FB_CAP:-100}"

mapfile -t CHUNKS < <(ls -1 ${CHUNK_GLOB})
echo "[INFO] Found ${#CHUNKS[@]} chunks"
(( ${#CHUNKS[@]} > 0 )) || { echo "[ERROR] No chunk files matched: ${CHUNK_GLOB}" >&2; exit 1; }

# limited parallelism; consistent with etiquette
printf "%s\n" "${CHUNKS[@]}" \
| xargs -I{} -P 2 \
  ${PY} ./scripts/skybot_fetch_chunk.py \
    --chunk-csv "{}" \
    --tile-to-plate "${T2P}" \
    --plate-epoch  "${PEP}" \
    --out-root     "${OUT_ROOT}" \
    --field-radius-arcmin "${FIELD_RS_ARCMIN}" \
    --match-arcsec "${MATCH_ARCSEC}" \
    --fallback-wide-arcsec "${WIDE_ARCSEC}" \
    --workers "${WORKERS}" \
    --connect-timeout "${TIMEOUT_S}" \
    --read-timeout "${TIMEOUT_S}" \
    --max-retries "${MAX_RETRIES}" \
    --fallback-per-row "${FB_PER_ROW}" \
    --fallback-per-row-cap "${FB_CAP}"
