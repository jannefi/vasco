#!/usr/bin/env bash
set -euo pipefail

CHUNK_GLOB="${CHUNK_GLOB:-work/scos_chunks/chunk_*.csv}"
OUT_ROOT="${OUT_ROOT:-data/local-cats/_master_optical_parquet_flags/skybot}"
T2P="${T2P:-metadata/tiles/tile_to_plate_lookup.parquet}"
PEP="${PEP:-metadata/plates/plate_epoch_lookup.parquet}"
PY="${PYTHON_BIN:-python}"

FIELD_RS_ARCMIN="${FIELD_RS_ARCMIN:-9}"
MATCH_ARCSEC="${MATCH_ARCSEC:-5}"
WIDE_ARCSEC="${WIDE_ARCSEC:-30}"
TIMEOUT_S="${TIMEOUT_S:-20}"
MAX_RETRIES="${MAX_RETRIES:-1}"
WORKERS="${WORKERS:-3}"

mapfile -t CHUNKS < <(ls -1 ${CHUNK_GLOB})

echo "[INFO] Found ${#CHUNKS[@]} chunks"
if (( ${#CHUNKS[@]} == 0 )); then
  echo "[ERROR] No chunk files matched: ${CHUNK_GLOB}" >&2
  exit 1
fi

# Run with limited parallelism; requires GNU parallel or use xargs -P
printf "%s\n" "${CHUNKS[@]}" | xargs -I{} -P 2 \
  ${PY} ./scripts/skybot_fetch_chunk.py \
      --chunk-csv "{}" \
      --tile-to-plate "${T2P}" \
      --plate-epoch "${PEP}" \
      --out-root "${OUT_ROOT}" \
      --field-radius-arcmin "${FIELD_RS_ARCMIN}" \
      --match-arcsec "${MATCH_ARCSEC}" \
      --fallback-wide-arcsec "${WIDE_ARCSEC}" \
      --workers "${WORKERS}" \
      --timeout "${TIMEOUT_S}" \
      --max-retries "${MAX_RETRIES}"

