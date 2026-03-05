#!/usr/bin/env bash
set -euo pipefail

OLD_ROOT="${OLD_ROOT:-./data/local-cats/_master_optical_parquet}"
CUR_ROOT="${CUR_ROOT:-./data/local-cats/_master_optical_parquet_with_plateid_region}"
MAP_CSV="${MAP_CSV:-./data/metadata/tile_to_dss1red.csv}"
PY="${PYTHON_BIN:-python}"
LOCK="./logs/.promote_master_tiles.lock"
LOG_DIR="./logs"
STAMP="$(date -u +%Y%m%d_%H%M%S)"
LOG="${LOG_DIR}/promote_master_tiles_${STAMP}.log"

mkdir -p "${LOG_DIR}"

exec 9>"${LOCK}"
if ! flock -n 9; then
  echo "[SKIP] another promotion is running (lock: ${LOCK})"
  exit 0
fi

echo "[RUN] $(date -u +%FT%TZ) old=${OLD_ROOT} cur=${CUR_ROOT} map=${MAP_CSV}" | tee -a "${LOG}"
${PY} ./scripts/promote_tiles_to_curated.py \
  --old-root "${OLD_ROOT}" \
  --cur-root "${CUR_ROOT}" \
  --map-csv "${MAP_CSV}" \
  --require-plate \
  >> "${LOG}" 2>&1

rc=$?
echo "[DONE] $(date -u +%FT%TZ) rc=${rc}" | tee -a "${LOG}"
exit $rc
