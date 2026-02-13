#!/usr/bin/env bash
# Parallel runner for PTF (ngood-gated) over survivors chunks.
set -euo pipefail

INPUT_DIR="./work/scos_chunks"
OUT_ROOT="./data/local-cats/_master_optical_parquet_flags"
PARTS_DIR="${OUT_ROOT}/ptf_ngood/parts"
CANON="${OUT_ROOT}/flags_ptf_objects_ngood.parquet"
AUDIT="${OUT_ROOT}/flags_ptf_objects_ngood_audit.parquet"
LOG_DIR="./logs"
RADIUS_ARCSEC=5
PTF_TABLE="ptf_objects"
CONCURRENCY=5
SLEEP_BETWEEN=0

mkdir -p "$LOG_DIR" "$PARTS_DIR"
LOG_DATE=$(date +"%d%m%y")
LOG_FILE="$LOG_DIR/ptf_ngood_${LOG_DATE}.log"

merge_parts() {
  LOCK="${PARTS_DIR}/.merge.lock"; exec 9>"$LOCK"
  if ! flock -n 9; then echo "[merge] another merge is in progress; skipping" | tee -a "$LOG_FILE"; return 0; fi
  echo "[merge] starting at $(date -u +%FT%TZ)" | tee -a "$LOG_FILE"

  shopt -s nullglob
  PART_FILES=("${PARTS_DIR}"/flags_ptf_objects__*.parquet)
  shopt -u nullglob
  if [ ${#PART_FILES[@]} -eq 0 ]; then
    echo "[merge] no part files under ${PARTS_DIR}, skipping." | tee -a "$LOG_FILE"; return 0
  fi

  CANON_TMP="${CANON}.tmp"; AUDIT_TMP="${AUDIT}.tmp"
  duckdb -c "
    INSTALL parquet; LOAD parquet;
    CREATE OR REPLACE VIEW parts AS
      SELECT * FROM read_parquet('${PARTS_DIR}/flags_ptf_objects__*.parquet');

    COPY (
      SELECT NUMBER, TRUE AS ptf_match_ngood
      FROM parts WHERE NUMBER IS NOT NULL GROUP BY NUMBER
    ) TO '${CANON_TMP}' (FORMAT PARQUET);

    COPY (SELECT * FROM parts) TO '${AUDIT_TMP}' (FORMAT PARQUET);
  " | tee -a "$LOG_FILE"

  mv -f "${CANON_TMP}" "${CANON}"
  mv -f "${AUDIT_TMP}" "${AUDIT}"

  [ -s "${CANON}" ] && duckdb -c "INSTALL parquet; LOAD parquet; SELECT COUNT(*) AS rows FROM read_parquet('${CANON}');" | tee -a "$LOG_FILE"
  echo "[merge] finished at $(date -u +%FT%TZ)" | tee -a "$LOG_FILE"
}

trap 'echo "[trap] signal; partial merge..."; merge_parts; exit 130' INT TERM

{
  echo "PTF (ngood) batch at $(date -u +%FT%TZ)  INPUT_DIR=${INPUT_DIR}  PARTS_DIR=${PARTS_DIR}"
  mapfile -t CHUNKS < <(find "${INPUT_DIR}" -maxdepth 1 -type f -name 'chunk_*.csv' | sort)
  [ ${#CHUNKS[@]} -eq 0 ] && { echo "[error] no chunks in ${INPUT_DIR}"; exit 2; }

  running=0
  for file in "${CHUNKS[@]}"; do
    fname="$(basename "$file")"; echo "[$(date +%H:%M:%S)] dispatch: ${fname}"
    (
      ./scripts/fetch_ptf_objects_ngood_stilts.sh "$file" "$PARTS_DIR" "$RADIUS_ARCSEC" "$PTF_TABLE"
      rc=$?; [ $rc -eq 0 ] && echo "[$(date +%H:%M:%S)] success: ${fname}" || echo "[$(date +%H:%M:%S)] ERROR($rc): ${fname}"
    ) >>"$LOG_FILE" 2>&1 &

    ((running+=1))
    if (( running >= CONCURRENCY )); then wait -n; ((running-=1)); fi
    sleep "${SLEEP_BETWEEN}"
  done

  wait
  merge_parts
  echo "All tasks complete at $(date -u +%FT%TZ)"
} >> "$LOG_FILE" 2>&1

