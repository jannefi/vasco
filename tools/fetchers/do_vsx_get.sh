#!/usr/bin/env bash
# Batch VSX flags over a directory of VOT chunks (e.g., ./work/scos_chunks/*.vot).
# Fast resume: pre-filters out chunks that already have a completed part file.
# Sleeps only when a job is actually dispatched (not for skipped chunks).
# Default: sequential (THREADS=1). Safe DuckDB merge to canonical/audit.
set -euo pipefail

CHUNKS_DIR="${1:?chunks_dir required}"       # e.g., ./work/scos_chunks
OUT_ROOT="${2:?out_root required}"           # e.g., ./data/local-cats/_master_optical_parquet_flags/vsx

# Tunables (environment)
THREADS="${THREADS:-1}"                      # keep conservative for VSX
SCAN_GLOB="${SCAN_GLOB:-chunk_*.vot}"       # VOT-only
PAUSE_SECS="${PAUSE_SECS:-1}"               # pause only when we dispatch a job
ONLY_MISSING="${ONLY_MISSING:-1}"           # 1 = skip pre-existing part files
SORT_INPUT="${SORT_INPUT:-1}"               # 1 = process in name order

PARTS_DIR="${OUT_ROOT}/parts"
CANON="${OUT_ROOT}/canonical/flags_vsx_known_variables.parquet"
AUDIT="${OUT_ROOT}/audit/flags_vsx_parts_all.parquet"
LOG_FILE="${OUT_ROOT}/vsx_batch.log"

mkdir -p "${PARTS_DIR}" "$(dirname "${CANON}")" "$(dirname "${AUDIT}")"
touch "${LOG_FILE}"

merge_parts() {
  # merge lock prevents overlaps
  LOCK="${PARTS_DIR}/.merge.lock"
  exec 9>"$LOCK"
  if ! flock -n 9; then
    echo "[merge] another merge in progress; skipping" | tee -a "$LOG_FILE"
    return 0
  fi

  echo "[merge] start $(date -u +%FT%TZ)" | tee -a "$LOG_FILE"

  shopt -s nullglob
  PARTS=("${PARTS_DIR}"/flags_vsx__*.parquet)
  shopt -u nullglob
  if [ ${#PARTS[@]} -eq 0 ]; then
    echo "[merge] no parts; skipping" | tee -a "$LOG_FILE"
    return 0
  fi

  CANON_TMP="${CANON}.tmp"
  AUDIT_TMP="${AUDIT}.tmp"

  duckdb -c "
    INSTALL parquet; LOAD parquet;
    CREATE OR REPLACE VIEW parts AS
      SELECT * FROM read_parquet('${PARTS_DIR}/flags_vsx__*.parquet');
    COPY (
      SELECT NUMBER, TRUE AS is_known_variable_or_transient
      FROM parts WHERE NUMBER IS NOT NULL
      GROUP BY NUMBER
    ) TO '${CANON_TMP}' (FORMAT PARQUET);
    COPY (SELECT * FROM parts) TO '${AUDIT_TMP}' (FORMAT PARQUET);
  " | tee -a "$LOG_FILE"

  mv -f "${CANON_TMP}" "${CANON}"
  mv -f "${AUDIT_TMP}" "${AUDIT}"
  echo "[merge] done $(date -u +%FT%TZ)" | tee -a "$LOG_FILE"
}

# --- build list (fast-resume: filter out completed parts) ---
# gather files
if [ "${SORT_INPUT}" = "1" ]; then
  # shellcheck disable=SC2207
  FILES=($(printf "%s\n" "${CHUNKS_DIR}"/${SCAN_GLOB} | sort))
else
  # shellcheck disable=SC2207
  FILES=($(printf "%s\n" "${CHUNKS_DIR}"/${SCAN_GLOB}))
fi

MISSING=()
for f in "${FILES[@]}"; do
  [ -e "$f" ] || continue
  base="$(basename "$f")"; base="${base%.*}"                # chunk_0000026
  out="${PARTS_DIR}/flags_vsx__${base}.parquet"
  if [ "${ONLY_MISSING}" = "1" ] && [ -s "$out" ]; then
    echo "[skip-fast] ${base} already done: ${out}" | tee -a "$LOG_FILE"
    continue
  fi
  MISSING+=("$f")
done

# --- bounded parallelism; pause only when dispatching a job ---
pids=(); active=0; count=0
for f in "${MISSING[@]}"; do
  while [ "$active" -ge "$THREADS" ]; do
    wait -n || true
    active=$((active-1))
  done
  count=$((count+1))
  echo "[batch] start #${count}: $(basename "$f")" | tee -a "$LOG_FILE"
  ./tools/fetchers/fetch_vsx_stilts_chunked.sh "${f}" "${PARTS_DIR}" &
  pids+=("$!")
  active=$((active+1))
  # Pause only when a job was dispatched
  sleep "${PAUSE_SECS}"
done

# Wait for all workers
wait "${pids[@]}" 2>/dev/null || true

# Final merge
merge_parts

echo "[done] VSX batch completed at $(date -u +%FT%TZ)" | tee -a "$LOG_FILE"
echo "[paths] CANON=${CANON}" | tee -a "$LOG_FILE"
echo "[paths] AUDIT=${AUDIT}" | tee -a "$LOG_FILE"
