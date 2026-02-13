#!/usr/bin/env bash
# Batch LOCAL VSX flags over VOT/CSV chunks (no network). Fast-resume & merge.
# Usage:
#   ./tools/fetchers/do_vsx_get_local.sh <chunks_dir> <out_root> [radius_arcsec] [vsx_fits]
set -euo pipefail

CHUNKS_DIR="${1:?chunks_dir required}"
OUT_ROOT="${2:?out_root required}"
R_AS="${3:-5}"
VSX_FITS="${4:-./data/local-cats/_ext/vsx/derived/vsx_master_slim.fits}"

THREADS="${THREADS:-1}"
SCAN_GLOB="${SCAN_GLOB:-chunk_*.vot}"   # use 'chunk_*.csv' if needed
ONLY_MISSING="${ONLY_MISSING:-1}"
PAUSE_SECS="${PAUSE_SECS:-0}"

PARTS="${OUT_ROOT}/parts"
CANON="${OUT_ROOT}/canonical/flags_vsx_known_variables.parquet"
AUDIT="${OUT_ROOT}/audit/flags_vsx_parts_all.parquet"
LOG="${OUT_ROOT}/vsx_local_batch.log"
mkdir -p "${PARTS}" "$(dirname "${CANON}")" "$(dirname "${AUDIT}")"
touch "${LOG}"

# Build list, skip-fast
mapfile -t FILES < <(printf "%s\n" "${CHUNKS_DIR}"/${SCAN_GLOB} | sort)
QUEUE=()
for f in "${FILES[@]}"; do
  [ -e "$f" ] || continue
  base="$(basename "$f")"; base="${base%.*}"
  out="${PARTS}/flags_vsx__${base}.parquet"
  if [ "${ONLY_MISSING}" = "1" ] && [ -s "$out" ]; then
    echo "[skip-fast] ${base} already done: ${out}" | tee -a "$LOG"
    continue
  fi
  QUEUE+=("$f")
done

# Run
pids=(); active=0; idx=0
for f in "${QUEUE[@]}"; do
  while [ "$active" -ge "$THREADS" ]; do wait -n || true; active=$((active-1)); done
  idx=$((idx+1)); echo "[batch] start #${idx}: $(basename "$f")" | tee -a "$LOG"
  ./tools/fetchers/fetch_vsx_local_chunked.sh "$f" "${PARTS}" "${R_AS}" "${VSX_FITS}" &
  pids+=("$!"); active=$((active+1))
  sleep "${PAUSE_SECS}"
done
wait "${pids[@]}" 2>/dev/null || true

# Merge to canonical/audit (DuckDB)
LOCK="${PARTS}/.merge.lock"; exec 9>"$LOCK"
if flock -n 9; then
  echo "[merge] start $(date -u +%FT%TZ)" | tee -a "$LOG"
  duckdb -c "
    INSTALL parquet; LOAD parquet;
    CREATE OR REPLACE VIEW parts AS
      SELECT * FROM read_parquet('${PARTS}/flags_vsx__*.parquet');
    COPY (
      SELECT NUMBER, TRUE AS is_known_variable_or_transient
      FROM parts WHERE NUMBER IS NOT NULL GROUP BY NUMBER
    ) TO '${CANON}.tmp' (FORMAT PARQUET);
    COPY (SELECT * FROM parts) TO '${AUDIT}.tmp' (FORMAT PARQUET);
  " | tee -a "$LOG"
  mv -f "${CANON}.tmp" "${CANON}"
  mv -f "${AUDIT}.tmp" "${AUDIT}"
  echo "[merge] done $(date -u +%FT%TZ)" | tee -a "$LOG"
else
  echo "[merge] skipped (lock busy)" | tee -a "$LOG"
fi

echo "[done] LOCAL VSX batch completed" | tee -a "$LOG"
echo "[paths] CANON=${CANON}" | tee -a "$LOG"
echo "[paths] AUDIT=${AUDIT}" | tee -a "$LOG"
