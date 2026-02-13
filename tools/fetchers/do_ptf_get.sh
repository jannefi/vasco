merge_parts() {
  # --- merge lock to avoid overlapping merges ---
  LOCK="${PARTS_DIR}/.merge.lock"
  exec 9>"$LOCK"
  if ! flock -n 9; then
    echo "[merge] another merge is in progress; skipping" | tee -a "$LOG_FILE"
    return 0
  fi

  echo "[merge] starting at $(date -u +%FT%TZ)" | tee -a "$LOG_FILE"

  # If no parts yet, skip
  shopt -s nullglob
  PART_FILES=("${PARTS_DIR}"/flags_ptf_objects__*.parquet)
  shopt -u nullglob
  if [ ${#PART_FILES[@]} -eq 0 ]; then
    echo "[merge] no part files found under ${PARTS_DIR}, skipping merge." | tee -a "$LOG_FILE"
    return 0
  fi

  # Write to temp files first, then atomic rename
  CANON_TMP="${CANON}.tmp"
  AUDIT_TMP="${AUDIT}.tmp"

  duckdb -c "
    INSTALL parquet; LOAD parquet;
    CREATE OR REPLACE VIEW parts AS
      SELECT * FROM read_parquet('${PARTS_DIR}/flags_ptf_objects__*.parquet');

    COPY (
      SELECT NUMBER, TRUE AS has_other_archive_match
      FROM parts
      WHERE NUMBER IS NOT NULL
      GROUP BY NUMBER
    )
    TO '${CANON_TMP}' (FORMAT PARQUET);

    COPY (SELECT * FROM parts)
    TO '${AUDIT_TMP}' (FORMAT PARQUET);
  " | tee -a "$LOG_FILE"

  mv -f "${CANON_TMP}" "${CANON}"
  mv -f "${AUDIT_TMP}" "${AUDIT}"

  # Only sanity-read canonical if it exists and is non-empty
  if [ -s "${CANON}" ]; then
    duckdb -c "INSTALL parquet; LOAD parquet; SELECT COUNT(*) AS rows FROM read_parquet('${CANON}');" | tee -a "$LOG_FILE"
  else
    echo "[merge] canonical file not present or empty (unexpected)" | tee -a "$LOG_FILE"
  fi

  echo "[merge] finished at $(date -u +%FT%TZ)" | tee -a "$LOG_FILE"
}
