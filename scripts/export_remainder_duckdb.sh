#!/usr/bin/env bash
# export_remainder_duckdb.sh — v2.2 (no early-exit; coord-safe if RA_row exists)
# Produces remainder exports (Inclusive + Core-only) from masked union using DuckDB.
set -euo pipefail

MASKED_DIR="${MASKED_DIR:-./work/survivors_masked_union}"
EDGE_REPORT_CSV="${EDGE_REPORT_CSV:-./data/metadata/tile_plate_edge_report.csv}"
OUT_DIR="${OUT_DIR:-./data/vasco-candidates/post16}"
DUCKDB_BIN="${DUCKDB_BIN:-duckdb}"

DUCKDB_THREADS="${DUCKDB_THREADS:-10}"
DUCKDB_MEM="${DUCKDB_MEM:-14GB}"
DUCKDB_TEMP="${DUCKDB_TEMP:-${OUT_DIR}/_duckdb_tmp}"

# Stable outputs (no timestamp) to match your note expectations
BASE_INCL="${OUT_DIR}/survivor_set_150_inclusive.provisional"
BASE_CORE="${OUT_DIR}/survivor_set_150_core_only.provisional"

print_help() {
  cat <<'EOF'
export_remainder_duckdb.sh (v2.2)
Flags:
  --masked <dir>       Masked-union parquet directory (default: ./work/survivors_masked_union)
  --edge-report <csv>  Plate-edge CSV (default: ./data/metadata/tile_plate_edge_report.csv)
  --out-dir <dir>      Output directory (default: ./data/vasco-candidates/post16)
  --duckdb <bin>       duckdb binary (default: duckdb)
  --threads <N>        DuckDB threads (default: 10)
  --mem <VAL>          DuckDB memory_limit (default: 14GB)
  --temp-dir <dir>     DuckDB temp_directory (default: <out-dir>/_duckdb_tmp)
EOF
}

while (("$#")); do
  case "$1" in
    --masked) MASKED_DIR="$2"; shift 2;;
    --edge-report) EDGE_REPORT_CSV="$2"; shift 2;;
    --out-dir) OUT_DIR="$2"; shift 2;;
    --duckdb) DUCKDB_BIN="$2"; shift 2;;
    --threads) DUCKDB_THREADS="$2"; shift 2;;
    --mem) DUCKDB_MEM="$2"; shift 2;;
    --temp-dir) DUCKDB_TEMP="$2"; shift 2;;
    --help|-h) print_help; exit 0;;
    *) echo "Unknown arg: $1" >&2; print_help; exit 2;;
  esac
done

mkdir -p "${OUT_DIR}" "${DUCKDB_TEMP}"

if [[ ! -d "${MASKED_DIR}" ]]; then
  echo "[ERR] Masked-union directory not found: ${MASKED_DIR}" >&2
  exit 1
fi
if ! command -v "${DUCKDB_BIN}" >/dev/null 2>&1; then
  echo "[ERR] duckdb binary not found: ${DUCKDB_BIN}" >&2
  exit 1
fi

EDGE_CSV_PRESENT=0
if [[ -f "${EDGE_REPORT_CSV}" ]]; then
  EDGE_CSV_PRESENT=1
else
  echo "[WARN] Edge report CSV not found (${EDGE_REPORT_CSV}); Core-only export will be skipped."
fi

echo "[INFO] Masked-union: ${MASKED_DIR}"
echo "[INFO] Out dir     : ${OUT_DIR}"
echo "[INFO] DuckDB temp : ${DUCKDB_TEMP}"
echo "[INFO] Threads/Mem : ${DUCKDB_THREADS} / ${DUCKDB_MEM}"

SQL_HEADER="$(cat <<EOSQL
PRAGMA threads=${DUCKDB_THREADS};
PRAGMA memory_limit='${DUCKDB_MEM}';
PRAGMA temp_directory='${DUCKDB_TEMP}';

CREATE OR REPLACE VIEW masked AS
  SELECT * FROM parquet_scan('${MASKED_DIR}');

-- Coordinate policy:
-- If RA_row/Dec_row exist (from fixed make_masked_union), export them as RA/Dec.
-- Else fall back to existing RA/Dec columns.
CREATE OR REPLACE VIEW masked_coords AS
SELECT
  row_id,
  NUMBER,
  tile_id,
  plate_id,
  date_obs_iso,
  CASE
    WHEN 'RA_row' IN (SELECT column_name FROM duckdb_columns() WHERE table_name='masked')
    THEN RA_row
    ELSE RA
  END AS RA_out,
  CASE
    WHEN 'Dec_row' IN (SELECT column_name FROM duckdb_columns() WHERE table_name='masked')
    THEN Dec_row
    ELSE Dec
  END AS Dec_out,
  *
EXCLUDE (RA, Dec)
FROM masked;

-- Remainder predicate (SkyBoT-aware). ptf_match_ngood tolerant to bool/int.
CREATE OR REPLACE VIEW remainder_inclusive AS
SELECT
  row_id, NUMBER, RA_out AS RA, Dec_out AS Dec, tile_id, plate_id, date_obs_iso
FROM masked_coords
WHERE
  COALESCE(NOT has_vosa_like_match, TRUE)
  AND COALESCE(NOT is_supercosmos_artifact, TRUE)
  AND COALESCE(CAST(ptf_match_ngood AS INTEGER), 0) = 0
  AND COALESCE(NOT is_known_variable_or_transient, TRUE)
  AND COALESCE(NOT skybot_strict, TRUE);
EOSQL
)"

echo "[STEP] Writing Inclusive remainder → ${BASE_INCL}.csv/.parquet"
"${DUCKDB_BIN}" -c "
${SQL_HEADER}
COPY (SELECT * FROM remainder_inclusive) TO '${BASE_INCL}.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM remainder_inclusive) TO '${BASE_INCL}.csv' (HEADER, DELIMITER ',');
" >/dev/null

echo "[OK] Inclusive: ${BASE_INCL}.parquet"
echo "[OK] Inclusive: ${BASE_INCL}.csv"

if [[ "${EDGE_CSV_PRESENT}" -eq 1 ]]; then
  echo "[STEP] Writing Core-only remainder → ${BASE_CORE}.csv/.parquet"
  "${DUCKDB_BIN}" -c "
  ${SQL_HEADER}
  CREATE OR REPLACE TABLE edge_report AS
  SELECT
    tile_id,
    number,
    (lower(coalesce(class_px,''))='core' OR lower(coalesce(class_arcsec,''))='core') AS is_core
  FROM read_csv_auto('${EDGE_REPORT_CSV}', HEADER=TRUE);

  CREATE OR REPLACE VIEW remainder_core AS
  SELECT r.*
  FROM remainder_inclusive r
  JOIN edge_report e
    ON r.tile_id = e.tile_id
   AND r.NUMBER = e.number
  WHERE e.is_core;

  COPY (SELECT * FROM remainder_core) TO '${BASE_CORE}.parquet' (FORMAT PARQUET);
  COPY (SELECT * FROM remainder_core) TO '${BASE_CORE}.csv' (HEADER, DELIMITER ',');
  " >/dev/null

  echo "[OK] Core-only: ${BASE_CORE}.parquet"
  echo "[OK] Core-only: ${BASE_CORE}.csv"
fi

echo "[DONE] export_remainder_duckdb.sh completed."
