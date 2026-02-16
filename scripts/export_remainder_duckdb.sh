#!/usr/bin/env bash
# export_remainder_duckdb.sh
# Produce R-like "remainder" exports (Inclusive and Core-only) from the masked-union using DuckDB only.
# Requirements:
#   - DuckDB CLI available as `duckdb`
#   - Masked union parquet directory exists (Hive partitioned is fine)
#   - (Optional) Plate-edge report CSV for Core-only selection
#
# Defaults follow the Post 1.6 runbook:
#   * SkyBoT-aware remainder predicate
#   * PRAGMA threads/memory/spill enabled
#   * Inclusive always, Core-only when edge report CSV is present
#
# Usage examples:
#   ./scripts/export_remainder_duckdb.sh
#   ./scripts/export_remainder_duckdb.sh --masked ./work/survivors_masked_union --out-dir ./out
#   DUCKDB_MEM=12GB DUCKDB_THREADS=10 DUCKDB_TEMP=/mnt/c/wsltmp/vasco_duckdb_tmp ./scripts/export_remainder_duckdb.sh
#
# Exit on error, unset vars, and pipeline failures
set -euo pipefail

# -------------------------
# Defaults (override via flags or env)
# -------------------------
MASKED_DIR="${MASKED_DIR:-./work/survivors_masked_union}"
EDGE_REPORT_CSV="${EDGE_REPORT_CSV:-./data/metadata/tile_plate_edge_report.csv}"
OUT_DIR="${OUT_DIR:-./data/vasco-candidates/post16}"
DUCKDB_BIN="${DUCKDB_BIN:-duckdb}"

# Resource knobs (can be "auto" on some builds; use explicit numbers if your build dislikes "auto")
DUCKDB_THREADS="${DUCKDB_THREADS:-10}"
DUCKDB_MEM="${DUCKDB_MEM:-14GB}"
DUCKDB_TEMP="${DUCKDB_TEMP:-${OUT_DIR}/_duckdb_tmp}"

# Output basename (timestamp for uniqueness)
STAMP="$(date +%Y%m%d_%H%M%S)"
BASE_INCL="${OUT_DIR}/survivors_remainder_inclusive_${STAMP}"
BASE_CORE="${OUT_DIR}/survivors_remainder_core_only_${STAMP}"

# -------------------------
# CLI parsing
# -------------------------
print_help() {
  cat <<'EOF'
export_remainder_duckdb.sh

Flags:
  --masked <dir>          Path to masked-union parquet directory (default: ./work/survivors_masked_union)
  --edge-report <csv>     Path to plate-edge CSV (default: ./data/metadata/tile_plate_edge_report.csv)
  --out-dir <dir>         Output directory (default: ./data/vasco-candidates/post16)
  --duckdb <bin>          duckdb binary (default: duckdb)
  --threads <N>           DuckDB PRAGMA threads (default: 10)
  --mem <VAL>             DuckDB PRAGMA memory_limit (default: 14GB)
  --temp-dir <dir>        DuckDB PRAGMA temp_directory (default: <out-dir>/_duckdb_tmp)
  --help                  This help
Env overrides:
  MASKED_DIR, EDGE_REPORT_CSV, OUT_DIR, DUCKDB_BIN, DUCKDB_THREADS, DUCKDB_MEM, DUCKDB_TEMP
EOF
}

while (( "$#" )); do
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

# Refresh derived paths after possible overrides
mkdir -p "${OUT_DIR}"
DUCKDB_TEMP="${DUCKDB_TEMP:-${OUT_DIR}/_duckdb_tmp}"
mkdir -p "${DUCKDB_TEMP}"

BASE_INCL="${OUT_DIR}/survivors_remainder_inclusive_${STAMP}"
BASE_CORE="${OUT_DIR}/survivors_remainder_core_only_${STAMP}"

# -------------------------
# Validation
# -------------------------
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

# -------------------------
# SQL blocks
# -------------------------

# Canonical remainder predicate (SkyBoT-aware)
# - Booleans guarded with COALESCE(..., FALSE)
# - For ptf_match_ngood treat missing as 0
read -r -d '' SQL_HEADER <<EOSQL
PRAGMA threads=${DUCKDB_THREADS};
PRAGMA memory_limit='${DUCKDB_MEM}';
PRAGMA temp_directory='${DUCKDB_TEMP}';

-- Masked union view (parquet_scan handles Hive partitions)
CREATE OR REPLACE VIEW masked AS
  SELECT * FROM parquet_scan('${MASKED_DIR}');

-- Helper: remainder predicate
CREATE OR REPLACE VIEW remainder_inclusive AS
SELECT *
FROM masked
WHERE
  COALESCE(NOT has_vosa_like_match, TRUE)             -- drop if it *has* a VOSA-like match
  AND COALESCE(NOT is_supercosmos_artifact, TRUE)     -- drop SuperCOSMOS artifacts
  AND COALESCE(ptf_match_ngood, 0)=0                  -- drop if PTF says "good" match(s)
  AND COALESCE(NOT is_known_variable_or_transient, TRUE) -- drop known variables/transients
  AND COALESCE(NOT skybot_strict, TRUE);              -- drop strict SkyBoT matches (keep when absent)
EOSQL

# Date-epoch passthrough note:
# We keep all columns from masked; if `date_obs_iso` exists there, it will be present in outputs.

# -------------------------
# Inclusive export
# -------------------------
echo "[STEP] Writing Inclusive remainder → Parquet + CSV"

"${DUCKDB_BIN}" -c "
${SQL_HEADER}
COPY (SELECT * FROM remainder_inclusive) TO '${BASE_INCL}.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM remainder_inclusive) TO '${BASE_INCL}.csv'     (HEADER, DELIMITER ',');
-- Quick counts
CREATE OR REPLACE TABLE __cnt_incl AS SELECT COUNT(*) AS n FROM remainder_inclusive;
COPY (SELECT 'inclusive' AS kind, n FROM __cnt_incl) TO '${BASE_INCL}.counts.csv' (HEADER, DELIMITER ',');
" >/dev/null

echo "[OK] Inclusive Parquet : ${BASE_INCL}.parquet"
echo "[OK] Inclusive CSV     : ${BASE_INCL}.csv"

# -------------------------
# Core-only export (if edge CSV is present)
# -------------------------
if [[ "${EDGE_CSV_PRESENT}" -eq 1 ]]; then
  echo "[STEP] Writing Core-only remainder (edge-class core) → Parquet + CSV"

  "${DUCKDB_BIN}" -c "
  ${SQL_HEADER}

  -- Edge report CSV (expected columns: tile_id, number, class_px, class_arcsec)
  CREATE OR REPLACE TABLE edge_report AS
    SELECT
      tile_id,
      number,
      class_px,
      class_arcsec,
      (lower(coalesce(class_px,''))='core' OR lower(coalesce(class_arcsec,''))='core') AS is_core
    FROM read_csv_auto('${EDGE_REPORT_CSV}', HEADER=TRUE);

  -- Core-only subset: join on (tile_id, NUMBER == number)
  CREATE OR REPLACE VIEW remainder_core AS
    SELECT r.*
    FROM remainder_inclusive r
    JOIN edge_report e
      ON r.tile_id = e.tile_id
     AND r.NUMBER = e.number
    WHERE e.is_core;

  COPY (SELECT * FROM remainder_core) TO '${BASE_CORE}.parquet' (FORMAT PARQUET);
  COPY (SELECT * FROM remainder_core) TO '${BASE_CORE}.csv'     (HEADER, DELIMITER ',');

  -- Quick counts
  CREATE OR REPLACE TABLE __cnt_core AS SELECT COUNT(*) AS n FROM remainder_core;
  COPY (SELECT 'core_only' AS kind, n FROM __cnt_core) TO '${BASE_CORE}.counts.csv' (HEADER, DELIMITER ',');
  " >/dev/null

  echo "[OK] Core-only Parquet: ${BASE_CORE}.parquet"
  echo "[OK] Core-only CSV    : ${BASE_CORE}.csv"
else
  echo "[SKIP] Core-only export skipped (edge report CSV missing)."
fi

# -------------------------
# Summary
# -------------------------
echo
echo "[DONE] Remainder export finished."
echo "       Inclusive → ${BASE_INCL}.parquet  |  ${BASE_INCL}.csv"
if [[ "${EDGE_CSV_PRESENT}" -eq 1 ]]; then
  echo "       Core-only → ${BASE_CORE}.parquet  |  ${BASE_CORE}.csv"
fi