#!/usr/bin/env bash
# export_remainder_duckdb.sh
# Builds Inclusive and Core-only R_like remainders from masked-union parquet.
# Writes both Parquet + CSV and (optionally) runs STILTS parity guardedly.

set -euo pipefail

# ---------- CONFIG (override via env) ----------
MU_PARQ="${MU_PARQ:-./work/survivors_masked_union/part-00000.parquet}"

EDGE_CSV="${EDGE_CSV:-./data/metadata/tile_plate_edge_report.csv}"

OUT_ROOT="${OUT_ROOT:-./data/vasco-candidates/post16}"
OUT_INC_PARQ="${OUT_INC_PARQ:-$OUT_ROOT/survivors_R_like_inclusive.provisional.parquet}"
OUT_INC_CSV="${OUT_INC_CSV:-$OUT_ROOT/survivors_R_like_inclusive.provisional.csv}"
OUT_CORE_PARQ="${OUT_CORE_PARQ:-$OUT_ROOT/survivors_R_like_core_only.provisional.parquet}"
OUT_CORE_CSV="${OUT_CORE_CSV:-$OUT_ROOT/survivors_R_like_core_only.provisional.csv}"

RUN_STILTS="${RUN_STILTS:-0}"
# 5399 list
MN_CSV="${MN_CSV:-./data/vasco-cats/vanish_possi_1765561258.csv}"
OUT_PARITY_DIR="${OUT_PARITY_DIR:-./reports}"

# DuckDB pragmas for WSL
THREADS="${THREADS:-4}"
MEM_GB="${MEM_GB:-12GB}"
TMPDIR="${TMPDIR:-/mnt/c/wsltmp/}"

mkdir -p "$OUT_ROOT" "$OUT_PARITY_DIR"

command -v duckdb >/dev/null 2>&1 || { echo "[ERR] duckdb not in PATH"; exit 2; }

# ---------- Build Inclusive + Core-only ----------
duckdb -batch <<SQL
PRAGMA threads=${THREADS};
SET memory_limit='${MEM_GB}';
SET temp_directory='${TMPDIR}';
SET preserve_insertion_order=false;
INSTALL parquet; LOAD parquet;

-- Masked union with flags + coords
CREATE OR REPLACE VIEW mu AS
  SELECT * FROM read_parquet('${MU_PARQ}');

-- Inclusive remainder: pass every gate; SkyBoT strict is optional -> treat missing as pass
CREATE OR REPLACE VIEW r_inclusive AS
SELECT
  row_id, NUMBER, RA, Dec, tile_id, plate_id, date_obs_iso
FROM mu
WHERE NOT has_vosa_like_match
  AND NOT is_supercosmos_artifact
  AND NOT ptf_match_ngood
  AND NOT is_known_variable_or_transient
  AND COALESCE(NOT skybot_strict, TRUE);

-- Edge classification (annotation-only, used for Core-only)
CREATE OR REPLACE VIEW edge AS
SELECT tile_id, plate_id, class_px, class_arcsec
FROM read_csv_auto('${EDGE_CSV}');

-- Core-only: keep rows where (px == 'core' OR arcsec == 'core') (tolerant default)
CREATE OR REPLACE VIEW r_core_only AS
SELECT r.*
FROM r_inclusive r
LEFT JOIN edge e USING (tile_id, plate_id)
WHERE COALESCE(e.class_px, 'core')='core' OR COALESCE(e.class_arcsec, 'core')='core';

-- Write outputs
COPY (SELECT * FROM r_inclusive) TO '${OUT_INC_PARQ}' (FORMAT PARQUET);
COPY (SELECT * FROM r_inclusive) TO '${OUT_INC_CSV}'  (HEADER, DELIMITER ',');
COPY (SELECT * FROM r_core_only) TO '${OUT_CORE_PARQ}' (FORMAT PARQUET);
COPY (SELECT * FROM r_core_only) TO '${OUT_CORE_CSV}'  (HEADER, DELIMITER ',');

-- Final counts
SELECT
  (SELECT COUNT(*) FROM r_inclusive) AS inclusive_rows,
  (SELECT COUNT(*) FROM r_core_only) AS core_only_rows;
SQL

echo "[OK] Remainders written:"
echo "     Inclusive : $OUT_INC_PARQ  |  $OUT_INC_CSV"
echo "     Core-only : $OUT_CORE_PARQ |  $OUT_CORE_CSV"

duckdb -batch <<'SQL'
PRAGMA threads=4;
SET memory_limit='12GB';
SET temp_directory='/mnt/c/wsltmp/';
INSTALL parquet; LOAD parquet;

CREATE OR REPLACE VIEW mu AS SELECT * FROM read_parquet('./work/survivors_masked_union/part-00000.parquet');
SELECT
  COUNT(*) AS masked_rows,
  COUNT(*) FILTER (
    WHERE NOT has_vosa_like_match
      AND NOT is_supercosmos_artifact
      AND NOT ptf_match_ngood
      AND NOT is_known_variable_or_transient
  ) AS remainder_wo_skybot,
  COUNT(*) FILTER (
    WHERE NOT has_vosa_like_match
      AND NOT is_supercosmos_artifact
      AND NOT ptf_match_ngood
      AND NOT is_known_variable_or_transient
      AND COALESCE(NOT skybot_strict, TRUE)
  ) AS remainder_with_skybot
FROM mu;
SQL
# --- STILTS positional parity at 5 arcsec, matched-only with separation ---
# Assumes your existing env from export_remainder_duckdb.sh
#   MN_CSV points to: ./data/vasco-cats/vanish_possi_1765561258.csv
#   OUT_ROOT has survivors_R_like_{inclusive,core_only}.provisional.csv

if (( RUN_STILTS == 1 )); then
  if command -v stilts >/dev/null 2>&1; then
    for view in inclusive core_only; do
      csv="$OUT_ROOT/survivors_R_like_${view}.provisional.csv"
      out_best="$OUT_PARITY_DIR/matched_to_MNRAS_within5_${view}_BEST.csv"   # matched pairs only
      if [ -s "$csv" ] && [ "$(wc -l < "$csv")" -gt 1 ]; then
        # tskymatch2: use RA/Dec from your R_like CSV, and \#RA/DEC from the MNRAS list
        stilts tskymatch2 \
          in1="$csv"  ifmt1=csv  ra1=RA  dec1=Dec \
          in2="$MN_CSV" ifmt2=csv ra2=\#RA dec2=DEC \
          error=5 find=best join=1and2 \
          out="$out_best" ofmt=csv

        echo "[OK] BEST-only parity for ${view} -> $out_best"
      else
        echo "[WARN] $csv empty or header-only; skipping STILTS for ${view}"
      fi
    done
  else
    echo "[WARN] STILTS not in PATH; skipping parity."
  fi
fi
