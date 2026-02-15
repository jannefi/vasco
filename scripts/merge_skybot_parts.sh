#!/usr/bin/env bash
# merge_skybot_parts.sh — canonicalize parts -> flags_skybot.parquet
set -euo pipefail
ROOT="${1:-./data/local-cats/_master_optical_parquet_flags/skybot}"
PARTS="${ROOT}/parts/flags_skybot__*.parquet"
OUT="${ROOT}/flags_skybot.parquet"
TMPDIR="${TMPDIR:-/mnt/c/wsltmp/}"

duckdb -batch <<SQL
PRAGMA threads=4;
SET memory_limit='12GB';
SET temp_directory='${TMPDIR}';
INSTALL parquet; LOAD parquet;
CREATE OR REPLACE VIEW parts AS
  SELECT * FROM parquet_scan('${PARTS}');
COPY (
  SELECT row_id,
         MAX(CASE WHEN has_skybot_match  THEN TRUE ELSE FALSE END) AS has_skybot_match,
         MAX(CASE WHEN wide_skybot_match THEN TRUE ELSE FALSE END) AS wide_skybot_match
  FROM parts
  GROUP BY row_id
) TO '${OUT}' (FORMAT PARQUET);
SELECT COUNT(*) AS skybot_row_id, SUM(has_skybot_match)::BIGINT AS strict_true, SUM(wide_skybot_match)::BIGINT AS wide_true
FROM read_parquet('${OUT}');
SQL

echo "[OK] wrote ${OUT}"
