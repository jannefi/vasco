#!/usr/bin/env bash
# make_masked_union_duckdb.sh — v3.4 (tile-safe + coord-safe)
# Fix: keep TWO coordinate streams:
#  - RA_row/Dec_row from chunk bridge (per-row sky coords, used for Gaia/PS1 etc.)
#  - RA_num_med/Dec_num_med = median coords per NUMBER (parity/summary only)
# Also: tile_id is ALWAYS extracted from row_id (no majority-tile logic).
set -euo pipefail

# ---- CONFIG (override via env) ----
SURV_GLOB="${SURV_GLOB:-./data/vasco-candidates/post16/candidates_final_core_dataset_20260205_170455/ra_bin=*/dec_bin=*/part-*.parquet}"
CHUNKS_GLOB="${CHUNKS_GLOB:-./work/scos_chunks/chunk_*.csv}"

TILE2PLATE="${TILE2PLATE:-./metadata/tiles/tile_to_plate_lookup.parquet}"     # tile_id, plate_id
PLATEEPOCH="${PLATEEPOCH:-./metadata/plates/plate_epoch_lookup.parquet}"     # plate_id, date_obs_iso

FLAGS_ROOT="${FLAGS_ROOT:-./data/local-cats/_master_optical_parquet_flags}"
VOSA_CANON="${VOSA_CANON:-$FLAGS_ROOT/vosa_like/flags_vosa_like.parquet}"
SCOS_CANON="${SCOS_CANON:-$FLAGS_ROOT/scos_flags.parquet}"
PTF_CANON="${PTF_CANON:-$FLAGS_ROOT/flags_ptf_objects_ngood.parquet}"
VSX_CANON="${VSX_CANON:-$FLAGS_ROOT/vsx_flags.parquet}"
SKYBOT_CANON="${SKYBOT_CANON:-$FLAGS_ROOT/skybot/flags_skybot.parquet}"

OUT_DIR="${OUT_DIR:-./work/survivors_masked_union}"
OUT_FILE="$OUT_DIR/part-00000.parquet"

THREADS="${THREADS:-4}"
MEM_GB="${MEM_GB:-12GB}"
TMPDIR="${TMPDIR:-/mnt/c/wsltmp/}"

mkdir -p "$OUT_DIR"

echo "[paths] SURV_GLOB=$SURV_GLOB"
echo "[paths] CHUNKS_GLOB=$CHUNKS_GLOB"
echo "[paths] OUT_FILE=$OUT_FILE"

duckdb -batch <<SQL
PRAGMA threads=${THREADS};
SET memory_limit='${MEM_GB}';
SET temp_directory='${TMPDIR}';
SET preserve_insertion_order=false;

INSTALL parquet; LOAD parquet;

-- Survivors: keep ALPHAWIN/DELTAWIN in case we want to compare later
CREATE OR REPLACE VIEW surv AS
SELECT
  NUMBER::BIGINT AS NUMBER,
  tile_id::VARCHAR AS tile_id,
  ALPHAWIN_J2000::DOUBLE AS RA_surv,
  DELTAWIN_J2000::DOUBLE AS Dec_surv
FROM parquet_scan('${SURV_GLOB}');

-- Collapsed (parity/summary) positions per NUMBER
CREATE OR REPLACE VIEW pos_num AS
SELECT
  NUMBER,
  median(RA_surv)  AS RA_num_med,
  median(Dec_surv) AS Dec_num_med
FROM surv
GROUP BY NUMBER;

-- Chunk bridge: row_id + NUMBER + per-row coords (this is what external queries should use)
CREATE OR REPLACE VIEW chunk_pos AS
SELECT
  CAST(row_id AS VARCHAR) AS row_id,
  CAST(number AS BIGINT)  AS NUMBER,
  CAST(ra AS DOUBLE)      AS RA_row,
  CAST(dec AS DOUBLE)     AS Dec_row
FROM read_csv_auto('${CHUNKS_GLOB}', HEADER=TRUE);

-- Base: extract tile_id from row_id ONLY, and keep per-row coords from chunk
CREATE OR REPLACE VIEW base AS
SELECT
  c.row_id,
  c.NUMBER,
  SPLIT_PART(c.row_id, ':', 1) AS tile_id,
  c.RA_row,
  c.Dec_row
FROM chunk_pos c;

-- Attach plate metadata via tile_id
CREATE OR REPLACE VIEW base2 AS
SELECT
  b.row_id,
  b.NUMBER,
  b.tile_id,
  t2p.plate_id,
  pep.date_obs_iso,
  b.RA_row,
  b.Dec_row
FROM base b
LEFT JOIN read_parquet('${TILE2PLATE}') t2p USING(tile_id)
LEFT JOIN read_parquet('${PLATEEPOCH}') pep USING(plate_id);

-- Flags
CREATE OR REPLACE VIEW vosa AS
SELECT row_id, has_vosa_like_match
FROM read_parquet('${VOSA_CANON}');

CREATE OR REPLACE VIEW scos AS
SELECT row_id, is_supercosmos_artifact
FROM read_parquet('${SCOS_CANON}');

CREATE OR REPLACE VIEW ptf AS
SELECT NUMBER, ptf_match_ngood
FROM read_parquet('${PTF_CANON}');

CREATE OR REPLACE VIEW vsx AS
SELECT NUMBER, is_known_variable_or_transient
FROM read_parquet('${VSX_CANON}');

CREATE OR REPLACE VIEW sb AS
SELECT
  row_id,
  COALESCE(has_skybot_match, FALSE) AS skybot_strict,
  COALESCE(wide_skybot_match, FALSE) AS skybot_wide
FROM read_parquet('${SKYBOT_CANON}');

-- Final union (1 row per row_id)
CREATE OR REPLACE VIEW union_rows AS
SELECT
  b2.row_id,
  b2.NUMBER,
  b2.tile_id,
  b2.plate_id,
  b2.date_obs_iso,

  -- Canonical per-row coords (what Gaia/PS1 checks must use)
  b2.RA_row,
  b2.Dec_row,

  -- NUMBER-collapsed coords (parity/summary only)
  p.RA_num_med,
  p.Dec_num_med,

  COALESCE(v.has_vosa_like_match, FALSE) AS has_vosa_like_match,
  COALESCE(s.is_supercosmos_artifact, FALSE) AS is_supercosmos_artifact,
  COALESCE(tf.ptf_match_ngood, FALSE) AS ptf_match_ngood,
  COALESCE(x.is_known_variable_or_transient, FALSE) AS is_known_variable_or_transient,
  COALESCE(sb.skybot_strict, FALSE) AS skybot_strict,
  COALESCE(sb.skybot_wide, FALSE) AS skybot_wide
FROM base2 b2
LEFT JOIN vosa v USING(row_id)
LEFT JOIN scos s USING(row_id)
LEFT JOIN ptf tf USING(NUMBER)
LEFT JOIN vsx x USING(NUMBER)
LEFT JOIN pos_num p USING(NUMBER)
LEFT JOIN sb USING(row_id);

COPY (SELECT * FROM union_rows) TO '${OUT_FILE}' (FORMAT PARQUET);

-- Gate tallies
SELECT
  COUNT(*) AS masked_rows,
  SUM(has_vosa_like_match::INT)::BIGINT AS vosa_rows,
  SUM(is_supercosmos_artifact::INT)::BIGINT AS scos_rows,
  SUM(ptf_match_ngood::INT)::BIGINT AS ptf_rows,
  SUM(is_known_variable_or_transient::INT)::BIGINT AS vsx_rows,
  SUM(skybot_strict::INT)::BIGINT AS skybot_strict_rows,
  SUM(skybot_wide::INT)::BIGINT AS skybot_wide_rows,
  SUM( (NOT has_vosa_like_match
        AND NOT is_supercosmos_artifact
        AND NOT ptf_match_ngood
        AND NOT is_known_variable_or_transient)::INT )::BIGINT AS remainder_wo_skybot,
  SUM( (NOT has_vosa_like_match
        AND NOT is_supercosmos_artifact
        AND NOT ptf_match_ngood
        AND NOT is_known_variable_or_transient
        AND NOT skybot_strict)::INT )::BIGINT AS remainder_with_skybot
FROM read_parquet('${OUT_FILE}');
SQL

echo "[OK] wrote ${OUT_FILE}"

