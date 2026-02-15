#!/usr/bin/env bash
# make_masked_union_duckdb.sh — v3.2 (SkyBoT explicit; prints gate tallies)
# Produces ./work/survivors_masked_union/part-00000.parquet with:
# row_id, NUMBER, tile_id, plate_id, date_obs_iso, RA, Dec, all flags (+skybot_*)

set -euo pipefail

# ---- CONFIG (override via env) ----
SURV_GLOB="${SURV_GLOB:-./data/vasco-candidates/post16/candidates_final_core_dataset_20260205_170455/ra_bin=*/dec_bin=*/part-*.parquet}"
CHUNKS_GLOB="${CHUNKS_GLOB:-./work/scos_chunks/chunk_*.csv}"

TILE2PLATE="${TILE2PLATE:-./metadata/tiles/tile_to_plate_lookup.parquet}"   # tile_id, plate_id
PLATEEPOCH="${PLATEEPOCH:-./metadata/plates/plate_epoch_lookup.parquet}"     # plate_id, date_obs_iso

FLAGS_ROOT="${FLAGS_ROOT:-./data/local-cats/_master_optical_parquet_flags}"
VOSA_CANON="${VOSA_CANON:-$FLAGS_ROOT/vosa_like/flags_vosa_like.parquet}"     # row_id
SCOS_CANON="${SCOS_CANON:-$FLAGS_ROOT/scos_flags.parquet}"                     # row_id
PTF_CANON="${PTF_CANON:-$FLAGS_ROOT/flags_ptf_objects_ngood.parquet}"          # NUMBER
VSX_CANON="${VSX_CANON:-$FLAGS_ROOT/vsx_flags.parquet}"                        # NUMBER
SKYBOT_CANON="${SKYBOT_CANON:-$FLAGS_ROOT/skybot/flags_skybot.parquet}"        # row_id (has_skybot_match, wide_skybot_match)

OUT_DIR="${OUT_DIR:-./work/survivors_masked_union}"
OUT_FILE="$OUT_DIR/part-00000.parquet"

# DuckDB pragmas for WSL
THREADS="${THREADS:-4}"
MEM_GB="${MEM_GB:-12GB}"
TMPDIR="${TMPDIR:-/mnt/c/wsltmp/}"

mkdir -p "$OUT_DIR"

echo "[paths] SURV_GLOB=$SURV_GLOB"
echo "[paths] CHUNKS_GLOB=$CHUNKS_GLOB"
echo "[paths] SKYBOT_CANON=$SKYBOT_CANON"
echo "[paths] OUT_FILE=$OUT_FILE"

duckdb -batch <<SQL
PRAGMA threads=${THREADS};
SET memory_limit='${MEM_GB}';
SET temp_directory='${TMPDIR}';
SET preserve_insertion_order=false;
INSTALL parquet; LOAD parquet;

-- Survivors: positions and tile_id per NUMBER
CREATE OR REPLACE VIEW surv AS
SELECT
  NUMBER::BIGINT         AS NUMBER,
  tile_id::VARCHAR       AS tile_id,
  ALPHAWIN_J2000::DOUBLE AS RA,
  DELTAWIN_J2000::DOUBLE AS Dec
FROM parquet_scan('${SURV_GLOB}');

-- Majority tile per NUMBER (stable & memory-safe)
CREATE OR REPLACE VIEW maj_tile AS
SELECT NUMBER, tile_id FROM (
  SELECT NUMBER, tile_id, COUNT(*) AS cnt,
         ROW_NUMBER() OVER (PARTITION BY NUMBER ORDER BY cnt DESC, tile_id ASC) AS rn
  FROM surv
  GROUP BY NUMBER, tile_id
) WHERE rn = 1;

-- Collapsed positions per NUMBER (median RA/Dec)
CREATE OR REPLACE VIEW pos_num AS
SELECT NUMBER, median(RA) AS RA, median(Dec) AS Dec
FROM surv
GROUP BY NUMBER;

-- Chunk bridge: row_id + NUMBER
CREATE OR REPLACE VIEW chunk_map AS
SELECT CAST(row_id AS VARCHAR) AS row_id,
       CAST(number AS BIGINT)  AS NUMBER
FROM read_csv_auto('${CHUNKS_GLOB}');

-- Base: attach majority tile + plate + epoch (date_obs_iso)
CREATE OR REPLACE VIEW base AS
SELECT
  m.row_id,
  m.NUMBER,
  mt.tile_id,
  t2p.plate_id,
  pep.date_obs_iso
FROM chunk_map m
LEFT JOIN maj_tile mt USING(NUMBER)
LEFT JOIN read_parquet('${TILE2PLATE}') t2p USING(tile_id)
LEFT JOIN read_parquet('${PLATEEPOCH}') pep USING(plate_id);

-- Flags
CREATE OR REPLACE VIEW vosa AS SELECT row_id, has_vosa_like_match
  FROM read_parquet('${VOSA_CANON}');
CREATE OR REPLACE VIEW scos AS SELECT row_id, is_supercosmos_artifact
  FROM read_parquet('${SCOS_CANON}');
CREATE OR REPLACE VIEW ptf  AS SELECT NUMBER, ptf_match_ngood
  FROM read_parquet('${PTF_CANON}');
CREATE OR REPLACE VIEW vsx  AS SELECT NUMBER, is_known_variable_or_transient
  FROM read_parquet('${VSX_CANON}');

-- SkyBoT canonical (if present). We still create a view; if file missing DuckDB will error
CREATE OR REPLACE VIEW sb AS
SELECT row_id,
       COALESCE(has_skybot_match,  FALSE) AS skybot_strict,
       COALESCE(wide_skybot_match, FALSE) AS skybot_wide
FROM read_parquet('${SKYBOT_CANON}');

-- Final union (one row per row_id)
CREATE OR REPLACE VIEW union_rows AS
SELECT
  b.row_id,
  b.NUMBER,
  b.tile_id,
  b.plate_id,
  b.date_obs_iso,
  p.RA, p.Dec,
  COALESCE(v.has_vosa_like_match, FALSE)            AS has_vosa_like_match,
  COALESCE(s.is_supercosmos_artifact, FALSE)        AS is_supercosmos_artifact,
  COALESCE(tf.ptf_match_ngood, FALSE)               AS ptf_match_ngood,
  COALESCE(x.is_known_variable_or_transient, FALSE) AS is_known_variable_or_transient,
  COALESCE(sb.skybot_strict, FALSE)                 AS skybot_strict,
  COALESCE(sb.skybot_wide,   FALSE)                 AS skybot_wide
FROM base b
LEFT JOIN vosa v USING(row_id)
LEFT JOIN scos s USING(row_id)
LEFT JOIN ptf  tf USING(NUMBER)
LEFT JOIN vsx  x USING(NUMBER)
LEFT JOIN pos_num p USING(NUMBER)
LEFT JOIN sb USING(row_id);

COPY (SELECT * FROM union_rows) TO '${OUT_FILE}' (FORMAT PARQUET);

-- Gate tallies (so you SEE SkyBoT presence right here)
SELECT
  COUNT(*)                                                     AS masked_rows,
  SUM(has_vosa_like_match::INT)::BIGINT                        AS vosa_rows,
  SUM(is_supercosmos_artifact::INT)::BIGINT                    AS scos_rows,
  SUM(ptf_match_ngood::INT)::BIGINT                            AS ptf_rows,
  SUM(is_known_variable_or_transient::INT)::BIGINT             AS vsx_rows,
  SUM(skybot_strict::INT)::BIGINT                              AS skybot_strict_rows,
  SUM(skybot_wide::INT)::BIGINT                                AS skybot_wide_rows,
  SUM( (NOT has_vosa_like_match
        AND NOT is_supercosmos_artifact
        AND NOT ptf_match_ngood
        AND NOT is_known_variable_or_transient)::INT )::BIGINT AS remainder_wo_skybot,
  SUM( (NOT has_vosa_like_match
        AND NOT is_supercosmos_artifact
        AND NOT ptf_match_ngood
        AND NOT is_known_variable_or_transient
        AND NOT skybot_strict)::INT )::BIGINT                  AS remainder_with_skybot
FROM read_parquet('${OUT_FILE}');
SQL

echo "[OK] wrote ${OUT_FILE}"
