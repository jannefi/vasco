#!/usr/bin/env bash
# make_survivors_enriched_duckdb.sh — v2 (majority-tile per NUMBER; memory-safe)
set -euo pipefail
SURV_GLOB="${SURV_GLOB:-./data/vasco-candidates/post16/candidates_final_core_dataset_20260205_170455/ra_bin=*/dec_bin=*/part-*.parquet}"
CHUNKS_GLOB="${CHUNKS_GLOB:-./work/scos_chunks/chunk_*.csv}"
TILE2PLATE="${TILE2PLATE:-./metadata/tiles/tile_to_plate_lookup.parquet}"
PLATEEPOCH="${PLATEEPOCH:-./metadata/plates/plate_epoch_lookup.parquet}"
OUT_DIR="${OUT_DIR:-./work/survivors_enriched}"
OUT_FILE="$OUT_DIR/part-00000.parquet"
mkdir -p "$OUT_DIR"

duckdb -batch <<SQL
PRAGMA threads=4;
SET memory_limit='12GB';
SET temp_directory='/mnt/c/wsltmp/';
SET preserve_insertion_order=false;
INSTALL parquet; LOAD parquet;

-- Survivors: NUMBER + tile_id (no row_id available)
CREATE OR REPLACE VIEW survivors AS
SELECT NUMBER::BIGINT AS NUMBER, tile_id::VARCHAR AS tile_id
FROM parquet_scan('$SURV_GLOB');

-- Majority tile per NUMBER (tie-breaker by lexical order of tile_id)
CREATE OR REPLACE VIEW maj AS
SELECT NUMBER, tile_id FROM (
  SELECT NUMBER, tile_id, COUNT(*) AS cnt,
         ROW_NUMBER() OVER (PARTITION BY NUMBER ORDER BY cnt DESC, tile_id ASC) AS rn
  FROM survivors
  GROUP BY NUMBER, tile_id
) WHERE rn = 1;

-- Chunk map: row_id + NUMBER
CREATE OR REPLACE VIEW chunk_map AS
SELECT CAST(row_id AS VARCHAR) AS row_id,
       CAST(number AS BIGINT)  AS NUMBER
FROM read_csv_auto('$CHUNKS_GLOB');

-- Attach majority tile to every row_id via NUMBER
CREATE OR REPLACE VIEW basic AS
SELECT m.row_id, m.NUMBER, maj.tile_id
FROM chunk_map m
LEFT JOIN maj USING(NUMBER);

-- Plate + epoch lookups
CREATE OR REPLACE VIEW t2p AS SELECT * FROM read_parquet('$TILE2PLATE');  -- expects tile_id, plate_id
CREATE OR REPLACE VIEW pep AS SELECT * FROM read_parquet('$PLATEEPOCH');  -- expects plate_id, epoch_mjd

-- Enriched rows
CREATE OR REPLACE VIEW enriched AS
SELECT b.row_id, b.NUMBER, b.tile_id, t2p.plate_id, pep.date_obs_iso
FROM basic b
LEFT JOIN t2p USING (tile_id)
LEFT JOIN pep USING (plate_id);

COPY (SELECT * FROM enriched) TO '$OUT_FILE' (FORMAT PARQUET);

SELECT COUNT(*) AS enriched_rows,
       COUNT(DISTINCT row_id) AS enriched_distinct_row_id,
       COUNT(DISTINCT NUMBER) AS enriched_distinct_number
FROM read_parquet('$OUT_FILE');
SQL

echo "[OK] wrote $OUT_FILE"
