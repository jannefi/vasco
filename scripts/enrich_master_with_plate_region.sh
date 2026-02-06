# ./scripts/enrich_master_with_plate_region.sh
#!/usr/bin/env bash
set -euo pipefail

# Inputs (override via env or on the command line: VAR=value ./script.sh)
MASTER="${MASTER:-./data/local-cats/_master_optical_parquet}"
MAPCSV="${MAPCSV:-./data/metadata/tile_to_dss1red.csv}"   # must have: tile_id, irsa_region
OUT="${OUT:-./data/local-cats/_master_optical_parquet_with_plateid_region}"
TMP="${TMP:-/tmp/vasco_duckdb_tmp}"                        # portable default; point to fast SSD if needed
MEM="${MEM:-8GB}"                                          # DuckDB memory cap
CLEAN="${CLEAN:-0}"                                        # set CLEAN=1 to wipe OUT before writing

mkdir -p "$TMP"

if [[ "$CLEAN" == "1" ]]; then
  echo "[INFO] CLEAN=1 → removing OUT: $OUT"
  rm -rf "$OUT"
fi
mkdir -p "$OUT"

# Quick sanity for mapping CSV (optional, but helpful)
if ! head -n1 "$MAPCSV" | grep -Eiq '(tile_id).*'; then
  echo "[ERROR] Mapping CSV '$MAPCSV' must contain column 'tile_id' (and 'irsa_region')." >&2
  exit 2
fi
if ! head -n1 "$MAPCSV" | grep -Eiq '(irsa_region|REGION|region)'; then
  echo "[ERROR] Mapping CSV '$MAPCSV' must contain column 'irsa_region' (or REGION/region)." >&2
  exit 2
fi

# Walk each (ra_bin, dec_bin) partition of the base master and enrich with plate_id = REGION
find "$MASTER" -type d -path "$MASTER/ra_bin=*/dec_bin=*" | while read -r PARTDIR; do
  echo ">> Processing $PARTDIR"
  duckdb -c "
    INSTALL parquet; LOAD parquet;
    PRAGMA threads=2;
    PRAGMA memory_limit='${MEM}';
    PRAGMA temp_directory='${TMP}';

    -- Mapping: tile_id -> plate_id (plate_id = FITS REGION)
    CREATE OR REPLACE TABLE plate_map AS
    SELECT CAST(tile_id AS VARCHAR)     AS tile_id,
           CAST(irsa_region AS VARCHAR) AS plate_id
    FROM read_csv_auto('${MAPCSV}');

    -- Read this partition of the base master (use writer-agnostic glob)
    CREATE OR REPLACE VIEW opt AS
    SELECT * FROM read_parquet('${PARTDIR}/*.parquet');

    -- Attach the correct plate_id (REGION)
    CREATE OR REPLACE VIEW opt_plus AS
    SELECT o.*, p.plate_id
    FROM opt o LEFT JOIN plate_map p USING (tile_id);

    -- Write to the new enriched tree
    COPY (SELECT * FROM opt_plus)
    TO '${OUT}'
      (FORMAT PARQUET,
       PARTITION_BY (ra_bin, dec_bin),
       COMPRESSION ZSTD,
       OVERWRITE_OR_IGNORE 1);
  "
done

echo "[OK] Wrote: ${OUT}"