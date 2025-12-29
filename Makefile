
# Makefile — Post 1.5 NEOWISE-SE via IRSA TAP (async upload), 8-way parallel

SHELL := /bin/bash

# Paths
PARQUET_ROOT      := ./data/local-cats/_master_optical_parquet
POSITIONS_DIR     := ./data/local-cats/tmp/positions
IRFLAGS_OUT_ROOT  := ./data/local-cats/_master_optical_parquet_irflags

# ADQL (simple, Oracle-friendly)
ADQL_SIMPLE := ./scripts/adql_neowise_se_SIMPLE.sql

# Parallel & batch
CHUNK_GLOB   := $(POSITIONS_DIR)/positions_chunk_*.csv
PARALLEL     := 8

# Scripts
CSV2VOT      := ./scripts/csv_to_votable_positions.py
ASYNC_ONE    := ./scripts/tap_async_one.sh
ASYNC_BATCH  := ./scripts/tap_async_batch.sh
CLOSEST_ONE  := ./scripts/closest_per_row_id.py
QC_CHUNK     := ./scripts/qc_chunk_summary.py
SIDECAR      := ./scripts/concat_flags_and_write_sidecar.py

# Parameters
RADIUS_ARCSEC := 5.0

PY ?= python
NEOWISE_YEARS ?= year8

.PHONY: post15_init
post15_init:
	@bash ./scripts/write_adql_simple.sh $(ADQL_SIMPLE)

.PHONY: post15_async_chunks
post15_async_chunks: post15_init
	@bash $(ASYNC_BATCH) $(ADQL_SIMPLE) "$(CHUNK_GLOB)" $(PARALLEL) 


.PHONY: post15_sidecar
post15_sidecar:
	@python $(SIDECAR) \
	--closest-dir "$(POSITIONS_DIR)" \
	--master-root "$(PARQUET_ROOT)" \
	--out-root "$(IRFLAGS_OUT_ROOT)" \
	--radius-arcsec "$(RADIUS_ARCSEC)"

.PHONY: post15_all
post15_all: post15_async_chunks post15_sidecar
	@echo "[DONE] Post 1.5 NEOWISE-SE via TAP (async), sidecar written to $(IRFLAGS_OUT_ROOT)"


# Makefile additions for Plan A (S3 parquet)
.PHONY: post15_s3_extract_positions post15_s3_pixel post15_s3_year post15_qc

# --- Step 0: positions extraction 
post15_s3_extract_positions:
	$(PY) ./scripts/extract_positions_for_neowise_se.py \
    	--parquet-root ./data/local-cats/_master_optical_parquet \
        --out-dir      ./data/local-cats/tmp/positions \
        --chunk-size   1000


# -- Parallel per pixel across all selected years
post15_s3_pixel:
    $(PY) ./scripts/neowise_s3_sidecar.py \
        --years "$(NEOWISE_YEARS)" \
        --parallel pixel \
        --workers 8 \
        --radius-arcsec 5.0 \
        --clean-tmp

# -- Parallel per year (and optionally per pixel within each year)
post15_s3_year:
    $(PY) ./scripts/neowise_s3_sidecar.py \
        --years "$(NEOWISE_YEARS)" \
        --parallel year \
        --workers 4 \
        --workers-pixel 2 \
        --radius-arcsec 5.0 \
        --clean-tmp

# -- QC-only (re-run summary after a previous sidecar run)
post15_qc:
    $(PY) ./scripts/qc_global_summary.py \
        ./data/local-cats/_master_optical_parquet_irflags/neowise_se_flags_ALL.parquet \
        ./data/local-cats/_master_optical_parquet_irflags/neowise_se_global_summary.csv

