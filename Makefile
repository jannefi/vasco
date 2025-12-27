
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

