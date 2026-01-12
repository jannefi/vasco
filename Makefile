SHELL := /bin/bash

# Paths
PARQUET_ROOT        := ./data/local-cats/_master_optical_parquet
POSITIONS_DIR       := ./data/local-cats/tmp/positions
IRFLAGS_OUT_ROOT    := ./data/local-cats/_master_optical_parquet_irflags

# ADQL (simple)
ADQL_SIMPLE         := ./scripts/adql_neowise_se_SIMPLE.sql

# Parallelism & batch
CHUNK_GLOB          := $(POSITIONS_DIR)/new/positions_chunk_*.csv
PARALLEL            ?= 4
USE_FIFO            ?= 0
VERBOSE_CONSOLE     ?= 0

# Scripts
CSV2VOT             := ./scripts/csv_to_votable_positions.py
ASYNC_ONE           := ./scripts/tap_async_one.sh
ASYNC_BATCH         := ./scripts/tap_async_batch.sh
CLOSEST_ONE         := ./scripts/closest_per_row_id.py
QC_CHUNK            := ./scripts/qc_chunk_summary.py
SIDECAR             := ./scripts/concat_flags_and_write_sidecar.py
QC_GLOBAL           := ./scripts/qc_global_summary.py
POST16              := ./scripts/final_candidates_post16.py
EXPORT_STRICT       := ./scripts/export_masked_view.py

# Parameters
RADIUS_ARCSEC       := 5.0
PY                  ?= python
POST16_RA_COL       ?= ALPHAWIN_J2000
POST16_DEC_COL      ?= DELTAWIN_J2000
POST16_JOIN_KEY     ?= NUMBER
IRFLAGS_PARQUET     ?= $(IRFLAGS_OUT_ROOT)/neowise_se_flags_ALL_NORMALIZED.parquet

.PHONY: help doctor post15_init post15_async_chunks post15_sidecar post15_qc post15_all post16_counts post16_strict

help:
	@echo "Targets: help doctor post15_async_chunks post15_sidecar post15_qc post15_all post16_counts post16_strict"
	@echo "Vars: PARALLEL USE_FIFO VERBOSE_CONSOLE POST16_RA_COL POST16_DEC_COL POST16_JOIN_KEY IRFLAGS_PARQUET"
	@echo "Example: make post15_async_chunks PARALLEL=6"

doctor:
	@echo "[OK] make is working; tabs parsed correctly"

# ---- Post 1.5
post15_init:
	@bash ./scripts/write_adql_simple.sh $(ADQL_SIMPLE) || true

post15_async_chunks: post15_init
	@USE_FIFO=$(USE_FIFO) VERBOSE_CONSOLE=$(VERBOSE_CONSOLE) \
		bash $(ASYNC_BATCH) "$(ADQL_SIMPLE)" "$(CHUNK_GLOB)" "$(PARALLEL)"

post15_sidecar:
	@$(PY) $(SIDECAR) \
		--closest-dir "$(POSITIONS_DIR)" \
		--master-root "$(PARQUET_ROOT)" \
		--out-root "$(IRFLAGS_OUT_ROOT)" \
		--radius-arcsec "$(RADIUS_ARCSEC)"

post15_qc:
	@$(PY) $(QC_GLOBAL) \
		$(IRFLAGS_OUT_ROOT)/neowise_se_flags_ALL.parquet \
		$(IRFLAGS_OUT_ROOT)/neowise_se_global_summary.csv

post15_all: post15_async_chunks post15_sidecar post15_qc
	@echo "[DONE] Post 1.5 finished under $(IRFLAGS_OUT_ROOT)"

# ---- Post 1.6
post16_counts:
	@$(PY) $(POST16) \
		--optical-master-parquet $(PARQUET_ROOT) \
		--irflags-parquet $(IRFLAGS_PARQUET) \
		--annotate-ir \
		--ra-col $(POST16_RA_COL) --dec-col $(POST16_DEC_COL) \
		--join-key $(POST16_JOIN_KEY) \
		--dedupe-tol-arcsec 0.5 \
		--counts-only \
		--out-dir ./data/vasco-candidates/post16

post16_strict:
	@$(PY) $(EXPORT_STRICT) \
		--input-parquet $(PARQUET_ROOT) \
		--irflags-parquet $(IRFLAGS_PARQUET) \
		--join-key $(POST16_JOIN_KEY) \
		--mask "exclude_ir_strict and exclude_hpm and exclude_skybot and exclude_supercosmos" \
		--ra-col $(POST16_RA_COL) --dec-col $(POST16_DEC_COL) \
		--dedupe-tol-arcsec 0.5 \
		--out ./data/vasco-candidates/post16/candidates_final_core.parquet

.PHONY: post16_counts
post16_counts:
	@python ./scripts/final_candidates_post16.py \
		--optical-master-parquet ./data/local-cats/_master_optical_parquet \
		--irflags-parquet       ./data/local-cats/_master_optical_parquet_irflags/neowise_se_flags_ALL.parquet \
		--annotate-ir \
		--counts-only \
		--out-dir ./data/vasco-candidates/post16

.PHONY: post16_strict
post16_strict:
	@python ./scripts/export_masked_view.py \
		--input-parquet  ./data/local-cats/_master_optical_parquet \
		--irflags-parquet ./data/local-cats/_master_optical_parquet_irflags/neowise_se_flags_ALL.parquet \
		--mask "exclude_ir_strict and exclude_hpm and exclude_skybot and exclude_supercosmos" \
		--out  ./data/vasco-candidates/post16/candidates_final_core.parquet
