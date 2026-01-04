
# NEOWISE Delta Runbook (Fast Path)

This is a compact runbook that complements `WORKFLOW.md` and keeps the README lean.

## 0) Configure Makefile
```make
CHUNK_GLOB := $(POSITIONS_DIR)/new/positions_chunk_*.csv
PARALLEL   := 8
```

## 1) Extract positions (incremental)
```bash
python ./scripts/extract_positions_for_neowise_se.py   --parquet-root ./data/local-cats/_master_optical_parquet   --out-dir      ./data/local-cats/tmp/positions   --chunk-size   20000   --manifest     ./data/local-cats/tmp/positions_manifest.json
```

## 2) Health check (pre-TAP)
```bash
python ./scripts/healthcheck_tap_neowise.py   --positions-dir ./data/local-cats/tmp/positions   --glob 'new/positions_chunk_*.csv'   --out-md  ./data/local-cats/tmp/healthcheck_neowise.md   --out-csv ./data/local-cats/tmp/healthcheck_neowise.csv
```

## 3) Async TAP (idempotent + delta)
```bash
make post15_async_chunks
```

## 4) Health check (post-TAP)
Same command as Step 2. Confirm `COMPLETED` counts.

## 5) Sidecar build (incremental ingest + upsert)
```bash
python ./scripts/concat_flags_and_write_sidecar.py   --closest-dir ./data/local-cats/tmp/positions   --out-root    ./data/local-cats/_master_optical_parquet_irflags   --dataset-name neowise_se   --incremental   --manifest    ./data/local-cats/_master_optical_parquet_irflags/_closest_manifest.json
```

## 6) Global QC summary
```bash
python ./scripts/qc_global_summary.py   ./data/local-cats/_master_optical_parquet_irflags/neowise_se_flags_ALL.parquet   ./data/local-cats/_master_optical_parquet_irflags/neowise_se_global_summary.csv
```

## 7) Full rebuild (only when needed)
```bash
# Force extractor rescan
python ./scripts/extract_positions_for_neowise_se.py   --parquet-root ./data/local-cats/_master_optical_parquet   --out-dir      ./data/local-cats/tmp/positions   --chunk-size   20000   --manifest     ./data/local-cats/tmp/positions_manifest.json   --full-rescan

# TAP async across full rescan set
bash ./scripts/tap_async_batch.sh ./scripts/adql_neowise_se_SIMPLE.sql      './data/local-cats/tmp/positions/new/positions_chunk_*.csv' 8

# Optional: clean sidecar then rebuild without incremental
rm -rf ./data/local-cats/_master_optical_parquet_irflags/sidecar
python ./scripts/concat_flags_and_write_sidecar.py   --closest-dir ./data/local-cats/tmp/positions   --out-root    ./data/local-cats/_master_optical_parquet_irflags   --dataset-name neowise_se
```
