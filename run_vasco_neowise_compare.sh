#!/usr/bin/env bash
set -euo pipefail

# --- Configuration (edit if your paths differ) ---
DATA_DIR=${DATA_DIR:-data}
TILES_ROOT=${TILES_ROOT:-$DATA_DIR/tiles}
VASCO_CSV=${VASCO_CSV:-$DATA_DIR/vasco-cats/vanish_neowise_1765546031.csv}
OPTICAL_MASTER=${OPTICAL_MASTER:-$DATA_DIR/local-cats/_master_optical_parquet}
OUT_DIR=${OUT_DIR:-$DATA_DIR}
LOG_DIR=${LOG_DIR:-$DATA_DIR/logs}
mkdir -p "$LOG_DIR"

# --- Preflight checks ---
cmd_exists() { command -v "$1" >/dev/null 2>&1; }

# STILTS is required by filter_unmatched_all.py when generating unmatched tables
if ! cmd_exists stilts; then
  echo "[ERROR] STILTS not found in PATH. Install STILTS (v3.5+), ensure 'stilts' is callable." >&2
  exit 1
fi

# Python 3 check
if ! cmd_exists python3 && ! cmd_exists python; then
  echo "[ERROR] Python is not available." >&2
  exit 1
fi

# Data layout check
if [ ! -d "$DATA_DIR" ]; then
  echo "[ERROR] DATA_DIR not found: $DATA_DIR" >&2
  exit 1
fi
if [ ! -d "$TILES_ROOT" ]; then
  echo "[WARN] TILES_ROOT not found: $TILES_ROOT — merge_tile_catalogs.py will still run if per-image catalogs exist under tiles." >&2
fi

# Inputs for the final comparison
if [ ! -f "$VASCO_CSV" ]; then
  echo "[ERROR] VASCO NEOWISE CSV missing: $VASCO_CSV" >&2
  exit 1
fi
if [ ! -d "$OPTICAL_MASTER" ]; then
  echo "[WARN] OPTICAL master not found at: $OPTICAL_MASTER" >&2
  echo "       compare_vasco_vs_optical.py will fallback to reading per-tile catalogs under $TILES_ROOT." >&2
fi

# --- Run in the requested order ---
{
  echo "[INFO] 1/5 filter_unmatched_all.py (backend=cds, tol-cdss=0.05)"
  python ./scripts/filter_unmatched_all.py --data-dir "$DATA_DIR" --tol-cdss 0.05

  echo "[INFO] 2/5 summarize_runs.py"
  python ./scripts/summarize_runs.py --data-dir "$DATA_DIR"

  echo "[INFO] 3/5 merge_tile_catalogs.py (tolerance 0.5 arcsec)"
  python ./scripts/merge_tile_catalogs.py --tiles-root "$TILES_ROOT" --tolerance-arcsec 0.5 --write-master

  echo "[INFO] 4/5 Convert large csv file to parquet format"
  python ./scripts/make_master_optical_parquet.py --csv data/tiles/_master_tile_catalog_pass2.csv --out data/local-cats/_master_optical_parquet --bin-deg 5 --chunksize 500000
  echo "[INFO] 5/5 compare_vasco_vs_optical.py"
  python ./scripts/compare_vasco_vs_optical.py --vasco "$VASCO_CSV" --radius-arcsec 2.0 --bin-deg 5 --chunk-size 20000 --out-dir data/local-cats/out/v3_match --write-chunks
 
} 2>&1 | tee "$LOG_DIR/vasco_neowise_compare_$(date +%Y%m%d_%H%M%S).log"

echo "[DONE] Outputs should be under: $OUT_DIR"
