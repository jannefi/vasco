#!/usr/bin/env bash
set -euo pipefail

# --- Configuration (edit if your paths differ) ---
DATA_DIR=${DATA_DIR:-data}
TILES_ROOT=${TILES_ROOT:-$DATA_DIR/tiles}
VASCO_CSV=${VASCO_CSV:-$DATA_DIR/vasco-svo/vanish_neowise_1765546031.csv}
OPTICAL_MASTER=${OPTICAL_MASTER:-$DATA_DIR/vasco-svo/_master_tile_catalog_pass2.csv}
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
if [ ! -f "$OPTICAL_MASTER" ]; then
  echo "[WARN] OPTICAL master not found at: $OPTICAL_MASTER" >&2
  echo "       compare_vasco_vs_optical.py will fallback to reading per-tile catalogs under $TILES_ROOT." >&2
fi

# --- Run in the requested order ---
{
  echo "[INFO] 1/4 filter_unmatched_all.py (backend=cds, tol-local=3.0)"
  python ./scripts/filter_unmatched_all.py --data-dir "$DATA_DIR" --backend cds --tol-local 3.0

  echo "[INFO] 2/4 summarize_runs.py"
  python ./scripts/summarize_runs.py --data-dir "$DATA_DIR"

  echo "[INFO] 3/4 merge_tile_catalogs.py (tolerance 0.5 arcsec)"
  python ./scripts/merge_tile_catalogs.py --tiles-root "$TILES_ROOT" --tolerance-arcsec 0.5

  echo "[INFO] 4/4 compare_vasco_vs_optical.py"
  python ./scripts/compare_vasco_vs_optical.py --vasco "$VASCO_CSV" --optical-master "$OPTICAL_MASTER" --out-dir "$OUT_DIR"
} 2>&1 | tee "$LOG_DIR/vasco_neowise_compare_$(date +%Y%m%d_%H%M%S).log"

echo "[DONE] Outputs should be under: $OUT_DIR"
