#!/usr/bin/env bash
set -euo pipefail
DATA_DIR=${DATA_DIR:-data}
TILES_ROOT=${TILES_ROOT:-$DATA_DIR/tiles}
VASCO_CSV=${VASCO_CSV:-$DATA_DIR/vasco-svo/vanish_neowise_1765546031.csv}
OUT_DIR=${OUT_DIR:-$DATA_DIR}

# Steps (same order)
python ./scripts/filter_unmatched_all.py --data-dir "$DATA_DIR" --backend cds --tol-local 3.0
python ./scripts/summarize_runs.py --data-dir "$DATA_DIR"
python ./scripts/merge_tile_catalogs.py --tiles-root "$TILES_ROOT" --tolerance-arcsec 0.5
python ./scripts/compare_vasco_vs_optical.py --vasco "$VASCO_CSV" --out-dir "$OUT_DIR"

echo "[first-light] Done. See $OUT_DIR for outputs and ./first-light for QA artifacts."
