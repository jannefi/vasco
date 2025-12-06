#!/usr/bin/env bash
set -euo pipefail
RUN_DIR="${1:-}"
if [ -z "$RUN_DIR" ] || [ ! -d "$RUN_DIR" ]; then
  echo "Usage: $0 <run_dir> (e.g., data/runs/run-20251206_101010)" >&2
  exit 2
fi
shopt -s nullglob
for X in "$RUN_DIR"/tiles/*/xmatch/*_xmatch_cdss.csv; do
  OUT="${X%.csv}_within5arcsec.csv"
  CNT=$(stilts tpipe in="$X" cmd='select angDist<=5' omode=count || echo 0)
  if [ "${CNT##* }" -gt 0 ]; then
    stilts tpipe in="$X" cmd='select angDist<=5' out="$OUT" ofmt=csv
  else
    stilts tpipe in="$X" cmd='select 3600*angDist<=5' out="$OUT" ofmt=csv
  fi
  echo "[OK] $OUT"
done
