#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="${1:-}"  # can be a run dir (with ./tiles) or ./data
if [[ -z "$BASE_DIR" ]] || [[ ! -d "$BASE_DIR" ]]; then
  echo "Usage: $0 <base_dir> (e.g., ./data or data/runs/run-YYYYMMDD_HHMMSS)" >&2
  exit 2
fi

refilter_one() {
  local x="$1"
  local out="${x%.csv}_within5arcsec.csv"
  local cnt
  cnt=$(stilts tpipe in="$x" cmd='select angDist<=5' omode=count || echo 0)
  if [[ "${cnt##* }" -gt 0 ]]; then
    stilts tpipe in="$x" cmd='select angDist<=5' out="$out" ofmt=csv
  else
    stilts tpipe in="$x" cmd='select 3600*angDist<=5' out="$out" ofmt=csv
  fi
  echo "[OK] $out"
}

# flat layout
if [[ -d "$BASE_DIR/tiles" ]]; then
  shopt -s nullglob
  for X in "$BASE_DIR"/tiles/*/xmatch/*_xmatch_cdss.csv; do
    refilter_one "$X"
  done
  shopt -u nullglob
fi

# sharded layout
if [[ -d "$BASE_DIR/tiles_by_sky" ]]; then
  shopt -s nullglob
  for X in "$BASE_DIR"/tiles_by_sky/ra_bin=*/dec_bin=*/tile-*/xmatch/*_xmatch_cdss.csv; do
    refilter_one "$X"
  done
  shopt -u nullglob
fi
