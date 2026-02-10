#!/usr/bin/env bash
set -euo pipefail

# --- config ---------------------------------------------------------------
ROOT="$(realpath ./data/local-cats)"                       # repo-local root
IRDIR="$ROOT/_master_optical_parquet_irflags"              # where IR flag files live
TS="$(date +%Y%m%d-%H%M%S)"
TRASH="$ROOT/_trash/$TS"                                   # quarantine target
DRY_RUN="${DRY_RUN:-0}"                                    # set to 1 for dry-run
# -------------------------------------------------------------------------

mkdir -p "$TRASH"
shopt -s nullglob        # empty globs expand to nothing (no literal patterns)

echo "[info] ROOT=$ROOT"
echo "[info] IRDIR=$IRDIR"
echo "[info] TRASH=$TRASH"
[[ "$DRY_RUN" == "1" ]] && echo "[info] DRY-RUN enabled; no files will be moved."

cd "$IRDIR"

move_pattern () {
  local pattern="$1"
  local moved=0
  for f in $pattern; do
    [[ -e "$f" ]] || continue
    if [[ "$DRY_RUN" == "1" ]]; then
      echo "[dry] mv -- '$f' '$TRASH/'"
    else
      mv -v -- "$f" "$TRASH/"
    fi
    moved=1
  done
  [[ $moved -eq 1 ]] || echo "[skip] no match: $pattern"
}

# Staging / backups / non-canonical variants (keeps canonical by-tile file untouched)
move_pattern 'tmp_neowise_*.duckdb'
move_pattern 'neowise_se_flags_ALL.parquet.BAK.*'
move_pattern 'neowise_se_flags_ALL.parquet.bak.*'
move_pattern 'neowise_se_flags_ALL_FIXED.parquet'
move_pattern 'neowise_se_flags_ALL_by_NUMBER.parquet'

echo "[ok] candidates processed. Listing quarantine:"
ls -lah "$TRASH" || true

