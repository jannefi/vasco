
#!/usr/bin/env bash
set -euo pipefail
# Usage: tap_async_batch.sh <adql.sql> <chunks_glob> [parallel]
ADQL="$1"
GLOB="$2"
PAR="${3:-8}"

# Sanity
[ -f "$ADQL" ] || { echo "ADQL not found: $ADQL"; exit 1; }

# Expand glob into an array (robust even when no matches)
shopt -s nullglob
files=( $GLOB )
shopt -u nullglob

count=${#files[@]}
echo "[INFO] Batch: pattern='$GLOB'  matches=$count  parallel=$PAR"

if (( count == 0 )); then
  echo "[ERROR] No files match: $GLOB"
  exit 2
fi

# Run one async job per file, with controlled parallelism
printf '%s\0' "${files[@]}" \
| xargs -0 -n1 -P "$PAR" -I{} bash ./scripts/tap_async_one.sh "{}" "$ADQL"
