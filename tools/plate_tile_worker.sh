#!/usr/bin/env bash
set -euo pipefail
# Worker invoked by xargs; marks completion via <DONE_DIR>/<id>.ok

if [[ $# -lt 9 ]]; then
  echo "Usage: plate_tile_worker.sh DONE_DIR RETRY_AFTER RUN_SH plate ra dec size overlap id" >&2
  exit 2
fi

DONE_DIR="$1"; shift
RETRY_AFTER="$1"; shift
RUN_SH="$1"; shift
plate="$1"; ra="$2"; dec="$3"; size="$4"; overlap="$5"; id="$6"

mkdir -p "$DONE_DIR"

echo "[RUN ] $plate  RA=$ra  Dec=$dec  size=$size"
if "$RUN_SH" --one --ra "$ra" --dec "$dec" --size-arcmin "$size" --retry-after "$RETRY_AFTER"; then
  : > "$DONE_DIR/$id.ok"
  echo "[DONE] $plate  id=$id"
else
  echo "[FAIL] $plate  id=$id" >&2
  exit 1
fi
