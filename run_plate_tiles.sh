#!/usr/bin/env bash
set -euo pipefail
TILES_CSV="plate_tiles.csv"
echo "Running tiles from $TILES_CSV"
tail -n +2 "$TILES_CSV" | while IFS="," read -r plate ra dec size overlap; do
  echo "==> $plate  RA=$ra  Dec=$dec  size=$size arcmin"
  ./run.sh --one --ra "$ra" --dec "$dec" --size-arcmin "$size" --retry-after 4
done
