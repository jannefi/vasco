#!/usr/bin/env bash
set -euo pipefail

CSV="${1:-plate_tiles.csv}"               # input from the tessellator
DONE="${CSV%.csv}.done"                   # checkpoint file: e.g. plate_tiles.done
RETRY_AFTER="${RETRY_AFTER:-4}"          # reuse your current default

touch "$DONE"

# process every line except header
tail -n +2 "$CSV" | while IFS=, read -r plate ra dec size overlap; do
  line="$plate,$ra,$dec,$size,$overlap"

  if grep -Fxq "$line" "$DONE"; then
    echo "[SKIP] $plate  RA=$ra  Dec=$dec  size=$size"
    continue
  fi

  echo "[RUN ] $plate  RA=$ra  Dec=$dec  size=$size"
  if ./run.sh --one --ra "$ra" --dec "$dec" --size-arcmin "$size" --retry-after "$RETRY_AFTER"; then
    echo "$line" >> "$DONE"
  else
    echo "[FAIL] $line" >&2
    # Do NOT mark as done; you can re-run later
  fi
done
