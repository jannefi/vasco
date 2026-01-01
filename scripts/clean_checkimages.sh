#!/usr/bin/env bash
# clean_checkimages.sh
# Deletes SExtractor diagnostic images from all tile folders in flat and sharded layouts.
set -euo pipefail

echo "Searching for diagnostic FITS under ./data/tiles and ./data/tiles_by_sky ..."

flat_root="./data/tiles"
sharded_root="./data/tiles_by_sky"
patterns=( -name 'resi_pass1.fits' -o -name 'chi_pass1.fits' -o -name 'samp_pass1.fits' )

count=0
if [[ -d "$flat_root" ]]; then
  count=$(( count + $(find "$flat_root" -type f \( "${patterns[@]}" \) | wc -l | tr -d ' ') ))
fi
if [[ -d "$sharded_root" ]]; then
  count=$(( count + $(find "$sharded_root" -type f \( "${patterns[@]}" \) | wc -l | tr -d ' ') ))
fi

echo "Found $count files to delete."
if [[ "$count" -eq 0 ]]; then
  echo "No files found. Nothing to do."
  exit 0
fi

echo
echo "Preview of files (flat layout):"
if [[ -d "$flat_root" ]]; then
  find "$flat_root" -type f \( "${patterns[@]}" \) -print || true
fi

echo
echo "Preview of files (sharded layout):"
if [[ -d "$sharded_root" ]]; then
  find "$sharded_root" -type f \( "${patterns[@]}" \) -print || true
fi

echo
echo "To delete these files, uncomment ONE of the lines below."
echo "# find \"$flat_root\"    -type f \\( ${patterns[*]} \\) -delete"
echo "# find \"$sharded_root\" -type f \\( ${patterns[*]} \\) -delete"
