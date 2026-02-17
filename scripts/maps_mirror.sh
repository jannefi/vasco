#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-./data/external/maps}"
BASE_URL="${BASE_URL:-https://aps.umn.edu/MAPS}"
CHECKSUMS_URL="${CHECKSUMS_URL:-$BASE_URL/checksums.txt}"
JOBS="${JOBS:-8}"

mkdir -p "$ROOT"
cd "$ROOT"

echo "[1/4] Fetch checksum manifest: $CHECKSUMS_URL"
curl -fsSLo checksums.txt "$CHECKSUMS_URL"

echo "[2/4] Build URL list from manifest"
# checksums.txt is assumed to be: "<md5>  <filename>"
awk -v base="$BASE_URL" '{print base "/" $2}' checksums.txt > maps_urls.txt

echo "[3/4] Download all files with resume"
aria2c -x "$JOBS" -s "$JOBS" -c -i maps_urls.txt

echo "[4/4] Verify integrity (MD5)"
md5sum -c checksums.txt | tee md5_report.txt

echo "[OK] MAPS mirror complete at: $ROOT"
