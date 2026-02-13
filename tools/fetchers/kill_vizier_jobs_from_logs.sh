#!/usr/bin/env bash
set -euo pipefail
LOGROOT=${1:?parts/logs dir required}   # e.g., ./data/local-cats/_master_optical_parquet_flags/vsx/parts/logs
TAP="${TAPURL:-https://tapvizier.cds.unistra.fr/TAPVizieR/tap}"
mapfile -t URLS < <(grep -hEo "https?://[^ ]+/async/[0-9]+" "${LOGROOT}"/vsx__*.log | sort -u)
for u in "${URLS[@]}"; do
  echo "[abort] $u"
  stilts tapresume delete=now joburl="$u" || true
done
