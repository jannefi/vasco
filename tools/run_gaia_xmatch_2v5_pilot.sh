#!/usr/bin/env bash
set -euo pipefail

# Run local Gaia xmatch on the WCSFIX pilot tiles and write within2" and within5" outputs.
# Assumes gaia_neighbourhood.csv is already present under each tile's catalogs/.

TILES_ROOT="${1:-./work/wcsfix_pilot_tiles}"

python -u scripts/xmatch_gaia_local_wcsfix_compare.py \
  --tiles-root "$TILES_ROOT" \
  --radii-arcsec 2 5 \
  --out-summary "$TILES_ROOT/GAIA_XMATCH_LOCAL_WCSFIX_COMPARISON.json"

echo "Outputs per tile written under <tile>/xmatch/:"
echo "  - gaia_xmatch_local_wcsfix_nearest.csv"
echo "  - gaia_xmatch_local_wcsfix_within2arcsec.csv"
echo "  - gaia_xmatch_local_wcsfix_within5arcsec.csv"
