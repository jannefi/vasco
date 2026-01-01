# VASCO v0.9 — NEOWISE version

## Sharded tile tree (tiles_by_sky)

- New layout: `data/tiles_by_sky/ra_bin=*/dec_bin=*/tile-*` with 5° bins by default
- All utilities are layout‑aware and scan both flat and sharded trees
- Significant performance gains observed on NTFS for large (>5k) tile sets
- No configuration switches required; drop‑in replacement via updated `scripts/`

**Highlights**
- NeoWISE stage was added. See the updated [workflow](WORKFLOW.md) for details. 
- Dockerfile was updated accordinly. Remember to rebuild the container before using new features
