# VASCO v0.9 — NEOWISE version


## Overlap-aware download randomizer (3-Jan-2026)
### How it works
- Treat each 30' tile as a square of **0.5° × 0.5°** on the sky. If you use any other tile size, don't use this feature.
- RA arc separation uses **cos(mean Dec)** scaling; Dec separation is direct.
- Overlap fraction = (overlap_x × overlap_y) / 0.25.
- If any existing tile (same survey/size/pixel) overlaps ≥ `--overlap-max-frac`,
  the randomizer draws a new coordinate. After `--overlap-max-attempts` retries,
  it accepts the candidate to avoid stalling.

Example

```bash
python run-random.py download_loop   --avoid-overlap   --overlap-max-frac 0.5   --overlap-max-attempts 2000   --tiles-root ./data --layout auto   --survey dss1-red --size-arcmin 30 --pixel-scale-arcsec 1.7
```


## Sharded tile tree (tiles_by_sky)

- New layout: `data/tiles_by_sky/ra_bin=*/dec_bin=*/tile-*` with 5° bins by default
- All utilities are layout‑aware and scan both flat and sharded trees
- Significant performance gains observed on NTFS for large (>5k) tile sets
- No configuration switches required; drop‑in replacement via updated `scripts/`

**Highlights**
- NeoWISE stage was added. See the updated [workflow](WORKFLOW.md) for details. 
- Dockerfile was updated accordinly. Remember to rebuild the container before using new features
