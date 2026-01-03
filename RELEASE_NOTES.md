# VASCO v0.9 — NEOWISE version


## Post-2026-01-03 guidance for Step 4

**Symptoms:** Some tiles may keep retrying Step 4 (CDS) and never produce xmatch files. 
This appears to affect only small number of tiles usually in larger dataset consisting of several thousands of tiles that have passed at least steps 2,3 and 4

**Root cause:** Older pipeline versions extracted the wrong LDAC HDU
(`in=pass2.ldac+2` instead of `#LDAC_OBJECTS/#N`), creating CSVs without RA/Dec.
CDS then failed, and the runner re-tried Step 4 indefinitely because no xmatch files existed.

**Fix:**
1. Update your repo with `vasco/cli_pipeline.py` dated 2026‑01‑03 (uses `#LDAC_OBJECTS/#N` and writes CDS placeholders).
2. Run `scripts/tile_repair_and_rerun.py --backend cds` to repair CSVs and re-run Step 4→5→6 for *all tiles*. Note try first with `--dry-run`.
3. Or run `scripts/tile_repair_and_rerun.py --backend cds --only-damaged --only-missing --skip-if-step6` to process only tiles whose SExtractor CSV is problematic, run steps 4-6 only if their outputs are missing and keep existing RUN_SUMMARY.md untouched.
4. Or run `scripts/tile_repair_and_rerun.py --report-only` to see the current status of tiles.
5. Inspect `./data/repair_rerun_summary.csv` for per-tile status.

**Notes:**
- PS1 will be auto-skipped by the pipeline for Dec < −30°
- Post‑pipeline steps use the master optical Parquet; they remain **unaffected** by Step‑4 failures. Impact on overall reports and summaries is likely small.


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
