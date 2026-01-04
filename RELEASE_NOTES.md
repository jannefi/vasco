# VASCO v0.9.2 — MNRAS spike boundary & morphology docs (2026-01-04)

## Summary
Small but impactful alignment with the MNRAS spike-removal procedure and a
workflow documentation fix to ensure SExtractor emits all columns required by
the morphology filters.

### Highlights
- **Spike line-rule boundary**: switch comparator to strict `<` so equality keeps;
  constant-rule equality still rejects.
- **SExtractor .param doc**: document `XMIN_IMAGE, XMAX_IMAGE, YMIN_IMAGE, YMAX_IMAGE`
  plus required keys; restate `FLAGS==0 & SNR_WIN>30` extraction screen in the workflow. 

### Impact
- Fixes the failing unit test at the 30″ boundary (`rmag=12.60` keeps).
- Prevents silent bypass of extent-based morphology checks by ensuring the four
  geometry columns are present in CSVs.

### Upgrade Notes
1. Pull latest software version (all files from the repository) - rebuild your docker image if you use it
2. To fully adopt the new early‑stage filters & morphology: remove various files from steps 2-6, and re-run all steps: `python run-random.py steps --steps 2,3,4,5,6 --layout auto --tiles-root ./data --xmatch-backend cds`. 
I made a helper script that can be used to remove the existing step files: `refresh_configs_and_outputs.py`. It assumes new sharded folder structure.
Dry-run: 
    ```bash
    python scripts/refresh_configs_and_outputs.py --tiles-root ./data \
    --remove-pass1 --remove-pass2 --remove-xmatch --dry-run
    ```
    Execute:
    ```bash
    python scripts/refresh_configs_and_outputs.py --tiles-root ./data \
    --remove-pass1 --remove-pass2 --remove-xmatch
    ```
    After that resume:
    ```bash
    python run-random.py steps --steps 2,3,4,5,6 --layout auto --tiles-root ./data --xmatch-backend cds
    ```

    ***Note*** This can take a long time depending on the size of your dataset.

3. After steps 2-6 are done for all tiles, you should run all post-processing steps. See the workflow documentation


## SExtractor .param requirements & extraction-time filters

### Summary
Document the four geometry columns required by the MNRAS morphology screen and
restate extraction-time filters so generated CSVs are analysis-ready.

### What changed
- Add an explicit SExtractor `.param` snippet listing **XMIN_IMAGE, XMAX_IMAGE,
  YMIN_IMAGE, YMAX_IMAGE** along with other required fields:
  ```text
  # sextractor.param (minimum for VASCO/MNRAS workflow)
  XMIN_IMAGE
  XMAX_IMAGE
  YMIN_IMAGE
  YMAX_IMAGE
  SPREAD_MODEL
  FWHM_IMAGE
  ELONGATION
  FLAGS
  SNR_WIN
  ALPHA_J2000
  DELTA_J2000
  ```

## spikes.py — Line-rule boundary handling (MNRAS spikes)

### Summary
Adjust the diffraction-spike line rule to use **strict `<`** at the boundary,
so equality on the Rmag–distance line is kept (not rejected). This matches
our paper-aligned convention and the unit test expectations.

### What changed
- Line rule comparator:
  - **Before:** `if m_near <= thresh: reject`
  - **After:**  `if m_near <  thresh: reject`
- Constant rule comparator remains unchanged: `m_near <= const_max_mag` (equality still rejects).

### Rationale
The MNRAS spike removal rule is defined as `Rmag ≤ a * d_arcsec + b` with
`a = -0.09`, `b = 15.3`. We adopt a conservative convention to **keep equality**
on the line rule to avoid over-rejecting true sources exactly on the boundary.
(At `d = 30"`, threshold is **12.6**.)  [MNRAS 2022, Sec. 2(iv)]  (paper basis)  🔗
``Rmag ≤ −0.09·d + 15.3``. 

### Impact
- Fixes the failing unit test:  
  `spike: 30" & rmag=12.60 -> KEEP (line rule)` now passes.
- No change to constant rule behavior:  
  `spike: 5" & rmag=12.40 -> REJECT (const rule)` remains correct.

## VASCO v0.9.1 — NEOWISE Delta/Idempotent TAP & Healthcheck (2026‑01‑04)

**Summary.** This release introduces a faster, incremental NEOWISE step (1.5) with idempotent TAP async execution, partition‑aware sidecar upserts, and a health checker for async job monitoring & recovery. It reduces repeat work, speeds up large runs (>5k tiles), and adds guardrails to avoid lost or duplicated TAP jobs.

### Highlights
- **Delta mode (Step 1.5):** Only **new/changed** optical partitions are extracted and sent to TAP.
- **Idempotent async TAP:** Batch skips chunks that already produced `*_closest.csv`. Per‑chunk runner resumes an existing async job via `*_tap.meta.json` instead of resubmitting.
- **Sidecar upserts:** IR flags now upsert into partitioned Parquet (`ra_bin/dec_bin`), then rebuild a clean global flags Parquet view.
- **Health checker:** Lists NEW,IN_FLIGHT,PARTIAL,NEED_RESUBMIT,COMPLETED chunks and prints safe remediation commands.

### Makefile change (delta fast path)
```make
# Use new-only chunks written by incremental extractor
CHUNK_GLOB := $(POSITIONS_DIR)/new/positions_chunk_*.csv
PARALLEL   := 8
```

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
