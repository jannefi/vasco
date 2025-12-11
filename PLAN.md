
# VASCO — Multi‑Step Split & Folder Map (Baseline: `vasco-feature-cds-default-30arcmin`)

> **Baseline going forward:** the contents and conventions of the zip package
> **`vasco-feature-cds-default-30arcmin.zip`** and its CI/workflows are treated
> as the starting point. This plan defines the stepwise split and the directory
> map to align with the current CDS‑only backend and the two‑pass PSF‑aware
> pipeline.

## Goals

1. **Split orchestration into clear, independently runnable steps** with
   reproducible inputs/outputs per step.
2. **Stabilize folder layout** so that configs, data, logs, and results are
   discoverable and CI‑friendly.
3. **Preserve current defaults** (DSS1‑red, 60×60 arcmin tiles to emulate 30′
   radius, CDS radius=5" for matching, hex 30′ tessellation, robust HTTP
   downloader, Markdown+CSV summaries).
4. **Keep PSF‑aware two‑pass processing** (SExtractor → PSFEx → SExtractor),
   with bright‑star masking and STILTS cross‑match (USNO‑B primary; PS1 disabled
   unless explicitly enabled).

## Stepwise Split (Inputs → Outputs → Key tools)

> Each step writes a machine‑readable manifest (`manifest.json`) describing
> inputs, outputs, and parameters. Steps accept a previous step’s manifest.

### Step 0 — Inputs & Tiling
- **Inputs:** RA/Dec list (CSV), field size defaults (60×60 arcmin), tiling mode
  (`hex30`).
- **Outputs:** `data/tiles/<run_id>/tiles.csv` with per‑tile center & bounds.
- **Tools:** Python tiler, run id generator; logs under `logs/`.

### Step 1 — Image Download (DSS1‑red)
- **Inputs:** `tiles.csv` from Step 0.
- **Outputs:** FITS files under `data/downloads/<run_id>/<tile_id>.fits`.
- **Tools:** Robust HTTP downloader (retry/backoff); honors DSS endpoint
  preferences; writes `downloads.manifest.json`.

### Step 2 — Preprocess & Bright‑Star Masks
- **Inputs:** FITS from Step 1.
- **Outputs:** cleaned images, masks under `data/masks/<run_id>/<tile_id>.mask`.
- **Tools:** Python wrapper around masking heuristics & catalogs; configurable.

### Step 3 — SExtractor (Pass 1)
- **Inputs:** cleaned FITS + masks.
- **Outputs:** catalogs under `data/catalogs/p1/<run_id>/<tile_id>.cat`.
- **Tools:** SExtractor (baseline configs), logs in `logs/sextractor-p1/`.

### Step 4 — PSFEx Model
- **Inputs:** `p1` catalogs.
- **Outputs:** PSF models under `data/psf/<run_id>/<tile_id>.psf`.
- **Tools:** PSFEx; config under `config/psfex/`.

### Step 5 — SExtractor (Pass 2, PSF‑aware)
- **Inputs:** cleaned FITS + masks + PSF models.
- **Outputs:** catalogs under `data/catalogs/p2/<run_id>/<tile_id>.cat`.
- **Tools:** SExtractor with PSF; logs `logs/sextractor-p2/`.

### Step 6 — External Catalog Fetch (VizieR / USNO‑B I/284)
- **Inputs:** `p2` catalogs; tile centers.
- **Outputs:** `data/ext/usnob/<run_id>/<tile_id>.csv` (and optional PS1 when
  enabled).
- **Tools:** Astroquery/VizieR; caching under `data/cache/`.

### Step 7 — STILTS Cross‑Match
- **Inputs:** `p2` catalogs + USNO‑B.
- **Outputs:** `data/xmatch/<run_id>/<tile_id>_xmatch.csv` with joins;
  unmatched split (`*_cdss.csv`, `*_local.csv`) using `join=1not2`.
- **Tools:** STILTS `tskymatch2` (arcsec‑first, degrees fallback); radius=5".

### Step 8 — Filtering & Validation
- **Inputs:** xmatch outputs.
- **Outputs:** filtered sets under `data/filter/<run_id>/`; validator reports
  (`within5`, tolerance 0.05").
- **Tools:** Python filters, STILTS helpers; write `filter.manifest.json`.

### Step 9 — Summaries & Dashboard
- **Inputs:** filtered sets + manifests.
- **Outputs:** Markdown + CSV summaries under `data/summaries/<run_id>/` and
  dashboard files (Minimal anomalies CSV) under `dashboard/`.
- **Tools:** `summarize_runs.py` (Markdown+CSV), dashboard writers.

### Step 10 — Packaging & Archival
- **Inputs:** run directory.
- **Outputs:** ZIPs under `artifacts/<run_id>/`, with `.sha256` checksums.
- **Tools:** packaging helper; retain `data/runs/run-YYYYMMDD_HHMMSS` naming.

## Directory Map (Proposed)

```
repo_root/
├─ README.md
├─ PLAN.md                     # this document
├─ mnras-repro.yaml            # reproducibility settings
├─ .github/
│  └─ workflows/
│     └─ ci.yml                # syntax/lint + per-step jobs
├─ config/
│  ├─ pipeline.yaml            # global knobs (survey, radii, toggles)
│  ├─ sextractor/              # *.sex, *.param
│  ├─ psfex/                   # *.psfex configs
│  ├─ stilts/                  # helper STILTS scripts
│  └─ masks/                   # bright-star mask params
├─ docker/
│  └─ Debian/                  # Dockerfiles for Apple Silicon compatible runs
├─ scripts/
│  ├─ cli/                     # user-facing CLIs (one2pass.py, run_random.py)
│  ├─ pipeline/                # orchestrators per step
│  ├─ utils/                   # common helpers (logging, manifests, IO)
│  ├─ validators/              # within5, tolerance checks
│  └─ vizier/                  # USNO-B fetch, optional PS1
├─ workflows/                  # step wrappers, each runnable
│  ├─ 00_inputs_tiling/
│  ├─ 01_download/
│  ├─ 02_preprocess_masks/
│  ├─ 03_sextractor_p1/
│  ├─ 04_psfex/
│  ├─ 05_sextractor_p2/
│  ├─ 06_ext_usnob/
│  ├─ 07_xmatch_stilts/
│  ├─ 08_filter_validate/
│  └─ 09_summarize_dashboard/
├─ data/
│  ├─ runs/                    # run-YYYYMMDD_HHMMSS/ (canonical)
│  ├─ tiles/
│  ├─ downloads/
│  ├─ masks/
│  ├─ catalogs/
│  │  ├─ p1/
│  │  └─ p2/
│  ├─ psf/
│  ├─ ext/
│  │  └─ usnob/
│  ├─ xmatch/
│  ├─ filter/
│  ├─ summaries/
│  └─ cache/
├─ dashboard/
│  └─ anomalies_minimal.csv
├─ artifacts/
│  └─ <run_id>/*.zip
├─ logs/
│  ├─ downloader/
│  ├─ sextractor-p1/
│  ├─ psfex/
│  ├─ sextractor-p2/
│  └─ xmatch/
└─ tests/
   └─ integration/
```

## Orchestration & CLI

- **`scripts/cli/one2pass.py`** — runs Steps 0→9 sequentially with a single
  config file; accepts `--from-step`/`--to-step` for partial runs.
- **`scripts/cli/run_random.py`** — retains existing behavior but delegates to
  per‑step modules.
- **Manifests:** JSON contracts between steps; each step validates presence of
  required keys.

## Configuration & Defaults

- **Survey:** `dss1-red`.
- **Tile size:** 60×60 arcmin square (emulates 30′ radius); tessellation `hex30`.
- **CDS cross‑match radius:** 5"; **validator tolerance:** 0.05".
- **PS1 (MAST):** disabled by default (enable via `config/pipeline.yaml`).
- **Endpoints:** prefer STScI DSS; fallback only when explicitly enabled.

## CI: Expand on existing `.github/workflows/ci.yml`

- **Jobs:** `py-compile`, `shell-scripts` (existing), plus per‑step smoke tests:
  `step00`, `step01`, … `step09` using a small tile at RA=150°, Dec=+20°.
- **Artifacts:** upload per‑step manifests and summary CSVs; fail on missing
  outputs.
- **Matrix:** Apple Silicon compatible Docker builds (Debian base).

## Testing & Quality Gates

- **Integration tests:** under `tests/integration/`, cover the full pass.
- **Dashboards:** assert existence of `dashboard/anomalies_minimal.csv`.
- **Lint:** shellcheck for `tools/**/*.sh`; black/flake8 for Python.

## Roadmap (near‑term)

- Implement bright‑star mask generator (Step 2) and plug into Pass 1.
- Add USNO‑B fetch caching & rate limits; confirm I/284 coverage.
- Finish STILTS wrappers with arcsec/degree fallback logic.
- Harden manifests + checksum generation in Step 10.
- Document CLI examples and run directory conventions.

---
**Notes:** This plan keeps the current CDS‑only backend and the two‑pass PSF
flow, aligns folder names with existing run directory patterns, and prepares for
CI step factoring.

---

## Step Checklists (script‑creates directories, no pre‑scaffolding)

### Global rules (apply to all steps)
- [ ] **Script‑creates directories** if missing (no pre‑scaffolding required).
- [ ] **Write a step manifest** `manifest.json` (inputs, outputs, params, run_id, timings).
- [ ] **Log to step‑specific subfolder** under `logs/` (created automatically).
- [ ] **Emit return code 0** only when all validations pass; otherwise non‑zero with a clear error.
- [ ] **Record metrics**: item counts, elapsed time, retries, warnings.
- [ ] **Honor defaults**: DSS1‑red, 60×60 arcmin tiles, CDS radius 5″, validator tolerance 0.05″.
- [ ] **Respect CI** in baseline zip (extend per‑step smoke jobs on top of existing `py-compile` & `shell-scripts`).

### Step 0 — Inputs & Tiling
**Purpose:** Normalize inputs and produce per‑tile coverage.

**Checklist**
- [ ] **Preconditions:** RA/Dec list available (CSV or CLI args).
- [ ] **Create/verify**: `data/tiles/<run_id>/`.
- [ ] **Run** tiler with `--tile-size 60x60 --mode hex30`.
- [ ] **Output:** `tiles.csv` (tile_id, RA, Dec, bounds).
- [ ] **Manifest fields:** `run_id`, `source_list`, `tile_size`, `tiling_mode`, `tiles_count`.
- [ ] **Validations:** non‑empty `tiles.csv`; RA/Dec in range; unique `tile_id`.
- [ ] **Logs:** `logs/00_inputs_tiling/`.
- [ ] **CI smoke:** generate exactly one tile for RA=150°, Dec=+20° and assert row count = 1.

### Step 1 — Image Download (DSS1‑red)
**Purpose:** Fetch FITS for each tile with robust retry/backoff.

**Checklist**
- [ ] **Preconditions:** `tiles.csv` present and valid.
- [ ] **Create/verify**: `data/downloads/<run_id>/`.
- [ ] **Run** downloader with `--survey dss1-red --retry 5 --backoff 1.5`.
- [ ] **Outputs:** `<tile_id>.fits` per tile; `downloads.manifest.json`.
- [ ] **Manifest fields:** `endpoint`, `survey`, `tiles_processed`, `fits_count`, `retries`.
- [ ] **Validations:** FITS exists and non‑zero; **FITS header has WCS**; basic checksum recorded.
- [ ] **Logs:** `logs/downloader/`.
- [ ] **Failure handling:** mark tile as failed in manifest; continue (unless `--fail-fast`).
- [ ] **CI smoke:** ensure one FITS file appears and header contains `CTYPE1/CTYPE2`.

### Step 2 — Preprocess & Bright‑Star Masks
**Purpose:** Clean image and build masks to suppress saturated/halos.

**Checklist**
- [ ] **Preconditions:** FITS from Step 1.
- [ ] **Create/verify**: `data/masks/<run_id>/`.
- [ ] **Run** preprocessor: cosmetic fixes, background; **mask generator** from catalog.
- [ ] **Outputs:** `<tile_id>.clean.fits`, `<tile_id>.mask`.
- [ ] **Params:** `--mask-threshold`, `--max-radius`, `--catalog` (e.g., Tycho/GAIA if available).
- [ ] **Manifest fields:** `input_fits`, `clean_fits`, `mask_file`, thresholds, counts.
- [ ] **Validations:** mask and clean FITS exist; masked pixels count > 0 when bright stars present.
- [ ] **Logs:** `logs/02_preprocess_masks/`.
- [ ] **CI smoke:** verify clean+mask filenames and non‑empty masks.

### Step 3 — SExtractor (Pass 1)
**Purpose:** Initial detection catalog without PSF.

**Checklist**
- [ ] **Preconditions:** clean FITS + mask.
- [ ] **Create/verify**: `data/catalogs/p1/<run_id>/`.
- [ ] **Run** `sextractor` with baseline `.sex` + `.param` configs (from repo).
- [ ] **Outputs:** `<tile_id>.cat` (Pass 1).
- [ ] **Params:** detection threshold, deblend.
- [ ] **Manifest fields:** `p1_cat`, `objects_detected`, thresholds.
- [ ] **Validations:** catalog exists; **columns present** (e.g., `X_IMAGE`, `Y_IMAGE`, `MAG_AUTO`).
- [ ] **Logs:** `logs/sextractor-p1/`.
- [ ] **CI smoke:** assert catalog row count > 0.

### Step 4 — PSFEx Model
**Purpose:** Build PSF models from Pass‑1 detections.

**Checklist**
- [ ] **Preconditions:** P1 catalog.
- [ ] **Create/verify**: `data/psf/<run_id>/`.
- [ ] **Run** `psfex` with repo configs.
- [ ] **Outputs:** `<tile_id>.psf`.
- [ ] **Manifest fields:** `psf_model`, `stars_used`, `model_quality`.
- [ ] **Validations:** model exists; quality metrics within bounds; star sample size ≥ minimum.
- [ ] **Logs:** `logs/psfex/`.
- [ ] **CI smoke:** confirm PSF file presence and reasonable star count.

### Step 5 — SExtractor (Pass 2, PSF‑aware)
**Purpose:** Final PSF‑aware catalog.

**Checklist**
- [ ] **Preconditions:** clean FITS + mask + PSF model.
- [ ] **Create/verify**: `data/catalogs/p2/<run_id>/`.
- [ ] **Run** `sextractor` with PSF settings referencing Step‑4 model.
- [ ] **Outputs:** `<tile_id>.cat` (Pass 2).
- [ ] **Manifest fields:** `p2_cat`, `psf_model_used`, `objects_detected`.
- [ ] **Validations:** catalog exists; PSF columns present (`FWHM_IMAGE`, `SNR_WIN`, etc.).
- [ ] **Logs:** `logs/sextractor-p2/`.
- [ ] **CI smoke:** row count ≥ P1 (or within configured tolerance).

### Step 6 — External Catalog Fetch (USNO‑B I/284; PS1 optional)
**Purpose:** Pull reference catalogs for cross‑matching.

**Checklist**
- [ ] **Preconditions:** P2 catalog and tile centers.
- [ ] **Create/verify**: `data/ext/usnob/<run_id>/` (and optional `ps1/`).
- [ ] **Run** Astroquery/VizieR for **USNO‑B**; **PS1 disabled by default** per baseline.
- [ ] **Outputs:** `<tile_id>.csv` (USNO‑B).
- [ ] **Manifest fields:** `catalog_name`, `rows_fetched`, `cache_hit`, rate‑limit info.
- [ ] **Validations:** CSV non‑empty; required columns present (ID, RA, Dec, mag).
- [ ] **Logs:** `logs/06_ext_usnob/`.
- [ ] **CI smoke:** fetch with small radius; assert >0 rows or documented cache miss.

### Step 7 — STILTS Cross‑Match
**Purpose:** Match detections to external catalog.

**Checklist**
- [ ] **Preconditions:** P2 catalog + USNO‑B CSV.
- [ ] **Create/verify**: `data/xmatch/<run_id>/`.
- [ ] **Run** `tskymatch2` with **arcsec‑first** (fallback to degrees); **radius = 5″**; write unmatched splits using `join=1not2`.
- [ ] **Outputs:** `<tile_id>_xmatch.csv`, `<tile_id>_cdss.csv`, `<tile_id>_local.csv`.
- [ ] **Manifest fields:** match radius, `matched_count`, `unmatched_cdss`, `unmatched_local`.
- [ ] **Validations:** counts sum correctly; RA/Dec units consistent; no NaN coordinates.
- [ ] **Logs:** `logs/xmatch/`.
- [ ] **CI smoke:** assert non‑empty xmatch file and both split files exist.

### Step 8 — Filtering & Validation
**Purpose:** Apply science filters and unit‑tolerant validation.

**Checklist**
- [ ] **Preconditions:** xmatch outputs.
- [ ] **Create/verify**: `data/filter/<run_id>/`.
- [ ] **Run** filters (magnitude, shape, flags) + **`within5` validator** (0.05″ tolerance).
- [ ] **Outputs:** filtered CSVs + `filter.manifest.json`.
- [ ] **Manifest fields:** filter criteria, `kept_count`, `dropped_count`, validator stats.
- [ ] **Validations:** tolerance < 0.05″ respected; no rows with missing critical fields.
- [ ] **Logs:** `logs/08_filter_validate/`.
- [ ] **CI smoke:** assert filtered file exists and validator summary matches thresholds.

### Step 9 — Summaries & Dashboard
**Purpose:** Produce human‑readable Markdown/CSV and dashboard feeds.

**Checklist**
- [ ] **Preconditions:** filtered sets + manifests.
- [ ] **Create/verify**: `data/summaries/<run_id>/` and `dashboard/`.
- [ ] **Run** `summarize_runs.py` to emit Markdown + CSV; update **Minimal anomalies CSV**.
- [ ] **Outputs:** `summary.md`, `summary.csv`, `dashboard/anomalies_minimal.csv`.
- [ ] **Metrics to include:** `FWHM_IMAGE`, `MAG_AUTO`, `SNR_WIN`, row counts, histograms.
- [ ] **Manifest fields:** `summary_paths`, metric aggregates, plots emitted.
- [ ] **Validations:** files exist and are non‑empty; histogram bins > 0; dashboard CSV schema stable.
- [ ] **Logs:** `logs/09_summarize_dashboard/`.
- [ ] **CI smoke:** assert presence of summary files and dashboard CSV.

### Step 10 — Packaging & Archival
**Purpose:** Zip deliverables and write checksums.

**Checklist**
- [ ] **Preconditions:** full `data/runs/run-YYYYMMDD_HHMMSS/` present.
- [ ] **Create/verify**: `artifacts/<run_id>/`.
- [ ] **Run** packager to zip run outputs; write `.sha256` for each ZIP.
- [ ] **Outputs:** `<run_id>.zip`, `<run_id>.zip.sha256`.
- [ ] **Manifest fields:** archive path(s), size, checksum(s), component counts.
- [ ] **Validations:** checksum matches; zip opens; manifests included.
- [ ] **Logs:** `logs/10_packaging/`.
- [ ] **CI smoke:** upload artifact and validate checksum in workflow.
