
# VASCO Pipeline: Typical Use Scenario

This document describes the standard workflow for running the VASCO pipeline to reproduce the results of the MNRAS 2022 study, including both the main per-tile processing steps and the post-pipeline aggregation and analysis scripts.


## Environment Variables and Recommended Settings

The VASCO pipeline uses several environment variables to control the behaviour of the CDS/VizieR cross-match backend. These variables affect how the pipeline interacts with external catalogues, manages retries, and handles timeouts.

### Default Environment Settings

You can set the recommended defaults by sourcing the provided script:

```bash
source ./scripts/.env.cds-fast
```

This sets:

```bash
export VASCO_CDS_MODE=single
export VASCO_CDS_MAX_RETRIES=2
export VASCO_CDS_BASE_BACKOFF=1.5
export VASCO_CDS_BLOCKSIZE=omit
export VASCO_CDS_INTER_CHUNK_DELAY=0
export VASCO_CDS_JITTER=0
export VASCO_CDS_PRECALL_SLEEP=0
export VASCO_CDS_GAIA_TABLE="I/350/gaiaedr3"
export VASCO_CDS_PS1_TABLE="II/389/ps1_dr2"
```
If the Vizier CDS service is busy or you experience frequent timeouts, try these more robust settings:
```bash
export VASCO_CDS_MODE=chunked
export VASCO_CDS_CHUNK_ROWS=500   # or 300 if still noisy
export VASCO_CDS_BLOCKSIZE=omit   # or =500 to align with chunks
export VASCO_CDS_MAX_RETRIES=2
export VASCO_CDS_BASE_BACKOFF=2.0
export VASCO_CDS_INTER_CHUNK_DELAY=1.0
export VASCO_CDS_JITTER=1.0
export VASCO_CDS_PRECALL
```

CDS environment value descriptions:
- VASCO_CDS_MODE: Use chunked for more robust, chunked queries if the service is unstable.
- VASCO_CDS_CHUNK_ROWS: Number of rows per chunk (reduce if you still see errors).
- VASCO_CDS_BLOCKSIZE: Set to omit or match the chunk size for best results.
- VASCO_CDS_MAX_RETRIES: Number of retry attempts for failed queries.
- VASCO_CDS_BASE_BACKOFF: Base time (seconds) for exponential backoff between retries.
- VASCO_CDS_INTER_CHUNK_DELAY: Delay (seconds) between chunked queries.
- VASCO_CDS_JITTER: Adds random jitter (seconds) to delays to avoid rate limits.
- VASCO_CDS_PRECALL_SLEEP: Sleep (seconds) before each CDS call.

## 1. Main Pipeline: Per-Tile Processing

The main pipeline is run for each sky tile, typically in batches of hundreds or thousands. Each tile is processed independently through the following steps:

### **Step 1: Download Sky Tile**
- **Command:**  
  `python run-random.py download_loop --size-arcmin 30 --survey dss1-red --pixel-scale 1.7`
- **Purpose:**  
  Downloads a random DSS1-red (POSSI-E) FITS image for a sky tile.
- **Output:**  
  `./data/tiles/<tileid>/raw/<fits_file>.fits` and header JSON.

---

### **Step 2: Source Detection (SExtractor Pass 1)**
- **Command:**  
  `python run-random.py steps --steps 2`
- **Purpose:**  
  Runs SExtractor to detect sources in the FITS image.
- **Output:**  
  `./data/tiles/<tileid>/pass1.ldac`

---

### **Step 3: PSF Modelling & PSF-aware Detection**
- **Command:**  
  `python run-random.py steps --steps 3`
- **Purpose:**  
  Builds a PSF model with PSFEx and reruns SExtractor for improved photometry.
- **Output:**  
  `./data/tiles/<tileid>/pass1.psf`, `pass2.ldac`

---

### **Step 4: Cross-match with Reference Catalogues**
- **Command:**  
  `python run-random.py steps --steps 4 --xmatch-backend cds`
- **Purpose:**  
  Cross-matches detected sources with Gaia EDR3 and PS1 DR2 using CDS/VizieR.
- **Output:**  
  `./data/tiles/<tileid>/xmatch/sex_gaia_xmatch_cdss.csv`, etc.

---

### **Step 5: Filter Matches within 5 arcsec**
- **Command:**  
  `python run-random.py steps --steps 5`
- **Purpose:**  
  Filters cross-match results to those within 5 arcsec and generates unmatched lists.
- **Output:**  
  `./data/tiles/<tileid>/xmatch/sex_gaia_xmatch_cdss_within5arcsec.csv`, etc.

---

### **Step 6: Summarise & Export**
- **Command:**  
  `python run-random.py steps --steps 6`
- **Purpose:**  
  Exports final catalogues, generates QA plots, and writes per-tile summaries.
- **Output:**  
  `./data/tiles/<tileid>/final_catalog.csv`, `RUN_SUMMARY.md`, QA PNGs.

---

## 2. Post-Pipeline: Aggregation & Analysis

After processing a large number of tiles (e.g., 1,000+), run the following scripts to aggregate, filter, and compare results across the entire dataset.

---

### **Post-Pipeline Step 1: Filter Unmatched Sources**
- **Script:**  
  `./scripts/filter_unmatched_all.py --data-dir ./data --tol-cdss 0.05`
- **Purpose:**  
  For each tile, generates lists of unmatched sources for Gaia, PS1, and strict no-optical-counterpart lists.
- **Output:**  
  Per-tile CSVs in `xmatch/` (e.g., `sex_gaia_unmatched_cdss.csv`).

---

### **Post-Pipeline Step 2: Summarise Runs**
- **Script:**  
  `./scripts/summarize_runs.py --data-dir ./data`
- **Purpose:**  
  Aggregates statistics across all tiles, producing Markdown and CSV summaries.
- **Output:**  
  `./data/run_summary.md`, `run_summary.csv`, `run_summary_tiles.csv`, `run_summary_tiles_counts.csv`

---

### **Post-Pipeline Step 3: Merge Tile Catalogues**
- **Script:**  
  `./scripts/merge_tile_catalogs.py --tiles-root ./data/tiles --tolerance-arcsec 0.5 --write-master`
- **Purpose:**  
  Merges all per-tile catalogues into a single, deduplicated master catalogue.
- **Output:**  
  `./data/tiles/_master_tile_catalog_pass2.csv` and related files.

---

### **Post-Pipeline Step 4: Convert Master Catalogue to Parquet (Optional, Per-Catalogue)**
- **Script:**  
  `./scripts/make_master_optical_parquet.py --csv data/tiles/_master_tile_catalog_pass2.csv --out data/local-cats/_master_optical_parquet --bin-deg 5 --chunksize 500000`
- **Purpose:**  
  Converts the master CSV catalogue to a partitioned Parquet dataset for efficient analysis.
- **Output:**  
  Parquet files in `data/local-cats/_master_optical_parquet/`

---

### **Post-Pipeline Step 5: Compare VASCO Results to External Catalogue (Optional, Per-Catalogue)**
- **Script:**  
  `./scripts/compare_vasco_vs_optical.py --vasco ./data/vasco-cats/vanish_neowise_1765546031.csv --radius-arcsec 2.0 --bin-deg 5 --chunk-size 20000 --out-dir data/local-cats/out/v3_match --write-chunks`
- **Purpose:**  
  Compares a science catalogue (e.g., NEOWISE vanishing sources) to the master optical catalogue, finding matches and unmatched sources.
- **Output:**  
  Chunked CSVs and summary in `data/local-cats/out/v3_match/`

---

## 3. Typical Workflow Summary

| Step | Script/Command | Purpose | Key Output(s) | Optional? |
|------|---------------|---------|---------------|-----------|
| 1–6 | `run-random.py steps ...` | Per-tile processing | Per-tile catalogues, matches, summaries | No |
| Post 1 | `filter_unmatched_all.py` | Per-tile unmatched lists | Unmatched CSVs | No |
| Post 2 | `summarize_runs.py` | Aggregate run summary | Markdown/CSV summaries | No |
| Post 3 | `merge_tile_catalogs.py` | Merge/dedupe all tile catalogues | Master CSVs | No |
| Post 4 | `make_master_optical_parquet.py` | Convert master CSV to Parquet | Parquet dataset | Yes |
| Post 5 | `compare_vasco_vs_optical.py` | Compare science catalogue to optical | Match/unmatched CSVs | Yes |

---

## 4. Notes & Best Practices

- Run the main pipeline steps (1–6) for all tiles before starting post-pipeline aggregation.
- Post-pipeline steps 4 and 5 are optional and should be run for each science catalogue as needed.
- All scripts are designed to be idempotent and safe to re-run if new tiles are added.
- For large datasets, use the Parquet format for scalable, efficient analysis- For large datasets, use the Parquet format for scalable, efficient analysis.


## Common Errors and Caveats

The VASCO pipeline is designed to be robust, but due to the nature of random sky sampling and external catalogue coverage, some tiles may encounter errors or be excluded from summaries. Below are the most common scenarios:

---

### 1. Tiles Outside POSS-I Coverage

- **Description:**  
  The pipeline randomly selects sky coordinates, but not all positions are covered by the POSS-I survey (DSS1-red). If a tile falls outside POSS-I coverage, the downloaded FITS file will not have `SURVEY == "POSSI-E"`.

- **Pipeline Behaviour:**  
  - Such tiles are detected and excluded after Step 1.
  - The FITS file and header are kept on disk (`raw/`), but no further processing (steps 2–6) is performed for these tiles.
  - These tiles are not included in run summaries or statistics.

- **How to spot:**  
  - The tile directory exists with only the `raw/` subfolder and possibly log files.
  - No `pass1.ldac` or later outputs are present.

---

### 2. cdsskymatch or Cross-match Errors (RA/Dec Out of Bounds)

- **Description:**  
  Occasionally, the cross-match step (Step 4, using `cdsskymatch`/STILTS) fails if the tile’s RA/Dec is out of bounds for the reference catalogue (e.g., PS1 or Gaia coverage gaps, or malformed coordinates).

- **Pipeline Behaviour:**  
  - If cross-match fails, expected outputs like `sex_gaia_xmatch_cdss.csv` or `sex_ps1_xmatch_cdss.csv` are not created.
  - Downstream steps (5, 6, and post-pipeline scripts) that depend on these files will skip the tile or log a warning.
  - These tiles are effectively excluded from summaries and aggregate statistics.



## Appendix: typical data/tiles/<tileid> folder structure


<details>
<summary>Tree-style (best viewed on GitHub or GFM-compatible viewers)</summary>

```text
data/tiles/<tileid>/
├── raw/
│   ├── <tileid>.fits
│   └── <tileid>.fits.header.json
├── pass1.ldac
├── pass1.psf
├── pass2.ldac
├── sex.out
├── sex.err
├── psfex.out
├── psfex.err
├── catalogs/
│   ├── sextractor_pass1.csv
│   ├── sextractor_pass2.csv
│   ├── tile_catalog_pass2_raw.csv
│   └── tile_catalog_pass2.csv
├── xmatch/
│   ├── sex_gaia_xmatch_cdss.csv
│   ├── sex_ps1_xmatch_cdss.csv
│   ├── sex_gaia_xmatch_cdss_within5arcsec.csv
│   ├── sex_ps1_xmatch_cdss_within5arcsec.csv
│   ├── sex_gaia_unmatched_cdss.csv
│   ├── sex_ps1_unmatched_cdss.csv
│   ├── no_optical_counterparts.csv
│   └── STEP4_CDS.log
├── final_catalog.csv
├── final_catalog.ecsv
├── final_catalog.parquet
├── RUN_SUMMARY.md
├── qa_fwhm_image.png
├── qa_mag_auto_hist.png
├── qa_class_star_hist.png
├── qa_snr_win_hist.png
├── qa_mag_vs_snr.png
├── qa_fwhm_vs_mag.png
├── qa_ellipticity_vs_mag.png
├── qa_class_star_vs_mag.png


 
