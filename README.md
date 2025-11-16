# VASCO v0.06.8 — PSF-aware 2-pass pipeline (release 16-Nov-2025)

This package lets you run the VASCO two-pass **SExtractor → PSFEx → SExtractor** pipeline on DSS tiles, with a robust downloader (SkyView fallback + STScI DSS), exports (ECSV+CSV/Parquet), and automation via `run.sh` (tessellation, post-run summary, and retry logic).

**Key features**
- Robust FITS downloader: **STScI DSS**. (Pixels → Size) with content validation; fallback to **SkyView**. Non-FITS saved as `.html` for inspection. *(SkyView Batch/Query and STScI DSS CGI are documented here:)* [SkyView Batch](https://skyview.gsfc.nasa.gov/current/docs/batchpage.html), [SkyView Query](https://skyview.gsfc.nasa.gov/current/cgi/query.pl), [STScI DSS search](https://stdatu.stsci.edu/dss/script_usage.html).
- **Two-pass PSF-aware** photometry with SExtractor/PSFEx.
- Orchestrator writes `RUN_COUNTS.json`, `RUN_INDEX.json`, **`RUN_OVERVIEW.md`**, and **`RUN_MISSING.json`** (planned/downloaded/processed + missing list).
- `run.sh` with **post-run summary**, **exit policy** (configurable), **`--retry-missing`**, and **`--retry-after`** (auto-retry missing right away).

> **Requires**: `sextractor` (or `sex`), stilts and `psfex` on PATH. On macOS: `brew install sextractor psfex`. Python deps: `astropy`, `requests`, `numpy`, `matplotlib` (and optionally `pandas`, `pyarrow`).

## Install
```bash
TODO
python -m py_compile vasco/*.py vasco/utils/*.py vasco/mnras/*.py
```

## Quick start
**Tessellate 60′×60′ (hex 30′ tiles), DSS1-Red**
```bash
python -m vasco.cli_pipeline tess2pass \
  --center-ra 150.0 \
  --center-dec 2.0 \
  --width-arcmin 120 \
  --height-arcmin 120 \
  --tile-radius-arcmin 30 \
  --overlap-arcmin 5 \
  --size-arcmin 60 \
  --survey dss1-red \
  --pixel-scale-arcsec 1.7 \
  --export csv \
  --hist-col FWHM_IMAGE
```
### Sexagesimal coordinates

You can pass RA/Dec in **sexagesimal** or **decimal** without manual conversion:

**One tile (smoke test)**
```bash
python -m vasco.cli_pipeline one2pass   --ra 150.123 --dec 2.345   --size 30 --survey dss1-red --pixel-scale 1.7
```

Outputs appear under `data/runs/run-YYYYMMDD_HHMMSS/`.

## Notes
- Downloader endpoints & parameters per the official docs: [SkyView](https://skyview.gsfc.nasa.gov/current/docs/batchpage.html), [Query form](https://skyview.gsfc.nasa.gov/current/cgi/query.pl), and [STScI DSS CGI](https://stdatu.stsci.edu/dss/script_usage.html). STScI is preferred.
- CSV omits multi-dimensional columns; full fidelity is in **ECSV**.
- Configs in `configs/` are **minimal** and intended as a starting point.
