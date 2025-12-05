# VASCO v0.06.91 — PSF-aware 2-pass pipeline (release 23-Nov-2025)


Current randomized run test (05-Dec-2025):
- Itroduced a working Docker image. You don't need to install anything except Docker. See [this document](/DOCKER_READ.md)
  - docker image contains psfex, sextractor, stilts and all required python modules.
- Start using Stilts CDSSkymatch
  - export VASCO_CDS_GAIA_TABLE="I/350/gaiaedr3"
  - export VASCO_CDS_PS1_TABLE="II/389/ps1_dr2"
- python run_random.py (let it handle at least 100 tiles)
- python ./scripts/filter_unmatched_all.py ./data/runs/ (final matching steps)
- python ./scripts/summarize_runs.py - produces markdown and csv report of all runs

Added possibility to use Vizier for downloading USNO-B data. 

Changed the downloader logic so that only POSSI-E/POSS-I images are allowed. Other images are dismissed. Example coordinates updated. Fallback to SkyView was removed.

This package lets you run the VASCO two-pass **SExtractor → PSFEx → SExtractor** pipeline on DSS tiles, with a robust downloader (SkyView fallback + STScI DSS), exports (ECSV+CSV/Parquet)

**Key features**
- Robust FITS downloader: **STScI DSS**. (Pixels → Size) with content validation; fallback to **SkyView**. Non-FITS saved as `.html` for inspection. *(SkyView Batch/Query and STScI DSS CGI are documented here:)* [SkyView Batch](https://skyview.gsfc.nasa.gov/current/docs/batchpage.html), [SkyView Query](https://skyview.gsfc.nasa.gov/current/cgi/query.pl), [STScI DSS search](https://stdatu.stsci.edu/dss/script_usage.html).
- **Two-pass PSF-aware** photometry with SExtractor/PSFEx.
- Orchestrator writes `RUN_COUNTS.json`, `RUN_INDEX.json`, **`RUN_OVERVIEW.md`**, and **`RUN_MISSING.json`** (planned/downloaded/processed + missing list).

> **Requires**: `sextractor` (or `sex`), `stilts` and `psfex` on PATH. 
On macOS: `brew install sextractor psfex`. Python deps: `astropy`, `requests`, `numpy`, `matplotlib` (and optionally `pandas`, `pyarrow`).

## Install
```bash
TODO
python -m py_compile vasco/*.py vasco/utils/*.py vasco/mnras/*.py
```

## Quick start
**Tessellate examplle. **
```bash
python -m vasco.cli_pipeline tess2pass \
  --center-ra 150.000 --center-dec 20.000 \
  --width-arcmin 120 --height-arcmin 120 \
  --tile-radius-arcmin 30 \
  --overlap-arcmin 0 \
  --size-arcmin 60 \
  --survey poss1-e \
  --pixel-scale-arcsec 1.7 \
  --export csv \
  --hist-col FWHM_IMAGE \
  --workdir data/runs
```

## Check all unmatched data, create *unmatched csv
```bash
python filter_unmatched_all.py data/runs/
```

## Show unmatched data
```bash
python scripts/summarize_xmatch.py data/runs/[runfolder]
```

### Sexagesimal coordinates

You can pass RA/Dec in **sexagesimal** or **decimal** without manual conversion:

**One tile (smoke test)**
```bash
python -m vasco.cli_pipeline one2pass \
  --ra 150.000 --dec 20.000 \
  --size-arcmin 60 \
  --survey poss1-e \
  --pixel-scale-arcsec 1.7 \
  --workdir data/runs
```

Outputs appear under `data/runs/run-YYYYMMDD_HHMMSS/`.

## Notes
- PS1/MAST calls can take a long time
- Downloader endpoints & parameters per the official docs: [SkyView](https://skyview.gsfc.nasa.gov/current/docs/batchpage.html), [Query form](https://skyview.gsfc.nasa.gov/current/cgi/query.pl), and [STScI DSS CGI](https://stdatu.stsci.edu/dss/script_usage.html). STScI is preferred.
- CSV omits multi-dimensional columns; full fidelity is in **ECSV**.
- Configs in `configs/` are **minimal** and intended as a starting point.
