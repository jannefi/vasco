# VASCO v0.07.1 — PSF-aware 2-pass pipeline


Current randomized run test (11-Dec-2025):
- 11/11: split the work into 6 different steps. See [Random run info](README-RUN-RANDOM.md)
  - This is still WIP. Testing, fixing and adding new features when time permits
- Itroduced a working Docker image. You don't need to install anything except Docker. See [this document](/DOCKER_READ.md)
  - docker image contains psfex, sextractor, stilts and all required python modules.
- Start using Stilts CDSSkymatch
  - export VASCO_CDS_GAIA_TABLE="I/350/gaiaedr3"
  - export VASCO_CDS_PS1_TABLE="II/389/ps1_dr2"
- python run_random.py download_loop   --sleep-sec 15 --size-arcmin 30 --survey dss1-red   --pixel-scale 1.7
- python ./scripts/filter_unmatched_all.py (final matching steps - WIP)
- python ./scripts/summarize_runs.py --data-dir ./data - produces markdown and csv report of all runs (WIP)

Added possibility to use Vizier for downloading USNO-B data. 

Changed the downloader logic so that only POSSI-E/POSS-I images are allowed. Other images are dismissed. Example coordinates updated. Fallback to SkyView was removed.

This package lets you run the VASCO two-pass **SExtractor → PSFEx → SExtractor** pipeline on DSS tiles, with a robust downloader (SkyView fallback + STScI DSS), exports (ECSV+CSV/Parquet)

**Key features**
- FITS downloader: **STScI DSS**. (Pixels → Size) with content validation; fallback to **SkyView**. Non-FITS saved as `.html` for inspection. *(SkyView Batch/Query and STScI DSS CGI are documented here:)* [SkyView Batch](https://skyview.gsfc.nasa.gov/current/docs/batchpage.html), [SkyView Query](https://skyview.gsfc.nasa.gov/current/cgi/query.pl), [STScI DSS search](https://stdatu.stsci.edu/dss/script_usage.html).
- **Two-pass PSF-aware** photometry with SExtractor/PSFEx.
- Orchestrator writes `RUN_COUNTS.json`, `RUN_INDEX.json`, **`RUN_OVERVIEW.md`**, and **`RUN_MISSING.json`** (planned/downloaded/processed + missing list).

> **Requires**: `sextractor` (or `sex`), `stilts` and `psfex` on PATH. 
On macOS: `brew install sextractor psfex`. Python deps: `astropy`, `requests`, `numpy`, `matplotlib` (and optionally `pandas`, `pyarrow`).

## Install
Note: it's recommended to use docker for running these scripts

```bash
TODO
python -m py_compile vasco/*.py vasco/utils/*.py vasco/mnras/*.py
```

## Quick start

## run random tile fetcher in loop
```bash
python run_random.py download_loop   --sleep-sec 15 --size-arcmin 30 --survey dss1-red --pixel-scale 1.7
```


### Sexagesimal coordinates

You can pass RA/Dec in **sexagesimal** or **decimal** without manual conversion:

**One tile (smoke test)**
```bash
python -m vasco.cli_pipeline one2pass \
  --ra 150.000 --dec 20.000 \
  --size-arcmin 30 \
  --survey dss1-red \
  --pixel-scale-arcsec 1.7 \
  --workdir data/tiles/tile-smoke-test
```

Outputs appear under `data/tiles/<tileRA-DEC>`.

## Notes
- PS1/MAST calls can take a long time
- Downloader endpoints & parameters per the official docs: [SkyView](https://skyview.gsfc.nasa.gov/current/docs/batchpage.html), [Query form](https://skyview.gsfc.nasa.gov/current/cgi/query.pl), and [STScI DSS CGI](https://stdatu.stsci.edu/dss/script_usage.html). STScI is preferred.
- CSV omits multi-dimensional columns; full fidelity is in **ECSV**.
- Configs in `configs/` are **minimal** and intended as a starting point.
