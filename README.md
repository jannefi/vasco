# VASCO v0.06.8 — PSF-aware 2-pass pipeline (release vasco_release_0.06.8_20251102)

This package lets you run the VASCO two-pass **SExtractor → PSFEx → SExtractor** pipeline on DSS tiles, with a robust downloader (SkyView fallback + STScI DSS), exports (ECSV+CSV/Parquet), and automation via `run.sh` (tessellation, post-run summary, and retry logic).

**Key features**
- Robust FITS downloader: **SkyView** (Pixels → Size) with content validation; fallback to **STScI DSS**. Non-FITS saved as `.html` for inspection. *(SkyView Batch/Query and STScI DSS CGI are documented here:)* [SkyView Batch](https://skyview.gsfc.nasa.gov/current/docs/batchpage.html), [SkyView Query](https://skyview.gsfc.nasa.gov/current/cgi/query.pl), [STScI DSS search](https://stdatu.stsci.edu/dss/script_usage.html).
- **Two-pass PSF-aware** photometry with SExtractor/PSFEx.
- Orchestrator writes `RUN_COUNTS.json`, `RUN_INDEX.json`, **`RUN_OVERVIEW.md`**, and **`RUN_MISSING.json`** (planned/downloaded/processed + missing list).
- `run.sh` with **post-run summary**, **exit policy** (configurable), **`--retry-missing`**, and **`--retry-after`** (auto-retry missing right away).

> **Requires**: `sextractor` (or `sex`) and `psfex` on PATH. On macOS: `brew install sextractor psfex`. Python deps: `astropy`, `requests`, `numpy`, `matplotlib` (and optionally `pandas`, `pyarrow`).

## Install
```bash
unzip vasco_release_0.06.8_20251102.zip -d vasco
cd vasco
chmod +x run.sh
python -m py_compile vasco/*.py vasco/utils/*.py
```

## Quick start
**Tessellate 60′×60′ (hex 30′ tiles), DSS1-Red**
```bash
./run.sh --tess   --center-ra 150.1145 --center-dec 2.2050   --width-arcmin 60 --height-arcmin 60   --retry-after 4
```
### Sexagesimal coordinates (convenient wrapper)

You can pass RA/Dec in **sexagesimal** or **decimal** without manual conversion:

```bash
./run_sexagesimal.sh --one \
  --ra "21:02:52.28" \
  --dec "+48:34:18.90" \
  --size-arcmin 60 --retry-after 4
```

**One tile (smoke test)**
```bash
./run.sh --one --ra 150.1145 --dec 2.2050 --size-arcmin 60
```

**Manual retry later**
```bash
./run.sh --retry-missing data/runs/run-YYYYMMDD_HHMMSS
```

Outputs appear under `data/runs/run-YYYYMMDD_HHMMSS/`.

## Notes
- Downloader endpoints & parameters per the official docs: [SkyView](https://skyview.gsfc.nasa.gov/current/docs/batchpage.html), [Query form](https://skyview.gsfc.nasa.gov/current/cgi/query.pl), and [STScI DSS CGI](https://stdatu.stsci.edu/dss/script_usage.html).
- CSV omits multi-dimensional columns; full fidelity is in **ECSV**.
- Configs in `configs/` are **minimal** and intended as a starting point.
