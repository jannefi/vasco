# CHANGELOG

## 0.06.8 (2025-11-02)
- Robust downloader (SkyView Pixels→Size; STScI DSS fallback) with gzip handling and FITS validation.
- Two-pass PSF-aware pipeline (SExtractor→PSFEx→SExtractor).
- Orchestrator writes RUN_COUNTS.json, RUN_INDEX.json, RUN_OVERVIEW.md, RUN_MISSING.json.
- Exporter writes full **ECSV** and 1‑D subset **CSV** (Parquet optional).
- `run.sh`: post-run summary, **exit policy**, **--retry-missing**, **--retry-after**.
- Missing tile list added to overview.
