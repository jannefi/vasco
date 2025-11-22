# Changelog
All notable changes to this project will be documented in this file.

## [0.06.9] - 2025-11-22
### Added
- **USNO B1.0** & related config changes, bright star mask
- **PS1 fetch** env knobs: `VASCO_PS1_RADIUS_DEG`, `VASCO_PS1_TIMEOUT`, `VASCO_PS1_ATTEMPTS`, `VASCO_PS1_COLUMNS`.
- **PS1 disable** toggle via `VASCO_DISABLE_PS1=1` (skips PS1 during dev runs).
- **Gaia fallback** x-match: if STILTS errors, fall back to Astropy spherical match (2") with numeric coercion.

### Changed
- **configs and python code** components: bright_star_mask, stilts_wrapper, testdata
- **PS1 DR2 mean-table** fetch now requests **explicit columns** (includes `raMean,decMean` + compact mags) for fast, predictable responses.
- **Auto-detect RA/Dec** in external CSVs (Gaia & PS1) now covers: 
  `('ra','dec')`, `('RA_ICRS','DE_ICRS')`, `('RAJ2000','DEJ2000')`, `('RA','DEC')`, `('lon','lat')`,
  and **PS1 mean** `('raMean','decMean')` (TAP alias `('RAMean','DecMean')`).
- CLI checker `_csv_has_radec()` recognizes **`raMean/decMean`** and **`RAMean/DecMean`** (PS1 x‑match no longer skipped).

### Fixed
- **Gaia STILTS error** “The name `ra` is unknown.” — robust auto-detection + fallback match.
- **Gaia unit rows** (e.g., `'deg'`) from VizieR TSV are filtered; fallback coerces to numeric to avoid conversion errors.
- Restored `retry-missing` command wiring.
- More robust LDAC→CSV export (Astropy first; STILTS fallback).
- BOM/whitespace header quirks handled in auto-detection.

### Files touched
- `vasco/external_fetch_online.py`
- `vasco/mnras/xmatch_stilts.py`
- `vasco/cli_pipeline.py`


