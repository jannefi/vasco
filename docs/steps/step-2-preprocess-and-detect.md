
# Step 2 — Preprocess & Source Detection

**Purpose:** Prepare inputs and run the first SExtractor pass.

## Preconditions
- Step 1 completed and validated
- SExtractor available in PATH

## Tasks
- Normalize headers; ensure WCS is consistent
- Run SExtractor (1st pass) to generate initial catalogs
- Persist runs under `data/runs/run-YYYYMMDD_HHMMSS/`

## Validations (Definition of Done)
- Catalogs exist (`*.cat` / ECSV) and rows > 0
- Key columns present (e.g., `MAG_AUTO`, `FWHM_IMAGE`, `SNR_WIN`)
- Logs exist and contain no ERROR

## Logs & Artifacts
- `logs/sextractor/`
- `data/runs/.../catalogs/`
