
# Step 1 — Image Download (DSS1-red)

**Purpose:** Fetch FITS for each tile with robust retry/backoff.

## Preconditions
- `tiles.csv` present and valid

## Tasks
- Create/verify `data/tiles/<tile_id>/downloads/`
- Run downloader with `--survey dss1-red --retry 5 --backoff 1.5`
- Save `<tile_id>.fits` per tile; write `downloads.manifest.json`
- Record endpoint, survey, retries

## Validations (Definition of Done)
- FITS exists and size > 0
- FITS header contains WCS (`CTYPE1`, `CTYPE2`)
- Basic checksum recorded

## Logs & Artifacts
- Logs under `logs/downloader/<tile_id>/`
- `downloads.manifest.json` lists tiles & checksums
