
# Step 3 — PSFEx & PSF-aware Refinement

**Purpose:** Run PSFEx and the second SExtractor pass with PSF enabled.

## Preconditions
- Step 2 completed and validated
- PSFEx available in PATH

## Tasks
- Generate PSF model(s) with PSFEx
- Run SExtractor (2nd pass) with PSF enabled

## Validations (Definition of Done)
- PSF models generated (`*.psf`) and referenced
- Improved star/galaxy separation metrics present

## Logs & Artifacts
- `logs/psfex/<tile_id>/`, `logs/sextractor_psf/<tile_id>/`
- `data/tiles/<tile_id>/psf/`
