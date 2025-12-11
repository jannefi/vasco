
# Step 4 — Cross‑match & Filter (CDS-only backend)

**Purpose:** Cross-match with USNO‑B (I/284) via STILTS; filter unmatched.

## Preconditions
- Step 3 completed and validated
- STILTS available in PATH

## Tasks
- Fetch USNO‑B neighborhood (Astroquery/VizieR) as needed
- STILTS join: `1not2` to split unmatched vs matched (suffix `_cdss`)
- Keep CDS radius = 5"

## Validations (Definition of Done)
- Unmatched set generated with tolerance 0.05"
- Summary CSV/MD produced

## Logs & Artifacts
- `logs/stilts/`
- `data/runs/.../xmatch/`
