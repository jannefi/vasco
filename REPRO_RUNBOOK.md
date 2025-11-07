
# REPRO_RUNBOOK — MNRAS 2022 (POSS-I Red) Section 2→3

This runbook executes the paper’s Section 2→3 logic on an existing run directory.
It works on **partial runs** (only tiles with catalogs are processed).

## Prereqs
- `astropy`, `astroquery`, `pyvo` (optional), internet access for PS1/GAIA
- Your `vasco` package with:
  - `vasco/mnras/spikes.py` (spike cuts)
  - the files in this PR (`filters_mnras.py`, `xmatch.py`, `hpm.py`, `buckets.py`, `report.py`, `cli_repro.py`)

## Config
`mnras-repro.yaml` encodes:
- **5" cross-match** to Pan-STARRS DR2 (MAST) and Gaia EDR3 (TAP)
- Spike cuts: `m <= 12.4` and `m <= -0.09*d_arcmin + 15.3`
- Morphology: `SPREAD_MODEL > -0.002`, `2*FWHM < 7`, `ELONGATION < 1.3`

## Run (example)
```bash
python -m vasco.cli_repro   --run-dir data/runs/run-20251107_140835   --yaml mnras-repro.yaml
```

Outputs in the run root:
- `REPRO_SUMMARY.json` — machine readable
- `REPRO_SUMMARY.md` — human friendly table

## Notes
- Pan-STARRS DR2 (MAST catalogs API, cone & cross-match) — scripted use supported.
- Gaia EDR3 via TAP/ADQL — cone queries at 5" and expanded 30" for the HPM phase.
- “SuperCOSMOS artifacts” bucket is represented here by morphology+spikes; we label the limitation.

