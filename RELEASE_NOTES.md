# VASCO v0.07.1 — work split version

**Highlights**
- All work 6 work steps have been separated
- Randomized run reworked

**Highlights**
- Possibility to get USNO-B data via VizieR I/284 

**Highlights**
- Sub‑second PS1 mean-table fetches using explicit columns + small degree radii.
- Bulletproof Gaia x‑match: STILTS first; if it balks, Astropy fallback finishes the job.
- PS1 x‑match never skipped: CLI recognizes `raMean/decMean` and `RAMean/DecMean`.

**New env knobs**
```bash
# disable USNO-B download via MAST
export VASCO_DISABLE_PS1=1
# disable USNO-B download via Vizier
export VASCO_DISABLE_USNOB=1
# Fast PS1 for dev runs (0.5 arcmin radius)
export VASCO_PS1_RADIUS_DEG=0.00833333
export VASCO_PS1_TIMEOUT=12
export VASCO_PS1_ATTEMPTS=2
```
