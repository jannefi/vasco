# VASCO v0.06.9 — PS1+Gaia x‑match hardening + USNO B.10, bright star mask

**Highlights**
- Sub‑second PS1 mean-table fetches using explicit columns + small degree radii.
- Bulletproof Gaia x‑match: STILTS first; if it balks, Astropy fallback finishes the job.
- PS1 x‑match never skipped: CLI recognizes `raMean/decMean` and `RAMean/DecMean`.

**New env knobs**
```bash
# Fast PS1 for dev runs (0.5 arcmin radius)
export VASCO_PS1_RADIUS_DEG=0.00833333
export VASCO_PS1_TIMEOUT=12
export VASCO_PS1_ATTEMPTS=2
# Optional: disable PS1 entirely while iterating
# export VASCO_DISABLE_PS1=1
