# WCSFIX (Gaia‑tied polynomial) — canonical coordinates (RA_corr/Dec_corr)

VASCO can optionally run an early WCS correction (“WCSFIX”) step that fits a Gaia‑tied polynomial distortion model for each tile and writes a corrected catalog. The resulting corrected coordinates are stored as RA_corr/Dec_corr and are preferred downstream for crossmatching and within‑radius filtering.

**Note** Use canonical coordinates is strongly recommended. They provide much better matching accuracy even at 2'-3' arcsec, and reduce the likelihood of false-matches

**What it produces**

When successful, per tile:
- catalogs/sextractor_pass2.wcsfix.csv — same rows as the input SExtractor pass‑2 catalog, plus corrected coordinate columns RA_corr and Dec_corr
- catalogs/wcsfix_status.json — status record indicating whether the fit was applied (and why it may have been skipped)

If the fit cannot be performed, the pipeline continues with the original coordinates and records the skip/failure in wcsfix_status.json 

## Prerequisites

WCSFIX is only attempted when the following are true for the tile:

1. SExtractor pass‑2 catalog exists (the pipeline ensures catalogs/sextractor_pass2.csv exists or re‑extracts it from pass2.ldac)
2. A local Gaia neighbourhood cache exists and is non‑empty:
catalogs/gaia_neighbourhood.csv (this is the key input used to tie the solution to Gaia). If it’s missing/empty, WCSFIX is skipped. 
**Note** Check the scripts folder for creating the local cache: e.g. `prewarm_gaia_neighbourhood_bg.sh`, `prewarm_gaia_neighbourhood_cache.py` These are just reference implementations. You can find similar scripts for creating a local cache for PS1 and spikes' removal, see `derive_spike_cache_bg.sh`. Local caching usually requires a lot of disk space, but it will speed up many operations.
3. Sufficient matchability: the fitter requires a minimum number of usable matches (controlled by VASCO_WCSFIX_MIN_MATCHES). If too few matches are available, the fit is skipped and recorded in wcsfix_status.json
4. Tile center is resolvable (helps with RA wrap/stability): the pipeline tries to derive it from RUN_INDEX.json or from the tile folder name tile-RA…-DEC…

**Note**: In intent‑mode LOCAL runs, it’s typical to prewarm Gaia neighbourhood caches first.

## Configuration env variables

These environment variables tune the fit (defaults shown are what the pipeline uses when unset):

* VASCO_WCSFIX_BOOTSTRAP_ARCSEC (default 5.0) — initial bootstrap radius (arcsec) for the Gaia tie
* VASCO_WCSFIX_DEGREE (default 2) — polynomial degree
* VASCO_WCSFIX_MIN_MATCHES (default 20) — minimum matches required to accept/attempt the fit 
* VASCO_WCSFIX_FORCE — if set, forces regeneration of the WCSFIX catalog for the tile

## How it affects downstream steps

* Step4 LOCAL uses the WCSFIX‑augmented catalog when wcsfix_status.json indicates success (otherwise falls back to raw coordinates).
* The within‑radius filter step has been updated to prefer corrected coordinates and to handle PS1 local columns (raMean/decMean + Separation) robustly.
* Guidance based on test results: once WCS‑fixed coordinates are in use, a tighter default counterpart radius (e.g., 3″) becomes viable while keeping 5″ for legacy/compat comparisons


