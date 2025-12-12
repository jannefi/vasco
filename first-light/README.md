# VASCO — First‑light (pilot)

This folder captures a minimal, end‑to‑end **first‑light** result: matching the published **NEOWISE‑only** VASCO catalogue (171,753 rows) against optical detections produced by the PSF‑aware pipeline over a small pilot footprint (\<300 tiles of 30′×30′).

> Goal: Provide visible evidence that the pipeline and comparison workflow work as intended, and that scaling to a larger sky area should reproduce the core results in the MNRAS study.

## Headline numbers (pilot)
- VASCO (NEOWISE‑only) rows: **171,753**
- Optical detections in pilot tiles: **190,969**
- Match radius: **2.0″**
- **Matched** NEOWISE→optical: **69**
- **Still IR‑only** within pilot footprint: **171,684**

These numbers come from `match_summary.txt` in the run output.

## What’s inside
- `plots/` — quick QA plots: distance histogram, residuals, and `ph_qual` distribution.
- `tables/` — `tile_stats.csv` (per‑tile match metrics) and a `vasco_matched_sample.csv` (≤100 rows).
- `scripts/` — `run_first_light.sh` to reproduce the four‑step comparison locally.
- `PROVENANCE.md` — commands, column mapping, and assumptions used for this pilot.

## Early observations
- All matches lie within **≤2.0″** (median ~**1.30″**).
- Residuals (optical − NEOWISE) are centered near zero with ~1″ scatter, consistent with faint matches.
- `ph_qual` is dominated by **B/C/U** flags, as expected near sensitivity limits; stricter quality cuts are possible later.

## Next steps
- Scale to additional tiles and write a cross‑tile master optical catalogue.
- Run the same comparison at **1.0″ / 1.5″ / 2.5″** to quantify robustness vs chance coincidences.
- Add W1–W2 color diagnostics and outlier vetting notebooks.

