# Methods (pilot)

1. **Per‑tile unmatched extraction** — Using STILTS `tskymatch2` with `join=1not2` over CDS xmatch outputs to derive GAIA/PS1‑unmatched SExtractor detections.
2. **Run summary** — VS Code‑friendly Markdown & CSV summaries across runs/tiles.
3. **Tile‑level optical catalogue** — Concatenate SExtractor PASS2 per‑image CSVs; deduplicate within **0.5″** cells (lowest `FLAGS`, then brightest `MAG_AUTO`). Optional cross‑tile master via `--write-master`.
4. **NEOWISE→optical comparison** — Read the published NEOWISE‑only CSV; detect RA/Dec columns on both sides; compute nearest neighbor within **2.0″** (one‑to‑one) and write matched and still‑IR‑only tables. If run **without** `--optical-master`, record `opt_source_file` for per‑tile lineage.

**Commands (exact):**
```bash
python ./scripts/filter_unmatched_all.py --data-dir data --backend cds --tol-local 3.0
python ./scripts/summarize_runs.py --data-dir data
python ./scripts/merge_tile_catalogs.py --tiles-root ./data/tiles --tolerance-arcsec 0.5
python ./scripts/compare_vasco_vs_optical.py --vasco data/vasco-svo/vanish_neowise_1765546031.csv --out-dir data
```

**Columns used:** `RA_NEOWISE/DEC_NEOWISE` vs `ALPHA_J2000/DELTA_J2000`.

