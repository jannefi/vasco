# Per‑plate tile coverage overlays

This tool renders **one PNG per DSS plate** containing all tiles that reference it,
using the v4.2 tangent‑plane mapping (no plate WCS required). It also writes an
index CSV with tile counts per plate.

## Usage
```bash
python scripts/render_plate_tile_coverage.py \
  --tiles-root ./data/tiles \
  --dss-headers ./data/dss1red-headers,./data/dss1red_headers \
  --out-dir ./data/metadata/plate_coverage \
  --fast-square --label --max-plates 50
```

- `--fast-square` draws axis‑aligned 30′ squares at the tile sky centers (fast).
  Omit to draw the **exact polygons** by projecting each tile’s corners via tile WCS.
- `--label` adds the tile folder name near its polygon.
- `--max-plates` limits how many per‑plate overlays are rendered in one run.

## Output
- PNGs under `./data/metadata/plate_coverage/` named like `<plate>.png`.
- `plates_with_tiles.csv` summarizing the number of tiles per plate.

## Colors
- **Blue**: core
- **Orange**: near_edge
- **Red**: edge_touch

## Notes
- Classification is recomputed quickly from pixel margin on the per‑plate overlay.
- This uses PLATERA/PLATEDEC + PLTSCALE + X/YPIXELSZ for plate geometry; if you
  need sub‑arcsecond accuracy near plate rims, we can add the plate polynomial later.
