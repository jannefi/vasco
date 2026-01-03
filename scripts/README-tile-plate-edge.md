# Tile vs Plate Edge (v4.2)

**Why this release:** DSS full-plate headers don't carry a standard TAN WCS
(CD/CDELT/CROTA). v4.2 removes the plate-WCS dependency and uses:

- `PLATERA`, `PLATEDEC` — plate sky center (deg, ICRS)
- `PLTSCALE` — arcsec per **mm**
- `XPIXELSZ`, `YPIXELSZ` — pixel size in **µm**

to compute a **gnomonic mapping** from sky to plate pixels. Tile WCS still comes
from the tile's JSON (`header{}` or `selected{}`), which *does* have a TAN WCS.

## Quick start
```bash
# CSV only (RA/Dec-aware; no plate WCS needed)
python scripts/check_tile_plate_edge.py \
  --tiles-root ./data/tiles \
  --out-csv ./data/metadata/tile_plate_edge_report.csv

# CSV + overlays for flagged tiles (sharded output)
python scripts/check_tile_plate_edge.py --plot --plot-dir ./data/metadata/edge_plots_by_sky

# Optional: fast square overlays instead of exact polygon
python scripts/check_tile_plate_edge.py --plot --fast-square
```

## Notes
- Arcsec/px is computed as `PLTSCALE * (PIXELSZ/1000)` separately for X and Y.
- Polynomials (AMDX*, AMDY*, etc.) aren't applied; for "edge proximity" this
  is generally sufficient. If you later want sub-arcsec accuracy at the rim,
  we can add the full polynomial plate model.
