
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Merge all per-image SExtractor pass2 CSV catalogs into a tile-level consolidated catalog,
optionally deduplicating sources within a specified sky tolerance.

Usage:
  python tools/merge_tile_catalogs.py --tiles-root ./vasco/data/tiles --tolerance-arcsec 0.5 --write-master

Outputs (per tile):
  catalogs/tile_catalog_pass2_raw.csv     # concatenated (no dedupe)
  catalogs/tile_catalog_pass2.csv         # deduped (best-of-cell selection)

Optional (across all tiles with --write-master):
  ./vasco/data/tiles/_master_tile_catalog_pass2_raw.csv
  ./vasco/data/tiles/_master_tile_catalog_pass2.csv
"""

import argparse
from pathlib import Path
import pandas as pd

CAND_RA = ["ALPHA_J2000", "RA", "X_WORLD", "RAJ2000"]
CAND_DEC = ["DELTA_J2000", "DEC", "Y_WORLD", "DEJ2000"]

def find_coord_columns(df):
    ra_col = next((c for c in CAND_RA if c in df.columns), None)
    dec_col = next((c for c in CAND_DEC if c in df.columns), None)
    if not ra_col or not dec_col:
        raise ValueError(f"Could not find RA/Dec columns in: {list(df.columns)}")
    return ra_col, dec_col

def pick_best(group, mag_col="MAG_AUTO", flags_col="FLAGS"):
    # Choose lowest FLAGS first; among ties, choose brightest (smallest MAG_AUTO)
    sort_cols = []
    asc = []
    if flags_col in group.columns:
        sort_cols.append(flags_col); asc.append(True)
    if mag_col in group.columns:
        sort_cols.append(mag_col); asc.append(True)
    if sort_cols:
        g2 = group.sort_values(sort_cols, ascending=asc)
        return g2.iloc[0]
    else:
        return group.iloc[0]

def dedupe_by_cells(df, ra_col, dec_col, tol_arcsec):
    tol_deg = tol_arcsec / 3600.0
    # Bucket RA/Dec to tolerance-scale cells; works well for small tolerances
    df["_ra_cell"]  = (df[ra_col]  / tol_deg).round().astype("int64")
    df["_dec_cell"] = (df[dec_col] / tol_deg).round().astype("int64")
    # Decide best representative per cell
    out = (
        df.groupby(["_ra_cell", "_dec_cell"], sort=False)
          .apply(lambda g: pick_best(g), include_groups=False)
          .reset_index(drop=True)
    )
    return out.drop(columns=["_ra_cell", "_dec_cell"], errors="ignore")

def merge_one_tile(tile_path: Path, tol_arcsec: float):
    catalogs_root = tile_path / "catalogs"
    files = list(catalogs_root.glob("**/sextractor_pass2.csv"))
    if not files:
        return None, None  # nothing to do

    frames = []
    for f in files:
        try:
            df = pd.read_csv(f)
        except Exception as e:
            raise RuntimeError(f"Failed to read {f}: {e}")
        ra_col, dec_col = find_coord_columns(df)
        # add lineage
        df["tile_id"] = tile_path.name
        df["image_catalog_path"] = str(f.relative_to(tile_path))
        df["image_id"] = f.parent.name  # folder name under catalogs/** presumed to be per-image bucket
        df["_RA_COL"] = ra_col
        df["_DEC_COL"] = dec_col
        frames.append(df)

    raw = pd.concat(frames, ignore_index=True)
    # Use the first frame to know the coord column names; if mixed, prefer the most common
    ra_col = raw["_RA_COL"].mode().iat[0] if "_RA_COL" in raw.columns else CAND_RA[0]
    dec_col = raw["_DEC_COL"].mode().iat[0] if "_DEC_COL" in raw.columns else CAND_DEC[0]
    raw = raw.drop(columns=["_RA_COL", "_DEC_COL"], errors="ignore")

    deduped = dedupe_by_cells(raw, ra_col, dec_col, tol_arcsec)

    # Write outputs
    out_dir = catalogs_root
    out_dir.mkdir(parents=True, exist_ok=True)
    raw_out = out_dir / "tile_catalog_pass2_raw.csv"
    dedup_out = out_dir / "tile_catalog_pass2.csv"
    raw.to_csv(raw_out, index=False)
    deduped.to_csv(dedup_out, index=False)
    return raw_out, dedup_out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tiles-root", default="./vasco/data/tiles", help="Path to tiles root")
    ap.add_argument("--tolerance-arcsec", type=float, default=0.5, help="Positional dedupe tolerance")
    ap.add_argument("--write-master", action="store_true", help="Also write master (all tiles) catalogs")
    args = ap.parse_args()

    tiles_root = Path(args.tiles_root)
    tile_dirs = sorted([p for p in tiles_root.glob("tile-RA*DEC*") if p.is_dir()])
    master_raw_frames = []
    master_dedup_frames = []

    for tile_path in tile_dirs:
        res = merge_one_tile(tile_path, args.tolerance_arcsec)
        if res == (None, None):
            continue
        raw_out, dedup_out = res
        # Accumulate for master
        master_raw_frames.append(pd.read_csv(raw_out))
        master_dedup_frames.append(pd.read_csv(dedup_out))

    if args.write_master and master_raw_frames:
        master_raw = pd.concat(master_raw_frames, ignore_index=True)
        master_dedup = pd.concat(master_dedup_frames, ignore_index=True)
        master_raw.to_csv(tiles_root / "_master_tile_catalog_pass2_raw.csv", index=False)
        master_dedup.to_csv(tiles_root / "_master_tile_catalog_pass2.csv", index=False)

if __name__ == "__main__":
    main()

