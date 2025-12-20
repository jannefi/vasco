
#!/usr/bin/env python3
"""
fit_plate_solution.py

Per-tile polynomial plate solution (Gaia tie) for VASCO pipeline.

- Scans tiles under --tiles-folder (default: ./data/tiles)
- Loads Gaia xmatch: xmatch/sex_gaia_xmatch_cdss_within5arcsec.csv
- Uses windowed centroids (ALPHAWIN/DELTAWIN) if available
- Quality filters: FLAGS==0, ELONGATION<=1.7, MAG_AUTO>-13 (applied if columns exist)
- Fits 2D polynomial (affine+quadratic) RA/Dec -> Gaia RA/Dec via RANSAC
- Applies correction to final_catalog.csv and writes final_catalog_wcsfix.csv
- Supports --degree (default 2), --dry-run, --min-matches
"""

import os
import glob
import argparse
import pandas as pd
import numpy as np
from sklearn.linear_model import RANSACRegressor
from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import make_pipeline

GAIA_XMATCH = "xmatch/sex_gaia_xmatch_cdss_within5arcsec.csv"
FINAL_CATALOG = "final_catalog.csv"
OUT_CATALOG = "final_catalog_wcsfix.csv"

def has_cols(df, *cols):
    return df is not None and not df.empty and all(c in df.columns for c in cols)

def pick_radec(df):
    if has_cols(df, "ALPHAWIN_J2000", "DELTAWIN_J2000"):
        return "ALPHAWIN_J2000", "DELTAWIN_J2000"
    if has_cols(df, "ALPHA_J2000", "DELTA_J2000"):
        return "ALPHA_J2000", "DELTA_J2000"
    return None, None

def pick_gaia(df):
    for ra, dec in [("RA_ICRS", "DE_ICRS"), ("RAJ2000", "DEJ2000"), ("RA", "DEC"), ("ra", "dec")]:
        if has_cols(df, ra, dec):
            return ra, dec
    return None, None

def apply_quality_mask(gdf, mag_auto_threshold=-13, elongation_max=1.7):
    mask = pd.Series(True, index=gdf.index)
    if "FLAGS" in gdf.columns:
        mask &= (pd.to_numeric(gdf["FLAGS"], errors="coerce") == 0)
    if "ELONGATION" in gdf.columns:
        mask &= (pd.to_numeric(gdf["ELONGATION"], errors="coerce") <= float(elongation_max))
    if "MAG_AUTO" in gdf.columns:
        mask &= (pd.to_numeric(gdf["MAG_AUTO"], errors="coerce") > float(mag_auto_threshold))
    return gdf.loc[mask]

def fit_plate_poly(ra_det, dec_det, ra_gaia, dec_gaia, degree=2):
    X = np.column_stack([ra_det, dec_det])
    model_ra = make_pipeline(PolynomialFeatures(degree, include_bias=True), RANSACRegressor())
    model_dec = make_pipeline(PolynomialFeatures(degree, include_bias=True), RANSACRegressor())
    model_ra.fit(X, ra_gaia)
    model_dec.fit(X, dec_gaia)
    return model_ra, model_dec

def apply_plate_poly(model_ra, model_dec, ra, dec):
    X = np.column_stack([ra, dec])
    ra_corr = model_ra.predict(X)
    dec_corr = model_dec.predict(X)
    return ra_corr, dec_corr

def process_tile(tile_dir, args):
    gpath = os.path.join(tile_dir, GAIA_XMATCH)
    fcat_path = os.path.join(tile_dir, FINAL_CATALOG)
    out_path = os.path.join(tile_dir, OUT_CATALOG)

    if not os.path.exists(gpath) or not os.path.exists(fcat_path):
        return False, "missing required files"
    
    # if output exists, skip
    if os.path.exists(out_path):
        return True, "output file already exists"

    try:
        gdf = pd.read_csv(gpath, engine="python", on_bad_lines="skip")
    except Exception as e:
        return False, f"read xmatch failed: {e}"

    # Quality mask
    gdf = apply_quality_mask(gdf, mag_auto_threshold=args.mag_auto_threshold,
                             elongation_max=args.elongation_max)

    # Pick columns
    ra_det_col, dec_det_col = pick_radec(gdf)
    ra_gaia_col, dec_gaia_col = pick_gaia(gdf)
    if not ra_det_col or not dec_det_col or not ra_gaia_col or not dec_gaia_col:
        return False, "missing RA/Dec columns"

    gdf = gdf.dropna(subset=[ra_det_col, dec_det_col, ra_gaia_col, dec_gaia_col])
    if len(gdf) < args.min_matches:
        return False, f"too few matches ({len(gdf)}<{args.min_matches})"

    ra_det  = pd.to_numeric(gdf[ra_det_col], errors="coerce")
    dec_det = pd.to_numeric(gdf[dec_det_col], errors="coerce")
    ra_gaia = pd.to_numeric(gdf[ra_gaia_col], errors="coerce")
    dec_gaia= pd.to_numeric(gdf[dec_gaia_col], errors="coerce")

    try:
        model_ra, model_dec = fit_plate_poly(ra_det, dec_det, ra_gaia, dec_gaia, degree=args.degree)
    except Exception as e:
        return False, f"fit failed: {e}"

    try:
        fcat = pd.read_csv(fcat_path, engine="python", on_bad_lines="skip")
    except Exception as e:
        return False, f"read final_catalog failed: {e}"

    ra_f_col, dec_f_col = pick_radec(fcat)
    if not ra_f_col or not dec_f_col:
        return False, "final_catalog missing RA/Dec"

    ra_f  = pd.to_numeric(fcat[ra_f_col], errors="coerce")
    dec_f = pd.to_numeric(fcat[dec_f_col], errors="coerce")

    ra_corr, dec_corr = apply_plate_poly(model_ra, model_dec, ra_f, dec_f)
    fcat["RA_corr"] = ra_corr
    fcat["Dec_corr"] = dec_corr

    if args.dry_run:
        return True, "dry-run (no file written)"

    fcat.to_csv(out_path, index=False)
    return True, f"wrote {OUT_CATALOG} ({len(fcat)} rows)"

def main():
    ap = argparse.ArgumentParser(description="Per-tile Gaia-tied plate solution for VASCO.")
    ap.add_argument("--tiles-folder", default="./data/tiles", help="Root folder of tiles (default: ./data/tiles)")
    ap.add_argument("--degree", type=int, default=2, help="Polynomial degree (default: 2)")
    ap.add_argument("--min-matches", type=int, default=10, help="Minimum xmatch rows required to fit (default: 10)")
    ap.add_argument("--elongation-max", type=float, default=1.7, help="ELONGATION upper bound (if column exists)")
    ap.add_argument("--mag-auto-threshold", type=float, default=-13, help="MAG_AUTO lower bound (if column exists)")
    ap.add_argument("--dry-run", action="store_true", help="Compute but do not write files")
    args = ap.parse_args()

    tiles = sorted(d for d in glob.glob(os.path.join(args.tiles_folder, "tile-*")) if os.path.isdir(d))
    print(f"[INFO] Tiles root: {args.tiles_folder} | tiles found: {len(tiles)}")

    ok, warn = 0, 0
    for idx, t in enumerate(tiles, 1):
        success, msg = process_tile(t, args)
        status = "[OK]" if success else "[WARN]"
        print(f"{status} {os.path.basename(t)}: {msg}")
        ok += int(success)
        warn += int(not success)
        if idx % 100 == 0:
            print(f"[PROGRESS] {idx}/{len(tiles)} processed | OK={ok} WARN={warn}")

    print(f"[DONE] processed={len(tiles)} OK={ok} WARN={warn}")

if __name__ == "__main__":
   raise SystemExit(main())
