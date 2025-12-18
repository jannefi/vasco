
#!/usr/bin/env python3
"""
astrometry_residuals_corrected.py

For each tile:
- Loads final_catalog_wcsfix.csv (with RA_corr, Dec_corr)
- Loads Gaia xmatch CSV (xmatch/sex_gaia_xmatch_cdss_within5arcsec.csv)
- Matches by rounded RA_corr/Dec_corr <-> Gaia detection RA/Dec
- Computes residuals (arcsec) between corrected detection and Gaia positions
- Writes per-tile summary to astrometry_residuals_corrected.csv
"""

import os
import glob
import pandas as pd
import numpy as np
import argparse

# --- Config ---
GAIA_XMATCH = "xmatch/sex_gaia_xmatch_cdss_within5arcsec.csv"
FINAL_CATALOG = "final_catalog_wcsfix.csv"
OUT_SUMMARY = "astrometry_residuals_corrected.csv"

# --- Helper functions ---
def has_cols(df, *cols):
    return df is not None and not df.empty and all(c in df.columns for c in cols)

def pick_gaia(df):
    for ra, dec in [("RA_ICRS", "DE_ICRS"), ("RAJ2000", "DEJ2000"), ("RA", "DEC")]:
        if has_cols(df, ra, dec):
            return ra, dec
    return None, None

def compute_residuals(ra_det, dec_det, ra_gaia, dec_gaia):
    d_ra  = (ra_det - ra_gaia) * np.cos(np.deg2rad(dec_det))
    d_dec = (dec_det - dec_gaia)
    return np.hypot(d_ra, d_dec) * 3600.0  # arcsec

def main():
    ap = argparse.ArgumentParser(description="Per-tile astrometry residuals checker (Gaia-tied).")
    ap.add_argument("--tiles-folder", default="./data/tiles", help="Root folder of tiles (default: ./data/tiles)")
    args = ap.parse_args()
    base = args.tiles_folder
    tiles = sorted(d for d in glob.glob(os.path.join(base, "tile-*")) if os.path.isdir(d))
    out_path = os.path.join(base, "astrometry_summary.csv")
    print(f"[INFO] Found {len(tiles)} tiles.")

    rows = []
    for t in tiles:
        fcat_path = os.path.join(t, FINAL_CATALOG)
        gpath = os.path.join(t, GAIA_XMATCH)
        if not os.path.exists(fcat_path) or not os.path.exists(gpath):
            continue

        # Load corrected final catalog
        try:
            fcat = pd.read_csv(fcat_path, engine="python", on_bad_lines="skip")
        except Exception as e:
            print(f"[WARN] {t}: failed to read final_catalog_wcsfix: {e}")
            continue

        # Use corrected columns
        if not has_cols(fcat, "RA_corr", "Dec_corr"):
            print(f"[WARN] {t}: missing RA_corr/Dec_corr in final_catalog_wcsfix.")
            continue

        # Build detection key (rounded RA_corr:Dec_corr)
        fcat["__detkey__"] = (
            pd.to_numeric(fcat["RA_corr"], errors="coerce").round(6).astype(str) + ":" +
            pd.to_numeric(fcat["Dec_corr"], errors="coerce").round(6).astype(str)
        )

        # Load Gaia xmatch
        try:
            gdf = pd.read_csv(gpath, engine="python", on_bad_lines="skip")
        except Exception as e:
            print(f"[WARN] {t}: failed to read Gaia xmatch: {e}")
            continue

        # Pick detection RA/Dec columns in Gaia xmatch (should match original, not corrected)
        ra_det_col, dec_det_col = None, None
        if has_cols(gdf, "ALPHAWIN_J2000", "DELTAWIN_J2000"):
            ra_det_col, dec_det_col = "ALPHAWIN_J2000", "DELTAWIN_J2000"
        elif has_cols(gdf, "ALPHA_J2000", "DELTA_J2000"):
            ra_det_col, dec_det_col = "ALPHA_J2000", "DELTA_J2000"
        else:
            print(f"[WARN] {t}: missing detection RA/Dec in Gaia xmatch.")
            continue

        gdf["__detkey__"] = (
            pd.to_numeric(gdf[ra_det_col], errors="coerce").round(6).astype(str) + ":" +
            pd.to_numeric(gdf[dec_det_col], errors="coerce").round(6).astype(str)
        )

        # Pick Gaia RA/Dec columns
        ra_gaia_col, dec_gaia_col = pick_gaia(gdf)
        if not ra_gaia_col or not dec_gaia_col:
            print(f"[WARN] {t}: missing Gaia RA/Dec in Gaia xmatch.")
            continue

        # Merge on detection key
        merged = pd.merge(
            fcat[["__detkey__", "RA_corr", "Dec_corr"]],
            gdf[["__detkey__", ra_gaia_col, dec_gaia_col]],
            on="__detkey__",
            how="inner"
        )
        if merged.empty:
            continue

        ra_corr  = pd.to_numeric(merged["RA_corr"], errors="coerce")
        dec_corr = pd.to_numeric(merged["Dec_corr"], errors="coerce")
        ra_gaia  = pd.to_numeric(merged[ra_gaia_col], errors="coerce")
        dec_gaia = pd.to_numeric(merged[dec_gaia_col], errors="coerce")

        # Drop rows with NaN
        ok = np.isfinite(ra_corr) & np.isfinite(dec_corr) & np.isfinite(ra_gaia) & np.isfinite(dec_gaia)
        if not ok.any():
            continue

        dr = compute_residuals(ra_corr[ok], dec_corr[ok], ra_gaia[ok], dec_gaia[ok])
        if dr.size == 0:
            continue

        med = float(np.median(dr))
        p90 = float(np.percentile(dr, 90))
        status = "OK" if (med <= 0.3 and p90 <= 1.0) else "WARN"

        rows.append({
            "tile": os.path.basename(t),
            "n_matches": int(dr.size),
            "median_arcsec": med,
            "p90_arcsec": p90,
            "status": status
        })

        if len(rows) % 100 == 0:
            print(f"[PROGRESS] {len(rows)} tiles processed.")

    # Write summary
    pd.DataFrame(rows).to_csv(out_path, index=False)
    print(f"[OK] wrote {out_path} rows: {len(rows)}")

if __name__ == "__main__":
    main()
