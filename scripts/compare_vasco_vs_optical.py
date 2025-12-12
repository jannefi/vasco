
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Compare the NEOWISE-only VASCO catalogue to your optical detections.

Inputs:
  --vasco <path>               Path to MNRAS/SVO NEOWISE-only CSV (171,753 sources)
  --tiles-root <path>          Root to your tiles folder (default: ./data/tiles)
  --optical-master <path>      Optional: use a prebuilt master optical CSV (from merge_tile_catalogs.py)
  --radius-arcsec <float>      Match radius in arcseconds (default: 2.0)
  --out-dir <path>             Output folder (default: ./out)

Outputs:
  <out>/vasco_matched_to_optical.csv
  <out>/vasco_still_ir_only.csv
  <out>/match_summary.txt

Notes:
  - Detects RA/Dec columns on both sides (common names handled).
  - If multiple optical detections fall within the radius, keeps the nearest one (one-to-one assignment).
  - Columns in the output are preserved from the original VASCO row, plus a few match diagnostics.
"""

import argparse
from pathlib import Path
import sys
import csv
import math
import glob
import pandas as pd
import numpy as np

# Candidate RA/Dec column names (optical side: SExtractor; VASCO side: NEOWISE)
OPT_RA_CANDS = ["ALPHA_J2000", "RAJ2000", "RA", "X_WORLD", "ra"]
OPT_DEC_CANDS = ["DELTA_J2000", "DEJ2000", "DEC", "Y_WORLD", "dec"]

VASCO_RA_CANDS = ["RA_NEOWISE", "RAJ2000", "RA", "ra"]
VASCO_DEC_CANDS = ["DEC_NEOWISE", "DEJ2000", "DEC", "dec"]

def find_coord_columns(df, ra_cands, dec_cands, label):
    ra = next((c for c in ra_cands if c in df.columns), None)
    dec = next((c for c in dec_cands if c in df.columns), None)
    if not ra or not dec:
        raise ValueError(f"[{label}] Could not find RA/Dec in: {list(df.columns)}")
    return ra, dec

def read_vasco_csv(path):
    df = pd.read_csv(path)
    # Prefer RA_NEOWISE/DEC_NEOWISE; fall back if needed
    ra_col, dec_col = find_coord_columns(df, VASCO_RA_CANDS, VASCO_DEC_CANDS, "VASCO")
    return df, ra_col, dec_col

def read_optical_master(or_path):
    df = pd.read_csv(or_path)
    ra_col, dec_col = find_coord_columns(df, OPT_RA_CANDS, OPT_DEC_CANDS, "OPTICAL(master)")
    return df, ra_col, dec_col

def read_optical_from_tiles(tiles_root):
    # Read all per-tile SExtractor PASS2 catalogues
    files = glob.glob(str(Path(tiles_root) / "*/*/sextractor_pass2.csv")) \
         or glob.glob(str(Path(tiles_root) / "tile-RA*DEC*/catalogs/sextractor_pass2.csv"))
    if not files:
        raise FileNotFoundError(f"No sextractor_pass2.csv found under {tiles_root}")
    frames = []
    for f in files:
        try:
            df = pd.read_csv(f)
            df["source_file"] = f
            frames.append(df)
        except Exception as e:
            raise RuntimeError(f"Failed to read {f}: {e}")
    big = pd.concat(frames, ignore_index=True)
    ra_col, dec_col = find_coord_columns(big, OPT_RA_CANDS, OPT_DEC_CANDS, "OPTICAL(tiles)")
    return big, ra_col, dec_col

def angsep_arcsec(ra1_deg, dec1_deg, ra2_deg, dec2_deg):
    # Vectorized small-angle spherical separation (ICRS/J2000 assumed)
    ra1 = np.deg2rad(ra1_deg); dec1 = np.deg2rad(dec1_deg)
    ra2 = np.deg2rad(ra2_deg); dec2 = np.deg2rad(dec2_deg)
    cos_d = np.sin(dec1) * np.sin(dec2) + np.cos(dec1) * np.cos(dec2) * np.cos(ra1 - ra2)
    # Guard for numerical issues
    cos_d = np.clip(cos_d, -1.0, 1.0)
    return np.rad2deg(np.arccos(cos_d)) * 3600.0  # arcsec

def nearest_within_radius(vasco_df, v_ra, v_dec, opt_df, o_ra, o_dec, radius_arcsec):
    """
    Compute nearest optical detection for each VASCO source.
    Returns (matched_df, unmatched_df), where matched_df contains:
      - all original VASCO columns
      - match_arcsec, opt_index, opt_ra, opt_dec, opt_source_file (if available)
    """
    # Build numpy arrays
    v_ra_arr = vasco_df[v_ra].values.astype(float)
    v_dec_arr = vasco_df[v_dec].values.astype(float)
    o_ra_arr = opt_df[o_ra].values.astype(float)
    o_dec_arr = opt_df[o_dec].values.astype(float)

    matched_rows = []
    unmatched_rows = []

    # Chunking for memory friendliness (adjust chunk size if needed)
    CHUNK = 20000
    for start in range(0, len(vasco_df), CHUNK):
        end = min(len(vasco_df), start + CHUNK)
        vra = v_ra_arr[start:end]
        vde = v_dec_arr[start:end]

        # Compute angular separation to all optical detections (brute-force by chunks)
        # For very large optical sets, consider a HEALPix or k-d tree approach.
        # Here we do a two-step filter to reduce compute: coarse RA/Dec window ~ radius
        # 1) quick rectangular prefilter
        rad_deg = radius_arcsec / 3600.0
        mask_candidates = (
            (o_ra_arr[:, None] > vra - rad_deg) &
            (o_ra_arr[:, None] < vra + rad_deg) &
            (o_dec_arr[:, None] > vde - rad_deg) &
            (o_dec_arr[:, None] < vde + rad_deg)
        )
        # Iterate each VASCO row in this chunk
        for i in range(end - start):
            cand_idx = np.where(mask_candidates[:, i])[0]
            if cand_idx.size == 0:
                # no optical within rectangular prefilter
                row = vasco_df.iloc[start + i].to_dict()
                unmatched_rows.append(row)
                continue
            sep = angsep_arcsec(vra[i], vde[i], o_ra_arr[cand_idx], o_dec_arr[cand_idx])
            m = sep.min()
            j = cand_idx[sep.argmin()]
            if m <= radius_arcsec:
                # matched: attach a few optical fields
                row = vasco_df.iloc[start + i].to_dict()
                row["match_arcsec"] = float(m)
                row["opt_index"] = int(j)
                row["opt_ra"] = float(o_ra_arr[j])
                row["opt_dec"] = float(o_dec_arr[j])
                if "source_file" in opt_df.columns:
                    row["opt_source_file"] = opt_df.iloc[j]["source_file"]
                matched_rows.append(row)
            else:
                row = vasco_df.iloc[start + i].to_dict()
                unmatched_rows.append(row)

    matched_df = pd.DataFrame(matched_rows)
    unmatched_df = pd.DataFrame(unmatched_rows)
    return matched_df, unmatched_df

def main():
    ap = argparse.ArgumentParser(description="Compare NEOWISE-only VASCO catalogue to optical detections.")
    ap.add_argument("--vasco", required=True, help="Path to vasco.csv (NEOWISE-only)")
    ap.add_argument("--tiles-root", default="./data/tiles", help="Tiles root (if optical-master not provided)")
    ap.add_argument("--optical-master", default=None, help="Optional master optical CSV (from merge_tile_catalogs.py)")
    ap.add_argument("--radius-arcsec", type=float, default=2.0, help="Match radius in arcsec (default 2.0)")
    ap.add_argument("--out-dir", default="./out", help="Output directory")
    args = ap.parse_args()

    out = Path(args.out_dir); out.mkdir(parents=True, exist_ok=True)

    vasco_df, v_ra, v_dec = read_vasco_csv(args.vasco)

    if args.optical_master:
        opt_df, o_ra, o_dec = read_optical_master(args.optical_master)
    else:
        opt_df, o_ra, o_dec = read_optical_from_tiles(args.tiles_root)

    matched_df, unmatched_df = nearest_within_radius(
        vasco_df, v_ra, v_dec, opt_df, o_ra, o_dec, args.radius_arcsec
    )

    # Write outputs
    matched_path = out / "vasco_matched_to_optical.csv"
    unmatched_path = out / "vasco_still_ir_only.csv"
    matched_df.to_csv(matched_path, index=False)
    unmatched_df.to_csv(unmatched_path, index=False)

    # Summary
    with open(out / "match_summary.txt", "w") as f:
        f.write(f"VASCO rows: {len(vasco_df)}\n")
        f.write(f"Optical detections: {len(opt_df)}\n")
        f.write(f"Radius (arcsec): {args.radius_arcsec}\n")
        f.write(f"Matched: {len(matched_df)}\n")
        f.write(f"Still IR-only: {len(unmatched_df)}\n")
        f.write("\nColumns used:\n")
        f.write(f"  VASCO RA/Dec: {v_ra}/{v_dec}\n")
        f.write(f"  OPTICAL RA/Dec: {o_ra}/{o_dec}\n")

    print("Wrote:", matched_path)
    print("Wrote:", unmatched_path)
    print("Wrote:", out / "match_summary.txt")

if __name__ == "__main__":
    raise SystemExit(main())

