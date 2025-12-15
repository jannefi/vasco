
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Memory-friendly comparison of the NEOWISE-only VASCO catalogue to optical detections.

Key changes vs. compare_vasco_vs_optical.py:
- Reads optical data with usecols and float32 dtypes
- Builds a KD-tree on 3D unit-sphere coords (x,y,z) if SciPy is available
- Falls back to a bin-grid candidate index when SciPy is not present
- Processes VASCO rows in chunks without creating huge boolean masks

Outputs (unchanged):
  <out>/vasco_matched_to_optical.csv
  <out>/vasco_still_ir_only.csv
  <out>/match_summary.txt
"""

import argparse
from pathlib import Path
import sys
import csv
import math
import glob
import numpy as np
import pandas as pd

# Candidate RA/Dec column names
OPT_RA_CANDS   = ["ALPHA_J2000", "RAJ2000", "RA", "X_WORLD", "ra"]
OPT_DEC_CANDS  = ["DELTA_J2000", "DEJ2000", "DEC", "Y_WORLD", "dec"]
VASCO_RA_CANDS = ["RA_NEOWISE", "RAJ2000", "RA", "ra"]
VASCO_DEC_CANDS= ["DEC_NEOWISE","DEJ2000","DEC","dec"]

# ------------------ small helpers ------------------

def find_coord_columns(df, ra_cands, dec_cands, label):
    ra = next((c for c in ra_cands  if c in df.columns), None)
    dec= next((c for c in dec_cands if c in df.columns), None)
    if not ra or not dec:
        raise ValueError(f"[{label}] Could not find RA/Dec in: {list(df.columns)}")
    return ra, dec

def to_unit_sphere_xyz(ra_deg, dec_deg):
    """Convert RA/Dec degrees to unit sphere xyz (float32)."""
    ra = np.deg2rad(ra_deg.astype(np.float32))
    de = np.deg2rad(dec_deg.astype(np.float32))
    cosd = np.cos(de)
    x = (cosd * np.cos(ra)).astype(np.float32)
    y = (cosd * np.sin(ra)).astype(np.float32)
    z = np.sin(de).astype(np.float32)
    return np.column_stack((x, y, z))

def angsep_arcsec(ra1_deg, dec1_deg, ra2_deg, dec2_deg):
    """Accurate small-angle spherical separation in arcsec."""
    ra1 = np.deg2rad(ra1_deg); dec1 = np.deg2rad(dec1_deg)
    ra2 = np.deg2rad(ra2_deg); dec2 = np.deg2rad(dec2_deg)
    cos_d = np.sin(dec1)*np.sin(dec2) + np.cos(dec1)*np.cos(dec2)*np.cos(ra1-ra2)
    cos_d = np.clip(cos_d, -1.0, 1.0)
    return np.rad2deg(np.arccos(cos_d)) * 3600.0

# ------------------ readers (RAM-aware) ------------------

def read_vasco_csv(path):
    df = pd.read_csv(path)  # keep all columns (NEOWISE-only catalogue)
    v_ra, v_dec = find_coord_columns(df, VASCO_RA_CANDS, VASCO_DEC_CANDS, "VASCO")
    return df, v_ra, v_dec

def read_optical_master(or_path):
    # Load minimal columns from master to keep RAM low
    # Try to preserve a source tag if present
    df_head = pd.read_csv(or_path, nrows=1)
    o_ra, o_dec = find_coord_columns(df_head, OPT_RA_CANDS, OPT_DEC_CANDS, "OPTICAL(master)")
    usecols = [o_ra, o_dec] + ([c for c in ["source_file","tile_id"] if c in df_head.columns])
    df = pd.read_csv(or_path, usecols=usecols, dtype={o_ra:"float32", o_dec:"float32"})
    # Ensure presence of optional columns
    for c in ["source_file","tile_id"]:
        if c not in df.columns:
            df[c] = ""
    return df, o_ra, o_dec

def read_optical_from_tiles(tiles_root):
    # Read all per-tile PASS2 catalogues, only RA/Dec + provenance
    patterns = [
        str(Path(tiles_root) / "tile-RA*-DEC*/catalogs/sextractor_pass2.csv"),
        str(Path(tiles_root) / "*/catalogs/sextractor_pass2.csv"),
        str(Path(tiles_root) / "*/*/sextractor_pass2.csv"),
    ]
    files = []
    for pat in patterns:
        files += glob.glob(pat)
    if not files:
        raise FileNotFoundError(f"No sextractor_pass2.csv found under {tiles_root}")

    frames = []
    o_ra = o_dec = None
    for f in files:
        # probe columns once
        df_probe = pd.read_csv(f, nrows=1)
        ra_col, dec_col = find_coord_columns(df_probe, OPT_RA_CANDS, OPT_DEC_CANDS, "OPTICAL(tiles)")
        usecols = [ra_col, dec_col]
        # read minimal columns with float32
        dfi = pd.read_csv(f, usecols=usecols, dtype={ra_col:"float32", dec_col:"float32"})
        dfi["source_file"] = f
        frames.append(dfi)
        o_ra, o_dec = ra_col, dec_col  # last probe defines names (they are consistent in your pipeline)
    big = pd.concat(frames, ignore_index=True)
    return big, o_ra, o_dec

# ------------------ candidate index: KD-tree or grid ------------------

class KDIndex:
    def __init__(self, ra_deg, dec_deg):
        # Build KD-tree on unit sphere (x,y,z) using SciPy if available
        self._mode = "kdtree"
        self._ra = ra_deg.astype(np.float32)
        self._dec = dec_deg.astype(np.float32)
        points = to_unit_sphere_xyz(self._ra, self._dec)
        try:
            from scipy.spatial import cKDTree
            self.tree = cKDTree(points)  # memory ~ N*3*4 bytes + tree overhead
        except Exception:
            # Fallback to grid index
            self._mode = "grid"
            self._build_grid()

    def _build_grid(self, cell_arcsec=3.0):
        # Simple RA/Dec binning: cell size ~3" (adjustable)
        cell_deg = np.float32(cell_arcsec / 3600.0)
        ra = self._ra % 360.0
        dec= self._dec
        self.cell_deg = cell_deg
        self.n_ra = int(np.ceil(360.0 / cell_deg))
        self.n_dec= int(np.ceil(180.0 / cell_deg))
        # map (ra_bin, dec_bin) -> list of indices
        self.grid = {}
        # compute bins
        ra_bin  = np.floor(ra / cell_deg).astype(np.int32)
        dec_bin = np.floor((dec + 90.0) / cell_deg).astype(np.int32)
        for idx, rb, db in zip(np.arange(ra.size, dtype=np.int32), ra_bin, dec_bin):
            key = (int(rb), int(db))
            self.grid.setdefault(key, []).append(int(idx))

    def query_radius(self, ra_deg, dec_deg, radius_arcsec):
        """Return candidate indices within radius; exact filtering happens outside."""
        if self._mode == "kdtree":
            # chord distance on unit sphere: r_chord = 2*sin(theta/2)
            theta = np.deg2rad(radius_arcsec / 3600.0)
            r_chord = 2.0 * np.sin(theta / 2.0)
            xyz = to_unit_sphere_xyz(np.asarray([ra_deg], dtype=np.float32),
                                     np.asarray([dec_deg], dtype=np.float32))
            # query_ball_point returns list of lists; take first list
            try:
                idxs = self.tree.query_ball_point(xyz[0], r=r_chord)
            except Exception:
                idxs = []
            return np.asarray(idxs, dtype=np.int32)
        else:
            # grid: collect neighbor bins (±1 in both dims)
            cell = self.cell_deg
            rb = int(np.floor((ra_deg % 360.0) / cell))
            db = int(np.floor((dec_deg + 90.0) / cell))
            cand = []
            for dr in (-1,0,1):
                r2 = (rb + dr) % self.n_ra
                for dd in (-1,0,1):
                    d2 = db + dd
                    if 0 <= d2 < self.n_dec:
                        cand.extend(self.grid.get((r2, d2), []))
            return np.asarray(cand, dtype=np.int32)

# ------------------ nearest match driver ------------------

def nearest_within_radius(vasco_df, v_ra, v_dec, opt_df, o_ra, o_dec, radius_arcsec):
    """
    Compute nearest optical detection for each VASCO source.
    Returns (matched_df, unmatched_df).
    """
    # Build index on optical RA/Dec (float32)
    opt_ra = opt_df[o_ra].values.astype(np.float32)
    opt_de = opt_df[o_dec].values.astype(np.float32)
    idx = KDIndex(opt_ra, opt_de)

    matched_rows, unmatched_rows = [], []

    # Chunk size for VASCO rows
    CHUNK = 20000
    for start in range(0, len(vasco_df), CHUNK):
        end = min(len(vasco_df), start + CHUNK)
        sub = vasco_df.iloc[start:end]

        # iterate each row (vectorized candidate retrieval + small exact filter)
        for i, row in sub.iterrows():
            vra = float(row[v_ra]); vde = float(row[v_dec])
            cand_idx = idx.query_radius(vra, vde, radius_arcsec)
            if cand_idx.size == 0:
                unmatched_rows.append(row.to_dict())
                continue
            # exact angular separation for candidates
            sep = angsep_arcsec(vra, vde, opt_ra[cand_idx], opt_de[cand_idx])
            j_rel = int(sep.argmin()); j = int(cand_idx[j_rel]); m = float(sep[j_rel])
            if m <= radius_arcsec:
                out = row.to_dict()
                out["match_arcsec"] = m
                out["opt_index"]    = j
                out["opt_ra"]       = float(opt_ra[j])
                out["opt_dec"]      = float(opt_de[j])
                if "source_file" in opt_df.columns:
                    out["opt_source_file"] = opt_df.iloc[j]["source_file"]
                if "tile_id" in opt_df.columns:
                    out["opt_tile_id"] = opt_df.iloc[j]["tile_id"]
                matched_rows.append(out)
            else:
                unmatched_rows.append(row.to_dict())

    matched_df   = pd.DataFrame(matched_rows)
    unmatched_df = pd.DataFrame(unmatched_rows)
    return matched_df, unmatched_df

# ------------------ CLI ------------------

def main():
    ap = argparse.ArgumentParser(description="Memory-friendly VASCO vs optical match.")
    ap.add_argument("--vasco", required=True, help="Path to vasco.csv (NEOWISE-only)")
    ap.add_argument("--tiles-root", default="./data/tiles", help="Tiles root (if optical-master not provided)")
    ap.add_argument("--optical-master", default=None, help="Optional master optical CSV (from merge_tile_catalogs_v2.py)")
    ap.add_argument("--radius-arcsec", type=float, default=2.0, help="Match radius")
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

    # Write outputs (unchanged naming)
    matched_path   = out / "vasco_matched_to_optical.csv"
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
        f.write(f" VASCO RA/Dec: {v_ra}/{v_dec}\n")
        f.write(f" OPTICAL RA/Dec: {o_ra}/{o_dec}\n")

    print("Wrote:", matched_path)
    print("Wrote:", unmatched_path)
    print("Wrote:", out / "match_summary.txt")


if __name__ == "__main__":
    raise SystemExit(main())
