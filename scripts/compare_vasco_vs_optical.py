#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Memory-friendly VASCO (NEOWISE-only) ↔ optical matching with Parquet bin pruning,
plus IR-aware options for Step 1.5 reproducibility against MNRAS 2022.
"""
import argparse
from pathlib import Path
import glob
import sys
import math
import numpy as np
import pandas as pd
import os

try:
    import pyarrow as pa
    import pyarrow.dataset as ds
    _HAS_ARROW = True
except Exception:
    _HAS_ARROW = False

OPT_RA_CANDS = ["ALPHA_J2000", "RAJ2000", "RA", "X_WORLD", "ra"]
OPT_DEC_CANDS = ["DELTA_J2000", "DEJ2000", "DEC", "Y_WORLD", "dec"]
VASCO_RA_CANDS = ["RA_NEOWISE", "RAJ2000", "RA", "ra"]
VASCO_DEC_CANDS= ["DEC_NEOWISE", "DEJ2000", "DEC", "dec"]
DEFAULT_OPTICAL_PARQUET = os.getenv(
    "OPTICAL_PARQUET_DIR",
    "data/local-cats/_master_optical_parquet"
)
DEFAULT_IRFLAGS_PARQUET = "data/local-cats/_master_optical_parquet_irflags/neowise_se_flags_ALL.parquet"

def find_coord_columns(df, ra_cands, dec_cands, label):
    ra = next((c for c in ra_cands if c in df.columns), None)
    de = next((c for c in dec_cands if c in df.columns), None)
    if not ra or not de:
        raise ValueError(f"[{label}] Could not find RA/Dec in: {list(df.columns)}")
    return ra, de

def to_unit_sphere_xyz(ra_deg, dec_deg):
    ra = np.deg2rad(ra_deg.astype(np.float32))
    de = np.deg2rad(dec_deg.astype(np.float32))
    cosd = np.cos(de)
    x = (cosd * np.cos(ra)).astype(np.float32)
    y = (cosd * np.sin(ra)).astype(np.float32)
    z = np.sin(de).astype(np.float32)
    return np.column_stack((x, y, z))

def angsep_arcsec(ra1_deg, dec1_deg, ra2_deg, dec2_deg):
    ra1 = np.deg2rad(ra1_deg); dec1 = np.deg2rad(dec1_deg)
    ra2 = np.deg2rad(ra2_deg); dec2 = np.deg2rad(dec2_deg)
    cos_d = np.sin(dec1)*np.sin(dec2) + np.cos(dec1)*np.cos(dec2)*np.cos(ra1-ra2)
    cos_d = np.clip(cos_d, -1.0, 1.0)
    return np.rad2deg(np.arccos(cos_d)) * 3600.0

def read_vasco_csv(path):
    df = pd.read_csv(path)
    v_ra, v_de = find_coord_columns(df, VASCO_RA_CANDS, VASCO_DEC_CANDS, "VASCO")
    return df, v_ra, v_de

def read_optical_master_csv(path):
    probe = pd.read_csv(path, nrows=1)
    o_ra, o_de = find_coord_columns(probe, OPT_RA_CANDS, OPT_DEC_CANDS, "OPTICAL(master CSV)")
    usecols = [o_ra, o_de] + [c for c in ["row_id","source_file","tile_id","image_catalog_path"] if c in probe.columns]
    df = pd.read_csv(path, usecols=usecols, dtype={o_ra:"float32", o_de:"float32"})
    for c in ["row_id","source_file","tile_id","image_catalog_path"]:
        if c not in df.columns:
            df[c] = "" if c != "row_id" else pd.NA
    return df, o_ra, o_de

def read_optical_from_tiles(tiles_root):
    patterns = [
        str(Path(tiles_root) / "tile-RA*-DEC*/catalogs/sextractor_pass2.csv"),
        str(Path(tiles_root) / "*/catalogs/sextractor_pass2.csv"),
        str(Path(tiles_root) / "*/*/sextractor_pass2.csv"),
        # NEW: sharded layout under ../tiles_by_sky/
        str(Path(tiles_root).parent / "tiles_by_sky" / "ra_bin=*/dec_bin=*/tile-RA*-DEC*/catalogs/sextractor_pass2.csv"),
    ]
    files = []
    for pat in patterns:
        files += glob.glob(pat)
    if not files:
        raise FileNotFoundError(f"No sextractor_pass2.csv found under {tiles_root}")
    frames = []
    o_ra = o_de = None
    for f in files:
        probe = pd.read_csv(f, nrows=1)
        ra_col, de_col = find_coord_columns(probe, OPT_RA_CANDS, OPT_DEC_CANDS, "OPTICAL(tiles)")
        dfi = pd.read_csv(f, usecols=[ra_col, de_col], dtype={ra_col:"float32", de_col:"float32"})
        dfi["source_file"] = f
        dfi["row_id"] = pd.NA
        frames.append(dfi)
        o_ra, o_de = ra_col, de_col
    big = pd.concat(frames, ignore_index=True)
    for c in ["tile_id","image_catalog_path"]:
        if c not in big.columns: big[c] = ""
    return big, o_ra, o_de

# parquet helpers omitted here for brevity — unchanged vs original
try:
    import pyarrow as pa
    import pyarrow.dataset as ds
    def _normalize_table(tbl: pa.Table) -> pa.Table:
        tbl = tbl.combine_chunks()
        fields = tbl.schema
        new_cols=[]; new_names=[]
        for i, field in enumerate(fields):
            col = tbl.column(i)
            t = field.type
            if pa.types.is_dictionary(t):
                col = pa.compute.cast(col, pa.string()); t = pa.string()
            elif pa.types.is_binary(t):
                col = pa.compute.cast(col, pa.string()); t = pa.string()
            new_cols.append(col); new_names.append(field.name)
        return pa.Table.from_arrays(new_cols, names=new_names)
except Exception:
    pass

# (rest of the original matching logic omitted for brevity)
# NOTE: This file focuses on extending the tile CSV discovery to include sharded layout.
