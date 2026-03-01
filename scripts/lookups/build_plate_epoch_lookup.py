#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build plate→epoch lookup (plate_id → date_obs_iso + jd) from data/metadata/tile_to_dss1red.csv.

- Canonical plate_id is REGION (tile_region).
- tile_date_obs may contain non-strict ISO variants; we sanitize using utils_epoch.parse_dateobs_with_sanitize().

Output columns (used by skybot_fetch_chunk.py):
  plate_id, date_obs_iso, jd
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

# ---- Make scripts/ importable without requiring it to be a Python package ----
_THIS = Path(__file__).resolve()
SCRIPTS_DIR = _THIS.parents[1]          # .../scripts
REPO_ROOT = _THIS.parents[2]            # repo root
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

# utils_epoch.py lives under scripts/ (as a plain module)
from utils_epoch import parse_dateobs_with_sanitize  

MJD_TO_JD = 2400000.5


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-csv", default=str(REPO_ROOT / "data/metadata/tile_to_dss1red.csv"))
    ap.add_argument("--out", default=str(REPO_ROOT / "metadata/plates/plate_epoch_lookup.parquet"))
    ap.add_argument("--format", choices=["parquet", "csv"], default="parquet")
    ap.add_argument("--debug-bad", type=int, default=25, help="print N unparseable examples")
    args = ap.parse_args()

    in_path = Path(args.in_csv)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Only required columns
    df = pd.read_csv(in_path, usecols=["tile_region", "tile_date_obs"])
    df = df.rename(columns={"tile_region": "plate_id", "tile_date_obs": "date_obs_raw"})
    df["plate_id"] = df["plate_id"].astype(str).str.strip()
    df["date_obs_raw"] = df["date_obs_raw"].astype(str).str.strip()
    df = df[(df["plate_id"] != "") & (df["date_obs_raw"] != "")].drop_duplicates()

    iso_out = []
    mjd_out = []
    bad_examples = []

    for plate_id, raw in df[["plate_id", "date_obs_raw"]].itertuples(index=False):
        res = parse_dateobs_with_sanitize(raw)  
        if res is None:
            iso_out.append(None)
            mjd_out.append(None)
            if len(bad_examples) < args.debug_bad:
                bad_examples.append((plate_id, raw))
        else:
            iso_utc, mjd = res
            iso_out.append(iso_utc)
            mjd_out.append(mjd)

    df["date_obs_iso"] = iso_out
    df["mjd"] = mjd_out

    n_bad = int(df["mjd"].isna().sum())
    if n_bad:
        print(f"[WARN] DATE-OBS parse failures after sanitize: {n_bad}")
        if bad_examples:
            print("[WARN] examples (plate_id, date_obs_raw):")
            for pid, raw in bad_examples:
                print(f"  {pid}\t{raw}")

    df = df.dropna(subset=["mjd"]).copy()
    df["jd"] = df["mjd"].astype(float) + MJD_TO_JD

    # One epoch per plate_id (DATE-OBS is plate-level; duplicates are fine)
    df = df.sort_values(["plate_id", "date_obs_iso"]).drop_duplicates(["plate_id"], keep="first")

    out = df[["plate_id", "date_obs_iso", "jd"]].copy()

    if args.format == "csv":
        out.to_csv(out_path, index=False)
    else:
        out.to_parquet(out_path, index=False)

    print(f"[OK] wrote {out_path} plates={out['plate_id'].nunique()} rows={len(out)}")


if __name__ == "__main__":
    main()
