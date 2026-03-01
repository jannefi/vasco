#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build tile→plate lookup from data/metadata/tile_to_dss1red.csv.

Canonical contract:
- plate_id == REGION (use tile_region as source of truth; see map_irsa_dss1red_to_tiles.py)

Input:  data/metadata/tile_to_dss1red.csv
Output: metadata/tiles/tile_to_plate_lookup.(csv|parquet) with columns: tile_id, plate_id
"""

import argparse
from pathlib import Path

import pandas as pd


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-csv", default="data/metadata/tile_to_dss1red.csv")
    ap.add_argument("--out", default="metadata/tiles/tile_to_plate_lookup.parquet")
    ap.add_argument("--format", choices=["parquet", "csv"], default="parquet")
    args = ap.parse_args()

    in_path = Path(args.in_csv)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(in_path)
    cols = {c.lower(): c for c in df.columns}

    if "tile_id" not in cols:
        raise SystemExit(f"[ERROR] missing tile_id in {in_path}")

    # REGION-first: tile_region is canonical
    if "tile_region" not in cols:
        raise SystemExit(f"[ERROR] missing tile_region (canonical plate_id/REGION) in {in_path}")

    out = df[[cols["tile_id"], cols["tile_region"]]].copy()
    out.columns = ["tile_id", "plate_id"]

    out["tile_id"] = out["tile_id"].astype(str).str.strip()
    out["plate_id"] = out["plate_id"].astype(str).str.strip()

    out = out[(out["tile_id"] != "") & (out["plate_id"] != "")].drop_duplicates(["tile_id"])

    if args.format == "csv":
        out.to_csv(out_path, index=False)
    else:
        out.to_parquet(out_path, index=False)

    print(f"[OK] wrote {out_path} rows={len(out)} (plate_id source: tile_region)")


if __name__ == "__main__":
    main()
