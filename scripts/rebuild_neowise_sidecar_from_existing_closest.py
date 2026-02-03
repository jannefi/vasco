#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Rebuild NEOWISE IR sidecar from existing *_closest.csv outputs (no TAP).

- Normalizes row_id from scientific notation to digit-string using Decimal (no float).
- Aggregates to one row per row_id: min separation.
- Optionally left-joins against a seed list (positions chunks) to produce a full-length sidecar.
"""

import argparse
import os
from pathlib import Path
from decimal import Decimal, InvalidOperation, getcontext

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

getcontext().prec = 50  # plenty for 64-bit ids expressed in scientific notation

def iter_closest_files(root: Path):
    for base in (root, root / "new"):
        if not base.exists():
            continue
        for dirpath, _, filenames in os.walk(base):
            for fn in filenames:
                if fn.endswith("_closest.csv"):
                    yield Path(dirpath) / fn

def canonical_row_id(s: str):
    """
    Convert row_id string possibly in scientific notation to an integer digit string.
    Returns None if cannot be parsed to an integer cleanly.
    """
    if s is None:
        return None
    s = str(s).strip()
    if s == "" or s.lower() == "nan":
        return None
    try:
        d = Decimal(s)
    except InvalidOperation:
        return None
    # Ensure it is an integer
    if d != d.to_integral_value():
        # Some representations might be like 1.23e19 with fractional -> still integral after scaling,
        # but Decimal already accounts for exponent; if it's not integral here, skip.
        return None
    # Convert to plain integer string
    return format(int(d), "d")

def load_seed_row_ids(seed_dir: Path):
    """
    Load all row_id values from seed positions chunks. Expect columns: row_id,ra,dec.
    Returns a DataFrame with unique row_id.
    """
    files = sorted((seed_dir).rglob("positions_chunk_*.csv"))
    if not files:
        raise RuntimeError(f"No positions_chunk_*.csv under {seed_dir}")
    frames = []
    for f in files:
        df = pd.read_csv(f, usecols=["row_id"], dtype={"row_id": "string"})
        frames.append(df)
    all_ids = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["row_id"])
    all_ids["row_id"] = all_ids["row_id"].astype("string")
    return all_ids

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--closest-dir", required=True, help="Folder containing *_closest.csv (will recurse into /new/ too)")
    ap.add_argument("--out-parquet", required=True, help="Output parquet path")
    ap.add_argument("--radius-arcsec", type=float, default=5.0)
    ap.add_argument("--seed-dir", default="", help="Optional: folder with positions_chunk_*.csv to make full sidecar")
    args = ap.parse_args()

    closest_dir = Path(args.closest_dir)
    out_parquet = Path(args.out_parquet)
    out_parquet.parent.mkdir(parents=True, exist_ok=True)

    # Aggregate matches
    agg = {}  # row_id -> min_sep
    n_files = 0
    n_rows = 0
    for f in iter_closest_files(closest_dir):
        n_files += 1
        df = pd.read_csv(f, dtype={"row_id": "string"}, low_memory=False)
        if "row_id" not in df.columns:
            continue
        sep_col = "sep_arcsec" if "sep_arcsec" in df.columns else ("dist_arcsec" if "dist_arcsec" in df.columns else None)
        if sep_col is None:
            continue
        # canonicalize row_id safely
        ids = df["row_id"].astype("string").map(canonical_row_id)
        seps = pd.to_numeric(df[sep_col], errors="coerce")

        for rid, sep in zip(ids, seps):
            n_rows += 1
            if rid is None or pd.isna(sep):
                continue
            if sep > args.radius_arcsec:
                continue
            prev = agg.get(rid)
            if prev is None or sep < prev:
                agg[rid] = float(sep)

        if n_files % 500 == 0:
            print(f"[INFO] processed {n_files} closest files; current unique matches={len(agg)}")

    print(f"[INFO] scanned closest files={n_files}; rows_seen~={n_rows}; unique_matched_row_id={len(agg)}")

    match_df = pd.DataFrame({"row_id": list(agg.keys()), "dist_arcsec": list(agg.values())})
    match_df["row_id"] = match_df["row_id"].astype("string")
    match_df["has_ir_match"] = True

    # If we have seed dir, left join to make full sidecar (row_id list from master)
    if args.seed_dir:
        seed_df = load_seed_row_ids(Path(args.seed_dir))
        out_df = seed_df.merge(match_df, on="row_id", how="left")
        out_df["has_ir_match"] = out_df["has_ir_match"].fillna(False).astype(bool)
    else:
        # matches-only sidecar; downstream must treat missing row_id as False
        out_df = match_df[["row_id", "has_ir_match", "dist_arcsec"]]

    # Ensure columns exist
    if "dist_arcsec" not in out_df.columns:
        out_df["dist_arcsec"] = pd.NA

    out_df = out_df[["row_id", "has_ir_match", "dist_arcsec"]]
    schema = pa.schema([
        pa.field("row_id", pa.string()),
        pa.field("has_ir_match", pa.bool_()),
        pa.field("dist_arcsec", pa.float64()),
    ])
    table = pa.Table.from_pandas(out_df, schema=schema, preserve_index=False)
    pq.write_table(table, out_parquet)
    print(f"[OK] wrote sidecar: {out_parquet} rows={len(out_df)}")

if __name__ == "__main__":
    main()
