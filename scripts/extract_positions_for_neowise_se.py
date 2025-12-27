
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Extract (row_id, ra, dec) from a partitioned Parquet master dataset and
optionally fan out NEOWISE-SE TAP jobs in strict replication mode.

Usage (positions only):
  python extract_positions_for_neowise_se.py \
      --parquet-root ./data/local-cats/_master_optical_parquet \
      --out-dir ./data/local-cats/tmp/positions \
      --chunk-size 2000

Usage (positions + run TAP per chunk):
  python extract_positions_for_neowise_se.py \
      --parquet-root ./data/local-cats/_master_optical_parquet \
      --out-dir ./data/local-cats/tmp/positions \
      --chunk-size 2000 \
      --run-neowise \
      --neowise-script ./scripts/xmatch_neowise_single_exposure.py \
      --neowise-out-dir ./data/local-cats/out/neowise_se

Notes:
- RA/Dec precedence: ALPHAWIN_J2000/DELTAWIN_J2000 first; fallback X_WORLD/Y_WORLD.
- If 'row_id' is missing, we generate a stable one:
  hash(tile_id, local_index) when tile_id is present; else a monotonic integer.
- Writes chunked CSVs named positions_chunk_00001.csv, etc.
- When --run-neowise is set, it invokes the TAP script per chunk with mjd<=59198.
"""

import argparse
import os
import re
import sys
import hashlib
import subprocess
from pathlib import Path

import pandas as pd

def find_parquet_parts(root: Path):
    """
    Enumerate part files under ra_bin=XX/dec_bin=YY/ directories.
    Accepts any .parquet under the tree.
    """
    return sorted(root.rglob("*.parquet"))

def autodetect_columns(df: pd.DataFrame):
    """
    Determine RA/Dec column names and presence of row_id/tile_id.
    Returns (ra_col, dec_col, has_row_id, tile_col or None).
    """
    # Preferred
    ra_candidates = ["ALPHAWIN_J2000", "ALPHA_J2000", "X_WORLD", "alpha", "ra"]
    dec_candidates = ["DELTAWIN_J2000", "DELTA_J2000", "Y_WORLD", "delta", "dec"]

    ra_col = next((c for c in ra_candidates if c in df.columns), None)
    dec_col = next((c for c in dec_candidates if c in df.columns), None)

    has_row_id = "row_id" in df.columns

    tile_col = None
    for c in ("tile_id", "tile", "tile_name"):
        if c in df.columns:
            tile_col = c
            break

    return ra_col, dec_col, has_row_id, tile_col

def stable_row_id(tile_id: str, local_index: int) -> int:
    """
    Produce a stable 64-bit integer row_id from tile_id and a local index.
    """
    h = hashlib.sha1(f"{tile_id}:{local_index}".encode("utf-8")).digest()
    # Take first 8 bytes for 64-bit, interpret as unsigned
    return int.from_bytes(h[:8], byteorder="big", signed=False)

def load_positions_from_part(part_path: Path):
    """
    Load minimal columns from a parquet part: RA/Dec + optional row_id + tile_id.
    Returns a DataFrame with columns ['row_id','ra','dec'].
    """
    # Read only the columns we may need; if unknown, read full then subset
    df = pd.read_parquet(part_path)
    ra_col, dec_col, has_row_id, tile_col = autodetect_columns(df)

    if ra_col is None or dec_col is None:
        raise RuntimeError(f"Could not find RA/Dec columns in {part_path}")

    # Build the output frame
    out = pd.DataFrame({
        "ra": df[ra_col].astype("float64"),
        "dec": df[dec_col].astype("float64"),
    })

    # Row id
    if has_row_id:
        out["row_id"] = df["row_id"].astype("int64")
    else:
        # Try to use tile_id + local index
        if tile_col is not None:
            # local index within this part
            local_idx = pd.RangeIndex(start=0, stop=len(out), step=1)
            # Tile id could be per-row or constant; handle both
            tile_series = df[tile_col].astype(str).fillna("unknown")
            # If constant for the entire part, speed up
            if len(tile_series.unique()) == 1:
                tile_const = tile_series.iloc[0]
                out["row_id"] = [stable_row_id(tile_const, i) for i in local_idx]
            else:
                out["row_id"] = [
                    stable_row_id(tile_series.iloc[i], i) for i in local_idx
                ]
        else:
            # Last resort: monotonic sequence (discouraged; use only if needed)
            out["row_id"] = pd.RangeIndex(start=0, stop=len(out), step=1).astype("int64")

    # Drop invalid rows (NaNs)
    out = out.dropna(subset=["ra", "dec"]).reset_index(drop=True)
    return out

def write_chunks(df_all: pd.DataFrame, out_dir: Path, chunk_size: int):
    out_dir.mkdir(parents=True, exist_ok=True)
    chunks = []
    counter = 1
    for start in range(0, len(df_all), chunk_size):
        chunk = df_all.iloc[start:start+chunk_size].copy()
        fname = out_dir / f"positions_chunk_{counter:05d}.csv"
        chunk[["row_id","ra","dec"]].to_csv(fname, index=False)
        chunks.append(fname)
        counter += 1
    return chunks

def run_neowise_per_chunk(neowise_script: Path, chunk_path: Path, out_dir: Path,
                          radius_arcsec=5.0, mjd_cap=59198, snr=5.0, chunk_size=2000, sleep=1.0):
    """
    Invoke xmatch_neowise_single_exposure.py for a single chunk.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    # Derive output name from chunk name
    stem = chunk_path.stem.replace("positions_chunk_", "neowise_se_matches_")
    out_csv = out_dir / f"{stem}.csv"

    cmd = [
        sys.executable, str(neowise_script),
        "--in-csv", str(chunk_path),
        "--out-csv", str(out_csv),
        "--radius-arcsec", str(radius_arcsec),
        "--mjd-cap", str(mjd_cap),
        "--snr", str(snr),
        "--chunk-size", str(chunk_size),
        "--sleep", str(sleep),
    ]
    subprocess.run(cmd, check=True)
    return out_csv

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet-root", required=True,
                    help="Root of the master Parquet dataset (partitioned by ra_bin=XX/dec_bin=YY).")
    ap.add_argument("--out-dir", required=True,
                    help="Directory to write positions chunks (CSV).")
    ap.add_argument("--chunk-size", type=int, default=2000,
                    help="Rows per positions chunk CSV (default: 2000).")
    ap.add_argument("--run-neowise", action="store_true",
                    help="If set, invoke NEOWISE-SE TAP script per chunk after writing positions.")
    ap.add_argument("--neowise-script", default="./scripts/xmatch_neowise_single_exposure.py",
                    help="Path to the NEOWISE-SE TAP script.")
    ap.add_argument("--neowise-out-dir", default="./data/local-cats/out/neowise_se",
                    help="Directory to write NEOWISE-SE match CSVs.")
    ap.add_argument("--radius-arcsec", type=float, default=5.0)
    ap.add_argument("--mjd-cap", type=int, default=59198)
    ap.add_argument("--snr", type=float, default=5.0)
    ap.add_argument("--sleep", type=float, default=1.0)
    args = ap.parse_args()

    root = Path(args.parquet_root)
    out_dir = Path(args.out_dir)
    parts = find_parquet_parts(root)
    if not parts:
        raise SystemExit(f"No parquet files found under {root}")

    frames = []
    for p in parts:
        try:
            frames.append(load_positions_from_part(p))
        except Exception as e:
            print(f"[WARN] Skipping {p}: {e}", file=sys.stderr)

    if not frames:
        raise SystemExit("No positions could be extracted (missing RA/Dec?)")

    df_all = pd.concat(frames, ignore_index=True)

    # Deduplicate on row_id if present
    if "row_id" in df_all.columns:
        df_all = df_all.drop_duplicates(subset=["row_id"]).reset_index(drop=True)

    chunks = write_chunks(df_all, out_dir, args.chunk_size)
    print(f"[INFO] Wrote {len(chunks)} positions chunk(s) to {out_dir}")

    if args.run_neowise:
        neo_out = Path(args.neowise_out_dir)
        produced = []
        for c in chunks:
            out_csv = run_neowise_per_chunk(
                neowise_script=Path(args.neowise_script),
                chunk_path=Path(c),
                out_dir=neo_out,
                radius_arcsec=args.radius_arcsec,
                mjd_cap=args.mjd_cap,
                snr=args.snr,
                chunk_size=args.chunk_size,
                sleep=args.sleep
            )
            produced.append(out_csv)
            print(f"[INFO] TAP results -> {out_csv}")
        print(f"[INFO] Completed NEOWISE-SE runs for {len(produced)} chunk(s).")

if __name__ == "__main__":
    main()

