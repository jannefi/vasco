
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CSV → Parquet (Hive-style partitions: ra_bin=XX/dec_bin=YY/) with ZSTD compression.
Writes **one file per partition at a time** to avoid 'Too many open files' (errno 24).

Usage (when amount of data grows, try bin-deg 2)
  python scripts/make_master_optical_parquet.py \
    --csv data/tiles/_master_tile_catalog_pass2.csv \
    --out data/local-cats/_master_optical_parquet \
    --bin-deg 5 \
    --chunksize 500000
"""

import argparse
from pathlib import Path
import numpy as np
import pandas as pd

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception as e:
    raise SystemExit(f"[ERROR] pyarrow is required: {e}")

CAND_RA  = ["ALPHA_J2000", "RA", "X_WORLD", "RAJ2000", "ra"]
CAND_DEC = ["DELTA_J2000", "DEC", "Y_WORLD", "DEJ2000", "dec"]

def detect_radec_columns(csv_path: Path):
    probe = pd.read_csv(csv_path, nrows=1)
    ra = next((c for c in CAND_RA  if c in probe.columns), None)
    de = next((c for c in CAND_DEC if c in probe.columns), None)
    if not ra or not de:
        raise ValueError(f"Could not find RA/Dec in {csv_path}; columns: {list(probe.columns)}")
    extra = [c for c in ["tile_id", "image_catalog_path", "source_file"] if c in probe.columns]
    return ra, de, extra

def add_bins(df: pd.DataFrame, ra_col: str, de_col: str, bin_deg: float):
    df[ra_col] = df[ra_col].astype("float32")
    df[de_col] = df[de_col].astype("float32")
    ra = df[ra_col].to_numpy(dtype=np.float32) % 360.0
    de = df[de_col].to_numpy(dtype=np.float32)
    df["ra_bin"]  = np.floor(ra / np.float32(bin_deg)).astype("int16")
    df["dec_bin"] = np.floor((de + 90.0) / np.float32(bin_deg)).astype("int16")
    return df

def write_partition_file(root: Path, ra_bin: int, dec_bin: int, df_part: pd.DataFrame, file_tag: str):
    # Ensure directories exist
    part_dir = root / f"ra_bin={ra_bin}" / f"dec_bin={dec_bin}"
    part_dir.mkdir(parents=True, exist_ok=True)
    # Use deterministic file names; one file per group per chunk/tag
    file_path = part_dir / f"part-{file_tag}.parquet"
    # Convert to Arrow with index dropped
    table = pa.Table.from_pandas(df_part, preserve_index=False)
    # Single-file write; opened and closed immediately
    pq.write_table(table, str(file_path), compression="zstd", use_dictionary=True)
    return file_path

def csv_to_parquet_sequential(csv_path: Path, out_root: Path,
                              ra_col: str, de_col: str, extra_cols: list[str],
                              bin_deg: float, chunksize: int):
    out_root.mkdir(parents=True, exist_ok=True)
    usecols = [ra_col, de_col] + extra_cols

    reader = pd.read_csv(csv_path, usecols=usecols, chunksize=chunksize,
                         dtype={ra_col: "float32", de_col: "float32"})
    total_rows = 0
    chunk_idx = 0

    for df in reader:
        chunk_idx += 1
        df = add_bins(df, ra_col, de_col, bin_deg)

        # Group by (ra_bin, dec_bin) and write each group to a file, sequentially
        # This guarantees only **one file handle** open at a time.
        written = 0
        for (rb, db), sub in df.groupby(["ra_bin", "dec_bin"], sort=False):
            if sub.empty:
                continue
            tag = f"{chunk_idx:05d}-{rb}-{db}"
            write_partition_file(out_root, int(rb), int(db), sub, tag)
            written += len(sub)

        total_rows += len(df)
        print(f"[WRITE] chunk={chunk_idx:05d} rows={len(df):8d} groups={written:8d} total={total_rows:10d}")

    print(f"[DONE] Wrote ~{total_rows} rows to {out_root} (bin_deg={bin_deg}°, chunksize={chunksize}).")

def main():
    ap = argparse.ArgumentParser(description="CSV → Parquet partitions (sequential writes).")
    ap.add_argument("--csv", required=True, help="Path to _master_tile_catalog_pass2.csv")
    ap.add_argument("--out", required=True, help="Output Parquet dataset root")
    ap.add_argument("--bin-deg", type=float, default=5.0, help="Bin size (degrees)")
    ap.add_argument("--chunksize", type=int, default=500000, help="CSV read chunk size")
    args = ap.parse_args()

    csv_path = Path(args.csv)
    out_root = Path(args.out)
    ra_col, de_col, extra_cols = detect_radec_columns(csv_path)
    print(f"[INFO] RA/Dec: {ra_col}/{de_col}; extras: {extra_cols or 'none'}")

    csv_to_parquet_sequential(csv_path, out_root, ra_col, de_col, extra_cols, args.bin_deg, args.chunksize)

if __name__ == "__main__":
    main()
