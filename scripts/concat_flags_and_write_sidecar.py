
#!/usr/bin/env python3
"""
concat_flags_and_write_sidecar.py

Scan positions roots for *_closest.csv, derive NEOWISE flags, and write a
compact, **normalized** Parquet sidecar for Post 1.6:

- Join key:  NUMBER        (always written)
- IR flag:   has_ir_match  (boolean)
- Distance:  dist_arcsec   (float, optional if unknown)

Searches both ./data/local-cats/tmp/positions and ./data/local-cats/tmp/positions/new.
Streams one file at a time; no giant concatenations.

Usage:
  python scripts/concat_flags_and_write_sidecar.py \
    --closest-dir ./data/local-cats/tmp/positions \
    --master-root ./data/local-cats/_master_optical_parquet \
    --out-root ./data/local-cats/_master_optical_parquet_irflags \
    --radius-arcsec 5.0
"""
import argparse, os, sys
from pathlib import Path
from typing import List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--closest-dir", required=True, help="Top-level positions folder (will recurse into /new/)")
    p.add_argument("--master-root", required=False, help="Unused here, reserved for future validations")
    p.add_argument("--out-root", required=True, help="Output root for Parquet sidecar")
    p.add_argument("--radius-arcsec", type=float, default=5.0, help="IR match radius threshold")
    return p.parse_args()

def find_closest_csvs(root: Path) -> List[Path]:
    files: List[Path] = []
    for base in (root, root / "new"):
        if not base.exists():
            continue
        for dirpath, _dirnames, filenames in os.walk(base):
            for fn in filenames:
                if fn.endswith("_closest.csv"):
                    files.append(Path(dirpath) / fn)
    files.sort()
    return files

def normalize_df(df: pd.DataFrame, radius: float) -> pd.DataFrame:
    """
    Normalize per-file schema to the contract:
    - Provide NUMBER (from row_id or NUMBER)
    - Provide has_ir_match (from ir_match_strict or computed from distance)
    - Provide dist_arcsec (float) when available
    """
    # 1) Key column NUMBER
    if 'NUMBER' in df.columns:
        key = 'NUMBER'
    elif 'row_id' in df.columns:
        df = df.rename(columns={'row_id': 'NUMBER'})
        key = 'NUMBER'
    else:
        # Last resort: if the file contains only one row per input record and has any numeric index column
        raise ValueError("Closest CSV missing join key (neither NUMBER nor row_id).")

    # 2) Distance column
    dist_col: Optional[str] = None
    for candidate in ('dist_arcsec', 'sep_arcsec', 'distance_arcsec', 'separation_arcsec'):
        if candidate in df.columns:
            dist_col = candidate
            break
    if dist_col and dist_col != 'dist_arcsec':
        df = df.rename(columns={dist_col: 'dist_arcsec'})

    # 3) IR match flag
    if 'has_ir_match' in df.columns:
        df['has_ir_match'] = df['has_ir_match'].astype(bool)
    elif 'ir_match_strict' in df.columns:
        df['has_ir_match'] = df['ir_match_strict'].astype(bool)
    else:
        # Compute from distance if present; otherwise assume True (closest implies match)
        if 'dist_arcsec' in df.columns:
            df['has_ir_match'] = pd.to_numeric(df['dist_arcsec'], errors='coerce').notna() & (
                pd.to_numeric(df['dist_arcsec'], errors='coerce') <= radius
            )
        else:
            df['has_ir_match'] = True

    # 4) Minimal normalized view
    keep = ['NUMBER', 'has_ir_match']
    if 'dist_arcsec' in df.columns:
        df['dist_arcsec'] = pd.to_numeric(df['dist_arcsec'], errors='coerce')
        keep.append('dist_arcsec')
    return df[keep].copy()

def main():
    args = parse_args()
    out_root = Path(args.out_root)
    out_root.mkdir(parents=True, exist_ok=True)
    out_path = out_root / "neowise_se_flags_ALL.parquet"

    inputs = find_closest_csvs(Path(args.closest_dir))
    if not inputs:
        print(f"[ERROR] No *_closest.csv under: {args.closest_dir} (and /new)", file=sys.stderr)
        sys.exit(2)

    writer = None
    rows = 0
    for i, f in enumerate(inputs, 1):
        try:
            df = pd.read_csv(f)
            df_norm = normalize_df(df, args.radius_arcsec)
        except Exception as e:
            print(f"[WARN] Skipping {f}: {e}", file=sys.stderr)
            continue

        tbl = pa.Table.from_pandas(df_norm, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(out_path, tbl.schema)
        writer.write_table(tbl)
        rows += df_norm.shape[0]
        if i % 100 == 0:
            print(f"[INFO] {i} files processed; rows so far = {rows}")

    if writer is None:
        print("[ERROR] No valid closest CSVs processed; aborting.", file=sys.stderr)
        sys.exit(3)

    writer.close()
    (out_root / "_SUCCESS").write_text("ok\n", encoding="utf-8")
    print(f"[OK] Flags written: {out_path} (rows={rows})")
    print(f"[OK] Marker: {out_root / '_SUCCESS'}")

if __name__ == "__main__":
    main()
