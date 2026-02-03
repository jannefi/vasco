#!/usr/bin/env python3
"""
concat_flags_and_write_sidecar.py

Scan positions roots for *_closest.csv, derive NEOWISE flags, and write a compact,
normalized Parquet sidecar for Post 1.6.

Post-1.6 FIX (IMPORTANT):
- The durable join key is row_id (string digits), NOT NUMBER.
- Do NOT rename row_id -> NUMBER (avoids Oracle/TAP reserved word pitfalls and prevents dtype corruption).
- Always read row_id as string to avoid float/scientific notation.
- Collapse to ONE ROW PER row_id (closest outputs can contain multiple rows per row_id).

Contract (output Parquet):
- row_id (string)
- has_ir_match (boolean)
- dist_arcsec (float64; min separation; null if unknown)

Usage:
  python scripts/concat_flags_and_write_sidecar.py \
    --closest-dir ./data/local-cats/tmp/positions \
    --out-root ./data/local-cats/_master_optical_parquet_irflags \
    --radius-arcsec 5.0
"""

import argparse
import os
import sys
from pathlib import Path
from typing import List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--closest-dir", required=True, help="Top-level positions folder (will recurse into /new/)")
    p.add_argument("--out-root", required=True, help="Output root for Parquet sidecar")
    p.add_argument("--radius-arcsec", type=float, default=5.0, help="IR match radius threshold")
    p.add_argument("--out-name", default="neowise_se_flags_ALL.parquet", help="Output parquet filename")
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


def _normalize_row_id_series(s: pd.Series) -> pd.Series:
    """
    Ensure row_id is preserved as string (no float parsing). We do not attempt
    lossy conversion if values already contain scientific notation; we keep
    the literal string so the pipeline can detect issues.
    """
    if s.dtype.name == "string":
        return s
    return s.astype("string")


def normalize_df(df: pd.DataFrame, radius: float) -> pd.DataFrame:
    """
    Normalize per-file schema to the contract:
    - Provide row_id (from row_id or NUMBER)
    - Provide has_ir_match (computed from distance if possible; else True for closest rows)
    - Provide dist_arcsec (float64) when available
    """
    # 1) Key column: row_id (accept NUMBER for backward compatibility, but do not emit NUMBER)
    if "row_id" in df.columns:
        df["row_id"] = _normalize_row_id_series(df["row_id"])
    elif "NUMBER" in df.columns:
        df = df.rename(columns={"NUMBER": "row_id"})
        df["row_id"] = _normalize_row_id_series(df["row_id"])
    else:
        raise ValueError("Closest CSV missing join key (neither row_id nor NUMBER).")

    # 2) Distance column -> dist_arcsec
    dist_col: Optional[str] = None
    for candidate in ("dist_arcsec", "sep_arcsec", "distance_arcsec", "separation_arcsec"):
        if candidate in df.columns:
            dist_col = candidate
            break
    if dist_col and dist_col != "dist_arcsec":
        df = df.rename(columns={dist_col: "dist_arcsec"})

    if "dist_arcsec" in df.columns:
        df["dist_arcsec"] = pd.to_numeric(df["dist_arcsec"], errors="coerce").astype("float64")

    # 3) has_ir_match
    if "has_ir_match" in df.columns:
        df["has_ir_match"] = df["has_ir_match"].astype(bool)
    elif "ir_match_strict" in df.columns:
        df["has_ir_match"] = df["ir_match_strict"].astype(bool)
    else:
        # Closest rows typically imply a candidate match exists; if dist is present, enforce radius.
        if "dist_arcsec" in df.columns:
            df["has_ir_match"] = df["dist_arcsec"].notna() & (df["dist_arcsec"] <= radius)
        else:
            df["has_ir_match"] = True

    keep = ["row_id", "has_ir_match"]
    if "dist_arcsec" in df.columns:
        keep.append("dist_arcsec")
    return df[keep].copy()


def collapse_per_row_id(df_norm: pd.DataFrame) -> pd.DataFrame:
    """
    Collapse to exactly one row per row_id:
    - has_ir_match: any(True)
    - dist_arcsec: min(dist_arcsec) among matches (NaN if none)
    """
    # Ensure row_id string
    df_norm["row_id"] = df_norm["row_id"].astype("string")

    # If dist_arcsec missing, just any(True)
    if "dist_arcsec" not in df_norm.columns:
        g = df_norm.groupby("row_id", as_index=False)["has_ir_match"].any()
        g["dist_arcsec"] = pd.NA
        return g[["row_id", "has_ir_match", "dist_arcsec"]]

    g = (
        df_norm.groupby("row_id", as_index=False)
        .agg(
            has_ir_match=("has_ir_match", "any"),
            dist_arcsec=("dist_arcsec", "min"),
        )
    )
    return g[["row_id", "has_ir_match", "dist_arcsec"]]


def main():
    args = parse_args()
    out_root = Path(args.out_root)
    out_root.mkdir(parents=True, exist_ok=True)
    out_path = out_root / args.out_name

    inputs = find_closest_csvs(Path(args.closest_dir))
    if not inputs:
        print(f"[ERROR] No *_closest.csv under: {args.closest_dir} (and /new)", file=sys.stderr)
        sys.exit(2)

    # Fixed schema for the sidecar
    schema = pa.schema(
        [
            pa.field("row_id", pa.string()),
            pa.field("has_ir_match", pa.bool_()),
            pa.field("dist_arcsec", pa.float64()),
        ]
    )

    writer = pq.ParquetWriter(out_path, schema)
    rows_in = 0
    groups_written = 0

    for i, f in enumerate(inputs, 1):
        try:
            # Force key column to remain text if present.
            # If file uses NUMBER, pandas will still read it; we normalize later.
            df = pd.read_csv(
                f,
                dtype={"row_id": "string", "NUMBER": "string"},
                low_memory=False,
            )
            df_norm = normalize_df(df, args.radius_arcsec)
            df_one = collapse_per_row_id(df_norm)
        except Exception as e:
            print(f"[WARN] Skipping {f}: {e}", file=sys.stderr)
            continue

        rows_in += df_norm.shape[0]
        groups_written += df_one.shape[0]

        tbl = pa.Table.from_pandas(df_one, schema=schema, preserve_index=False)
        writer.write_table(tbl)

        if i % 200 == 0:
            print(f"[INFO] {i} files processed; raw_rows={rows_in}; unique_row_id_rows={groups_written}")

    writer.close()
    (out_root / "_SUCCESS").write_text("ok\n", encoding="utf-8")
    print(f"[OK] Sidecar written: {out_path} (unique_row_id_rows={groups_written})")
    print(f"[OK] Marker: {out_root / '_SUCCESS'}")


if __name__ == "__main__":
    main()