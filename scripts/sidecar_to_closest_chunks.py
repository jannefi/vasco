#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Formatter: sidecar (ALL) -> TAP-compatible 'closest' CSV per chunk.

Key fixes:
- Use INNER JOIN on (opt_source_id == source_id) so only rows whose seeds
  belong to *this* chunk survive.
- Do NOT backfill missing chunk_id with the path-derived CID; unmatched rows
  must be dropped, not relabeled.
- Keep --row-id-float canonicalization exactly as before.
- Write a single positions<CID>_closest.csv for the chunk referenced by --optical-root.

Usage:
  python scripts/sidecar_to_closest_chunks.py \
    --sidecar-all  ./data/local-cats/_aws_sidecar_flags/neowise_se_flags_ALL.parquet \
    --optical-root ./data/local-cats/optical_seeds/chunk_02104 \
    --out-dir      ./data/local-cats/tmp/positions/aws_compare_out \
    [--row-id-float]
"""

import argparse, os, re, sys
import pandas as pd
import pyarrow.dataset as pds
from pyarrow import fs as pafs

def _mk_s3fs():
    return pafs.S3FileSystem(anonymous=False, region="us-west-2")

def _read_optical_chunkmap(opt_root: str) -> pd.DataFrame:
    """Load mapping (source_id, row_id?, chunk_id?) from the *chunk* seeds."""
    if opt_root.startswith("s3://"):
        ds = pds.dataset(opt_root.replace("s3://", "", 1),
                         format="parquet", filesystem=_mk_s3fs())
    else:
        ds = pds.dataset(opt_root, format="parquet")

    names = set(ds.schema.names)
    want = ["source_id", "row_id", "chunk_id"]
    cols = [c for c in want if c in names]
    if not cols or "source_id" not in cols:
        raise RuntimeError(f"Optical seeds must contain 'source_id'. Got: {sorted(names)}")

    tbl = ds.to_table(columns=cols)
    df = tbl.to_pandas()
    # Ensure only this chunk’s rows are present (some seeds parquet may include chunk_id)
    return df[cols]

def _infer_chunk_id_from_path(opt_root: str) -> str:
    # e.g. '.../chunk_00005' or '.../chunk-00005' -> '00005'
    m = re.search(r"chunk[_\-]([0-9]{5})", opt_root)
    return m.group(1) if m else ""

def _to_row_id(src_val, rowid_val, as_float: bool) -> str:
    v = rowid_val if pd.notna(rowid_val) else src_val
    if pd.isna(v):
        return ""
    if as_float:
        try:
            return f"{float(v):.16g}"
        except Exception:
            return str(v)
    return str(v)

def main():
    ap = argparse.ArgumentParser(description="Format sidecar results into per-chunk closest CSV (fixed inner-join)")
    ap.add_argument("--sidecar-all", required=True)
    ap.add_argument("--optical-root", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--row-id-float", action="store_true")
    a = ap.parse_args()

    os.makedirs(a.out_dir, exist_ok=True)

    # Load sidecar (ALL) – must contain 'opt_source_id' and NEOWISE columns
    try:
        neo = pd.read_parquet(a.sidecar_all)
    except Exception as e:
        print(f"[ERROR] reading sidecar-all '{a.sidecar_all}': {e}", file=sys.stderr)
        sys.exit(2)

    if "opt_source_id" not in neo.columns:
        print("[ERROR] Sidecar file missing 'opt_source_id' column.", file=sys.stderr)
        sys.exit(2)

    # Load this chunk's seed mapping
    opt = _read_optical_chunkmap(a.optical_root)
    # Keep only the columns we need for the join and row_id
    join_cols = ["source_id"]
    if "row_id" in opt.columns:
        join_cols.append("row_id")
    opt_small = opt[join_cols].copy()

    # INNER JOIN: only seeds of THIS chunk survive
    neo = neo.merge(opt_small, left_on="opt_source_id", right_on="source_id",
                    how="inner", validate="m:1", suffixes=("", "__opt"))

    # Build output frame; prefer provided row_id; fallback to opt_source_id
    out = pd.DataFrame({
        "row_id": [ _to_row_id(s, r, a.row_id_float)
                    for s, r in zip(neo.get("opt_source_id"), neo.get("row_id")) ],
        "in_ra":        neo.get("opt_ra_deg"),
        "in_dec":       neo.get("opt_dec_deg"),
        "cntr":         neo.get("cntr"),
        "ra":           neo.get("ra"),
        "dec":          neo.get("dec"),
        "mjd":          neo.get("mjd"),
        "w1snr":        neo.get("w1snr"),
        "w2snr":        neo.get("w2snr"),
        "qual_frame":   neo.get("qual_frame"),
        "qi_fact":      neo.get("qi_fact"),
        "saa_sep":      neo.get("saa_sep"),
        "moon_masked":  neo.get("moon_masked"),
        "sep_arcsec":   neo.get("sep_arcsec"),
    })[[
        "row_id","in_ra","in_dec","cntr","ra","dec","mjd","w1snr","w2snr",
        "qual_frame","qi_fact","saa_sep","moon_masked","sep_arcsec"
    ]]

    cid = _infer_chunk_id_from_path(a.optical_root) or "unknown"

    out_csv = os.path.join(a.out_dir, f"positions{cid}_closest.csv")
    qc_txt  = os.path.join(a.out_dir, f"positions{cid}_closest.qc.txt")

    out.to_csv(out_csv, index=False, float_format="%.10g")
    with open(qc_txt, "w") as f:
        f.write(f"rows={len(out)}\n")
        f.write("columns=" + ",".join(out.columns) + "\n")

    print(f"[WRITE] {out_csv} (rows={len(out)})")

if __name__ == "__main__":
    main()