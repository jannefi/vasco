#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Merge SkyBoT parts into canonical flags without DuckDB.

Inputs:
  <outroot>/parts/flags_skybot__*.parquet

Outputs:
  <outroot>/flags_skybot.parquet        (canonical; 1 row per id)
  <outroot>/flags_skybot_audit.parquet  (full concatenation of parts)

Aggregation:
  id: src_id (preferred) else row_id
  has_skybot_match: OR/ANY
  wide_skybot_match: OR/ANY (if column exists)
  best_sep_arcsec_min: MIN(best_sep_arcsec) (if column exists)

This stays compatible with existing skybot_fetch_chunk.py which writes Parquet parts. 
"""

import argparse
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-root", required=True, help="SkyBoT outroot containing parts/")
    ap.add_argument("--pattern", default="flags_skybot__*.parquet", help="Parts filename glob")
    ap.add_argument("--compression", default="zstd", help="Parquet compression")
    args = ap.parse_args()

    outroot = Path(args.out_root)
    parts_dir = outroot / "parts"
    if not parts_dir.exists():
        raise SystemExit(f"[ERROR] parts dir missing: {parts_dir}")

    files = sorted(parts_dir.glob(args.pattern))
    if not files:
        raise SystemExit(f"[ERROR] no part files matching {parts_dir}/{args.pattern}")

    # Build dataset from explicit file list
    dataset = ds.dataset([str(p) for p in files], format="parquet")
    names = set(dataset.schema.names)

    id_col = "src_id" if "src_id" in names else ("row_id" if "row_id" in names else None)
    if not id_col:
        raise SystemExit("[ERROR] parts missing both src_id and row_id")

    cols = [id_col]
    if "has_skybot_match" in names:
        cols.append("has_skybot_match")
    else:
        raise SystemExit("[ERROR] parts missing has_skybot_match")

    if "wide_skybot_match" in names:
        cols.append("wide_skybot_match")

    if "best_sep_arcsec" in names:
        cols.append("best_sep_arcsec")

    # Audit = concatenation of projected columns (plus any extras if you want)
    audit_tbl = dataset.to_table(columns=list(names))  # full audit
    audit_out = outroot / "flags_skybot_audit.parquet"
    pq.write_table(audit_tbl, audit_out, compression=args.compression)

    # Canonical aggregation
    tbl = dataset.to_table(columns=cols)

    gb = tbl.group_by(id_col)

    aggs = [("has_skybot_match", "any")]
    if "wide_skybot_match" in cols:
        aggs.append(("wide_skybot_match", "any"))
    if "best_sep_arcsec" in cols:
        aggs.append(("best_sep_arcsec", "min"))

    canon = gb.aggregate(aggs)

    # Rename outputs to match prior duckdb naming
    new_names = []
    for n in canon.schema.names:
        if n == "best_sep_arcsec_min":
            new_names.append("best_sep_arcsec_min")
        elif n == "best_sep_arcsec_min":  # (some versions already)
            new_names.append(n)
        else:
            new_names.append(n)
    canon = canon.rename_columns(new_names)

    canon_out = outroot / "flags_skybot.parquet"
    pq.write_table(canon, canon_out, compression=args.compression)

    print(f"[OK] merged parts={len(files)} -> {canon_out} rows={canon.num_rows}; audit={audit_out} rows={audit_tbl.num_rows}")


if __name__ == "__main__":
    main()
