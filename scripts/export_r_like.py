
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
export_r_like.py

Purpose
    Export VASCO "R_like" survivors in two policy modes:
        - Inclusive (default): retain all rows; annotate edge info if provided.
        - Core-only: exclude plate-edge tiles (near_edge|edge_touch|off_plate).

Inputs
    --masked PATH
        Parquet dataset/file containing the masked survivors after union gates
        (VOSA-like, SCOS, PTF ngood, VSX, optional SkyBoT). May be a partitioned
        Parquet directory.

    --edge-report PATH (optional)
        CSV from check_tile_plate_edge.py with columns including
        tile_id, plate_id, class_arcsec/class_px, min_edge_dist_px, min_edge_dist_arcsec.

    --core-only {true,false}
        If true, filter to edge_class == 'core'. If no edge-report is supplied,
        this option has no effect (warns and proceeds Inclusive).

    --out PATH
        Output Parquet file path. A sidecar JSON with simple metrics will be
        written alongside (same base name + .metrics.json).

    --emit-csv {true,false} (default: true)
        Emit a CSV alongside Parquet for external tools (e.g., STILTS parity).

    --csv-out PATH (optional)
        Explicit path for CSV; if omitted and emit-csv=true, uses --out with
        extension changed to .csv.

    --keep-cols COL1,COL2,... (optional)
        Additional columns to keep verbatim if present.

Notes
    * The script tries to normalize RA/Dec columns to 'RA' and 'Dec' in the
      output, sourcing from common variants (RA_corr/Dec_corr, ALPHAWIN_J2000/
      DELTAWIN_J2000, ra/dec, etc.).
    * Joins edge-report using (tile_id, plate_id). If class_arcsec is present it
      is preferred; otherwise class_px.

"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import List, Tuple

import pandas as pd

# ----------------------------- helpers -----------------------------------

COMMON_RA_CANDIDATES = [
    'RA', 'ra', 'RA_corr', 'ALPHAWIN_J2000', 'ra_deg', 'alpha_j2000'
]
COMMON_DEC_CANDIDATES = [
    'Dec', 'DEC', 'dec', 'Dec_corr', 'DEC_corr', 'DELTAWIN_J2000', 'dec_deg', 'delta_j2000'
]


def pick_column(df: pd.DataFrame, cands: List[str]) -> str:
    for c in cands:
        if c in df.columns:
            return c
    return ''


def normalize_radec(df: pd.DataFrame) -> pd.DataFrame:
    ra_col = pick_column(df, COMMON_RA_CANDIDATES)
    de_col = pick_column(df, COMMON_DEC_CANDIDATES)
    out = df.copy()
    if ra_col and 'RA' not in out.columns:
        out = out.rename(columns={ra_col: 'RA'})
    if de_col and 'Dec' not in out.columns:
        out = out.rename(columns={de_col: 'Dec'})
    return out


def parse_keep_cols(arg: str) -> List[str]:
    if not arg:
        return []
    return [c.strip() for c in arg.split(',') if c.strip()]


# ---------------------------- core logic ----------------------------------

def load_parquet_any(path: str) -> pd.DataFrame:
    p = Path(path)
    if p.is_dir():
        return pd.read_parquet(p, engine='pyarrow')
    return pd.read_parquet(p)


def main():
    ap = argparse.ArgumentParser(description='Export VASCO R_like survivors (inclusive/core-only)')
    ap.add_argument('--masked', required=True, help='Path to masked survivors Parquet (dir or file)')
    ap.add_argument('--edge-report', default='', help='Path to tile_plate_edge_report.csv (optional)')
    ap.add_argument('--core-only', default='false', choices=['true', 'false'], help='Filter to edge core only')
    ap.add_argument('--out', required=True, help='Output Parquet file path')
    ap.add_argument('--emit-csv', default='true', choices=['true', 'false'], help='Also write CSV next to Parquet')
    ap.add_argument('--csv-out', default='', help='Explicit CSV output path (optional)')
    ap.add_argument('--keep-cols', default='', help='Comma-separated extra columns to keep')

    args = ap.parse_args()

    core_only = (args.core_only.lower() == 'true')
    emit_csv = (args.emit_csv.lower() == 'true')
    out_parq = Path(args.out)
    out_parq.parent.mkdir(parents=True, exist_ok=True)

    print(f"[INFO] Loading masked survivors from: {args.masked}")
    df = load_parquet_any(args.masked)
    print(f"[INFO] Masked survivors: {len(df):,} rows, {len(df.columns)} columns")

    # Merge edge report if provided
    edge_class_used = ''
    if args.edge_report:
        er_path = Path(args.edge_report)
        if not er_path.exists():
            print(f"[WARN] Edge-report not found: {er_path} — proceeding without it (Inclusive)")
        else:
            er = pd.read_csv(er_path)
            # prefer class_arcsec over class_px
            if 'class_arcsec' in er.columns:
                er = er.rename(columns={'class_arcsec': 'edge_class'})
                edge_class_used = 'class_arcsec'
            elif 'class_px' in er.columns:
                er = er.rename(columns={'class_px': 'edge_class'})
                edge_class_used = 'class_px'
            else:
                er['edge_class'] = 'unknown'
                edge_class_used = 'unknown'

            keep_er_cols = ['tile_id', 'plate_id', 'edge_class']
            for c in ['min_edge_dist_px', 'min_edge_dist_arcsec']:
                if c in er.columns:
                    keep_er_cols.append(c)
            er = er[keep_er_cols].drop_duplicates()

            # Key join: (tile_id, plate_id)
            if not {'tile_id', 'plate_id'}.issubset(df.columns):
                print("[WARN] masked survivors missing (tile_id, plate_id); cannot join edge report — proceeding without it")
            else:
                df = df.merge(er, on=['tile_id', 'plate_id'], how='left')
                print(f"[INFO] Edge-report merged using {edge_class_used}; nulls mean 'not classified' ")

    # Edge policy filtering
    if core_only:
        if 'edge_class' not in df.columns:
            print("[WARN] --core-only requested but edge_class missing; proceeding without filtering (Inclusive)")
        else:
            before = len(df)
            df = df[df['edge_class'].fillna('core') == 'core'].copy()
            print(f"[INFO] Core-only filter applied: {before:,} -> {len(df):,}")

    # Normalize RA/Dec and pick output column set
    df = normalize_radec(df)
    base_cols = []
    for c in ['tile_id', 'plate_id', 'RA', 'Dec']:
        if c in df.columns:
            base_cols.append(c)
    # carry common identifiers if present
    for c in ['NUMBER', 'row_id', 'plate_epoch_mjd', 'epoch_mjd', 'DATE_OBS']:
        if c in df.columns:
            base_cols.append(c)
    # carry edge info if present
    for c in ['edge_class', 'min_edge_dist_px', 'min_edge_dist_arcsec']:
        if c in df.columns:
            base_cols.append(c)
    # carry user requested keep-cols
    for c in parse_keep_cols(args.keep_cols):
        if c in df.columns and c not in base_cols:
            base_cols.append(c)

    out_df = df[base_cols].copy()

    # Write Parquet
    out_df.to_parquet(out_parq, index=False)
    print(f"[OK] Parquet written: {out_parq} ({len(out_df):,} rows)")

    # Emit CSV for parity if requested
    if emit_csv:
        csv_path = Path(args.csv_out) if args.csv_out else out_parq.with_suffix('.csv')
        out_df.to_csv(csv_path, index=False)
        print(f"[OK] CSV written: {csv_path}")

    # Simple metrics sidecar
    metrics = {
        'rows': int(len(out_df)),
        'core_only': bool(core_only),
        'edge_policy': 'core' if core_only else 'inclusive',
        'edge_class_source': edge_class_used or 'none',
        'source_masked': str(Path(args.masked).resolve()),
        'edge_report': str(Path(args.edge_report).resolve()) if args.edge_report else '',
        'parquet': str(out_parq.resolve()),
        'csv': str((Path(args.csv_out).resolve() if args.csv_out else out_parq.with_suffix('.csv').resolve())) if emit_csv else ''
    }
    with open(out_parq.with_suffix('.metrics.json'), 'w', encoding='utf-8') as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    print(f"[OK] Metrics sidecar written: {out_parq.with_suffix('.metrics.json')}")


if __name__ == '__main__':
    main()
