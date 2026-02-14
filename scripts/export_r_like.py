# ./scripts/export_r_like.py  (v1.1)
# - Robust directory read (ignores non-Parquet files)
# - Inclusive/Core-only with optional edge annotation
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
export_r_like.py (v1.1)
- Robust to directories that contain non-parquet sidecars
- Accepts either a single Parquet file or a partitioned directory.
"""
import argparse, json
from pathlib import Path
from typing import List
import pandas as pd

COMMON_RA_CANDIDATES = ['RA','ra','RA_corr','ALPHAWIN_J2000','ra_deg','alpha_j2000']
COMMON_DEC_CANDIDATES = ['Dec','DEC','dec','Dec_corr','DEC_corr','DELTAWIN_J2000','dec_deg','delta_j2000']

def pick_column(df: pd.DataFrame, cands: List[str]) -> str:
    for c in cands:
        if c in df.columns:
            return c
    return ''

def normalize_radec(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    ra_col = pick_column(out, COMMON_RA_CANDIDATES)
    de_col = pick_column(out, COMMON_DEC_CANDIDATES)
    if ra_col and 'RA' not in out.columns:
        out = out.rename(columns={ra_col: 'RA'})
    if de_col and 'Dec' not in out.columns:
        out = out.rename(columns={de_col: 'Dec'})
    return out

def load_parquet_any(path: str) -> pd.DataFrame:
    p = Path(path)
    if p.is_dir():
        files = sorted(p.rglob('*.parquet'))
        if not files:
            raise FileNotFoundError(f'No .parquet files under {p}')
        dfs = [pd.read_parquet(f) for f in files]
        return pd.concat(dfs, ignore_index=True)
    return pd.read_parquet(p)

def main():
    ap = argparse.ArgumentParser(description='Export VASCO R_like survivors (inclusive/core-only)')
    ap.add_argument('--masked', required=True, help='Path to masked survivors Parquet (dir or file)')
    ap.add_argument('--edge-report', default='', help='Path to tile_plate_edge_report.csv (optional)')
    ap.add_argument('--core-only', default='false', choices=['true','false'])
    ap.add_argument('--out', required=True)
    ap.add_argument('--emit-csv', default='true', choices=['true','false'])
    ap.add_argument('--csv-out', default='')
    ap.add_argument('--keep-cols', default='')
    args = ap.parse_args()

    core_only = (args.core_only.lower() == 'true')
    emit_csv  = (args.emit_csv.lower() == 'true')

    print(f"[INFO] Loading masked survivors from: {args.masked}")
    df = load_parquet_any(args.masked)
    print(f"[INFO] Masked survivors: {len(df):,} rows, {len(df.columns)} cols")

    # Edge annotation
    edge_src = ''
    if args.edge_report:
        er_path = Path(args.edge_report)
        if er_path.exists():
            er = pd.read_csv(er_path)
            if 'class_arcsec' in er.columns:
                er = er.rename(columns={'class_arcsec': 'edge_class'}); edge_src='class_arcsec'
            elif 'class_px' in er.columns:
                er = er.rename(columns={'class_px': 'edge_class'}); edge_src='class_px'
            else:
                er['edge_class'] = 'unknown'; edge_src='unknown'
            keep = ['tile_id','plate_id','edge_class']
            for c in ['min_edge_dist_px','min_edge_dist_arcsec']:
                if c in er.columns: keep.append(c)
            er = er[keep].drop_duplicates()
            if {'tile_id','plate_id'}.issubset(df.columns):
                df = df.merge(er, on=['tile_id','plate_id'], how='left')
                print(f"[INFO] Edge-report merged using {edge_src}")
            else:
                print('[WARN] masked survivors missing (tile_id, plate_id); edge annotation skipped')
        else:
            print(f"[WARN] Edge-report not found: {er_path} — skipping")

    # Core-only filter
    if core_only:
        if 'edge_class' not in df.columns:
            print('[WARN] --core-only requested but edge_class missing; keeping Inclusive')
        else:
            before = len(df)
            df = df[df['edge_class'].fillna('core') == 'core'].copy()
            print(f"[INFO] Core-only applied: {before:,} -> {len(df):,}")

    # Normalize RA/Dec & pick columns
    df = normalize_radec(df)
    cols = [c for c in ['tile_id','plate_id','RA','Dec','NUMBER','row_id','plate_epoch_mjd','epoch_mjd','DATE_OBS','edge_class','min_edge_dist_px','min_edge_dist_arcsec'] if c in df.columns]
    if args.keep_cols:
        for c in [c.strip() for c in args.keep_cols.split(',') if c.strip()]:
            if c in df.columns and c not in cols:
                cols.append(c)
    out_df = df[cols].copy()

    out_parq = Path(args.out); out_parq.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_parquet(out_parq, index=False); print(f"[OK] Parquet written: {out_parq}")
    csv_path = ''
    if emit_csv:
        csv_path = str((Path(args.csv_out) if args.csv_out else out_parq.with_suffix('.csv')))
        out_df.to_csv(csv_path, index=False); print(f"[OK] CSV written: {csv_path}")

    metrics = {
        'rows': int(len(out_df)), 'core_only': core_only,
        'edge_policy': 'core' if core_only else 'inclusive',
        'edge_class_source': edge_src or 'none',
        'source_masked': str(Path(args.masked).resolve()),
        'edge_report': str(Path(args.edge_report).resolve()) if args.edge_report else '',
        'parquet': str(out_parq.resolve()), 'csv': csv_path
    }
    with open(out_parq.with_suffix('.metrics.json'), 'w', encoding='utf-8') as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    print(f"[OK] Metrics sidecar: {out_parq.with_suffix('.metrics.json')}")

if __name__ == '__main__':
    main()
