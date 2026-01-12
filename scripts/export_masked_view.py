
#!/usr/bin/env python3
"""
export_masked_view.py

Strict IR-excluded view from optical+flags, with:
- Auto join key (NUMBER/row_id/source_id, overridable)
- Auto RA/Dec pick (ALPHAWIN_J2000/DELTAWIN_J2000 → ALPHA_J2000/DELTA_J2000 → X_WORLD/Y_WORLD)
- IR flag normalization (ir_match_strict → has_ir_match)
- Mask OR-combiner
"""

import argparse, os, sys
from typing import Optional, Tuple

import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import numpy as np

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--input-parquet', required=True)
    p.add_argument('--irflags-parquet', required=True)
    p.add_argument('--join-key', default=None)
    p.add_argument('--mask', required=True)
    p.add_argument('--dedupe-tol-arcsec', type=float, default=0.5)
    p.add_argument('--out', required=True)
    p.add_argument('--ra-col', default=None)
    p.add_argument('--dec-col', default=None)
    return p.parse_args()

def load_dataset(root: str) -> ds.Dataset: return ds.dataset(root, format='parquet')

def read_all_to_df(dataset: ds.Dataset, columns: Optional[list] = None) -> pd.DataFrame:
    dfs = []; 
    for frag in dataset.get_fragments(): dfs.append(frag.to_table(columns=columns).to_pandas())
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def pick_coords(df: pd.DataFrame, ra_override: Optional[str], dec_override: Optional[str]) -> Tuple[str, str]:
    if ra_override and dec_override and {ra_override, dec_override}.issubset(df.columns):
        return ra_override, dec_override
    for pair in [('ALPHAWIN_J2000','DELTAWIN_J2000'),
                 ('ALPHA_J2000','DELTA_J2000'),
                 ('X_WORLD','Y_WORLD')]:
        if set(pair).issubset(df.columns): return pair
    print("[ERROR] RA/Dec columns not found; use --ra-col/--dec-col.", file=sys.stderr); sys.exit(2)

def pick_join_key(opt_cols, ir_cols, override: Optional[str]) -> str:
    if override:
        if override in opt_cols and override in ir_cols: return override
        print(f"[ERROR] Override join key '{override}' not present in both datasets.", file=sys.stderr); sys.exit(2)
    for key in ('NUMBER', 'row_id', 'source_id'):
        if key in opt_cols and key in ir_cols: return key
    print("[ERROR] No common join key (NUMBER/row_id/source_id).", file=sys.stderr); sys.exit(2)

def approx_dedupe(df: pd.DataFrame, tol_arcsec: float, ra_col: str, dec_col: str) -> pd.DataFrame:
    grid = tol_arcsec / 3600.0
    ra = pd.to_numeric(df[ra_col], errors='coerce'); dec = pd.to_numeric(df[dec_col], errors='coerce')
    key = np.round(ra / grid).astype('Int64').astype('string') + ':' + np.round(dec / grid).astype('Int64').astype('string')
    return df.loc[~key.duplicated()].copy()

def apply_mask(df: pd.DataFrame, mask_expr: str) -> pd.DataFrame:
    masks = {
        'exclude_ir_strict': df.get('has_ir_match', False),
        'exclude_hpm': df.get('is_hpm', False),
        'exclude_skybot': df.get('is_skybot', False),
        'exclude_supercosmos': df.get('is_supercosmos_artifact', False),
        'exclude_spikes': df.get('is_spike', False),
        'exclude_morphology': df.get('is_morphology_bad', False),
    }
    expr = mask_expr.lower()
    excl = pd.Series(False, index=df.index)
    for token, series in masks.items():
        if token in expr:
            excl = excl | pd.Series(series).astype(bool)
    return df.loc[~excl].copy()

def main():
    a = parse_args()
    os.makedirs(os.path.dirname(a.out) or '.', exist_ok=True)

    opt_df = read_all_to_df(load_dataset(a.input_parquet))
    if opt_df.empty: print('[WARN] Optical dataset empty.'); sys.exit(0)
    ra_col, dec_col = pick_coords(opt_df, a.ra_col, a.dec_col)

    ir_df = read_all_to_df(load_dataset(a.irflags_parquet))
    if ir_df.empty:
        opt_df['has_ir_match'] = False
        key = a.join_key or 'NUMBER'
    else:
        if 'has_ir_match' not in ir_df.columns and 'ir_match_strict' in ir_df.columns:
            ir_df['has_ir_match'] = ir_df['ir_match_strict'].astype(bool)
        if 'dist_arcsec' not in ir_df.columns and 'sep_arcsec' in ir_df.columns:
            ir_df = ir_df.rename(columns={'sep_arcsec':'dist_arcsec'})
        key = pick_join_key(set(opt_df.columns), set(ir_df.columns), a.join_key)
        ir_flags = ir_df[[key, 'has_ir_match']].drop_duplicates()
        opt_df = opt_df.merge(ir_flags, on=key, how='left')
        s = opt_df['has_ir_match'].fillna(False)
        opt_df['has_ir_match'] = s.infer_objects(copy=False)

    for col in ('is_morphology_bad','is_spike','is_hpm','is_skybot','is_supercosmos_artifact'):
        if col not in opt_df.columns: opt_df[col] = False

    opt_df = approx_dedupe(opt_df, a.dedupe_tol_arcsec, ra_col, dec_col)
    masked = apply_mask(opt_df, a.mask)

    pq.write_table(pa.Table.from_pandas(masked, preserve_index=False), a.out)
    print(f"[OK] Strict view written: {a.out} (rows={len(masked)})")

if __name__ == '__main__':
    main()
