
#!/usr/bin/env python3
"""
final_candidates_post16.py

Hybrid IR-aware finalization:
- Annotate-only, emit MNRAS-style counts immediately (--counts-only).
- Optionally publish annotated dataset (--publish-annotated).
- Auto-detects coordinate columns (SExtractor) and join key (NUMBER/row_id/source_id).
"""

import argparse, os, sys
from typing import Optional, Tuple

import pyarrow.dataset as ds
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import numpy as np

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--optical-master-parquet', required=True)
    p.add_argument('--irflags-parquet', required=True)
    p.add_argument('--annotate-ir', action='store_true')
    p.add_argument('--counts-only', action='store_true')
    p.add_argument('--publish-annotated', action='store_true')
    p.add_argument('--join-key', default=None, help='Override join key; otherwise auto-detected')
    p.add_argument('--dedupe-tol-arcsec', type=float, default=0.5)
    p.add_argument('--out-dir', required=True)
    p.add_argument('--ra-col', default=None, help='Override RA column name')
    p.add_argument('--dec-col', default=None, help='Override Dec column name')
    return p.parse_args()

def load_dataset(root: str) -> ds.Dataset:
    return ds.dataset(root, format='parquet')

def read_all_to_df(dataset: ds.Dataset, columns: Optional[list] = None) -> pd.DataFrame:
    dfs = []
    for frag in dataset.get_fragments():
        tbl = frag.to_table(columns=columns)
        dfs.append(tbl.to_pandas())
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def pick_coords(df: pd.DataFrame, ra_override: Optional[str], dec_override: Optional[str]) -> Tuple[str, str]:
    if ra_override and dec_override and {ra_override, dec_override}.issubset(df.columns):
        return ra_override, dec_override
    for pair in [('ALPHAWIN_J2000','DELTAWIN_J2000'),
                 ('ALPHA_J2000','DELTA_J2000'),
                 ('X_WORLD','Y_WORLD')]:
        if set(pair).issubset(df.columns):
            return pair
    print("[ERROR] RA/Dec columns not found; use --ra-col/--dec-col.", file=sys.stderr); sys.exit(2)

def pick_join_key(opt_cols, ir_cols, override: Optional[str]) -> str:
    if override:
        if override in opt_cols and override in ir_cols:
            return override
        print(f"[ERROR] Override join key '{override}' not present in both datasets.", file=sys.stderr); sys.exit(2)
    for key in ('NUMBER', 'row_id', 'source_id'):
        if key in opt_cols and key in ir_cols:
            return key
    print("[ERROR] No common join key (NUMBER/row_id/source_id).", file=sys.stderr); sys.exit(2)

def approx_dedupe(df: pd.DataFrame, tol_arcsec: float, ra_col: str, dec_col: str) -> pd.DataFrame:
    grid = tol_arcsec / 3600.0
    ra = pd.to_numeric(df[ra_col], errors='coerce'); dec = pd.to_numeric(df[dec_col], errors='coerce')
    key = np.round(ra / grid).astype('Int64').astype('string') + ':' + np.round(dec / grid).astype('Int64').astype('string')
    return df.loc[~key.duplicated()].copy()

def main():
    a = parse_args()
    os.makedirs(a.out_dir, exist_ok=True)

    opt_df = read_all_to_df(load_dataset(a.optical_master_parquet))
    if opt_df.empty: print('[WARN] Optical master empty.'); sys.exit(0)
    ra_col, dec_col = pick_coords(opt_df, a.ra_col, a.dec_col)

    ir_df = read_all_to_df(load_dataset(a.irflags_parquet))
    if ir_df.empty:
        opt_df['has_ir_match'] = False
        key = a.join_key or 'NUMBER'
    else:
        # normalize flags column name if needed
        if 'has_ir_match' not in ir_df.columns and 'ir_match_strict' in ir_df.columns:
            ir_df['has_ir_match'] = ir_df['ir_match_strict'].astype(bool)
        # distance alias
        if 'dist_arcsec' not in ir_df.columns and 'sep_arcsec' in ir_df.columns:
            ir_df = ir_df.rename(columns={'sep_arcsec':'dist_arcsec'})
        key = pick_join_key(set(opt_df.columns), set(ir_df.columns), a.join_key)
        ir_flags = ir_df[[key, 'has_ir_match']].drop_duplicates()
        opt_df = opt_df.merge(ir_flags, on=key, how='left')
        opt_df['has_ir_match'] = opt_df['has_ir_match'].fillna(False).infer_objects(copy=False)

    # default False if absent
    for col in ('is_morphology_bad','is_spike','is_hpm','is_skybot','is_supercosmos_artifact'):
        if col not in opt_df.columns: opt_df[col] = False

    before = len(opt_df)
    opt_df = approx_dedupe(opt_df, a.dedupe_tol_arcsec, ra_col, dec_col); after = len(opt_df)

    def count(s: pd.Series) -> int: return int(pd.Series(s).astype(bool).sum())
    total = len(opt_df); ir_pos = count(opt_df['has_ir_match'])
    morph_bad = count(opt_df['is_morphology_bad']); spikes = count(opt_df['is_spike'])
    hpm = count(opt_df['is_hpm']); skybot = count(opt_df['is_skybot']); sc_art = count(opt_df['is_supercosmos_artifact'])
    survivors_ir_strict = total - ir_pos
    survivors_after_filters = survivors_ir_strict - (morph_bad + spikes + hpm + skybot + sc_art)

    summary_path = os.path.join(a.out_dir, 'post16_match_summary.txt')
    with open(summary_path, 'w', encoding='utf-8') as f:
        f.write('POST16 SUMMARY (Hybrid, counts-only)\n')
        f.write(f"Total (after approx dedupe): {total} (was {before} → {after})\n")
        f.write(f"IR-positive rows: {ir_pos}\n")
        f.write(f"Morphology bad: {morph_bad}\n")
        f.write(f"Diffraction spikes: {spikes}\n")
        f.write(f"High proper motion (POSS-I epoch): {hpm}\n")
        f.write(f"SkyBoT asteroid proximity: {skybot}\n")
        f.write(f"SuperCOSMOS artifacts: {sc_art}\n")
        f.write("———\n")
        f.write(f"Survivors (IR-strict): {survivors_ir_strict}\n")
        f.write(f"Survivors (after all filters): {survivors_after_filters}\n")
    print(f"[OK] Summary written: {summary_path}")

    if a.counts_only and not a.publish_annotated: return
    if a.publish_annotated:
        out_parquet = os.path.join(a.out_dir, 'annotated.parquet')
        pq.write_table(pa.Table.from_pandas(opt_df, preserve_index=False), out_parquet)
        print(f"[OK] Annotated dataset written: {out_parquet}")

if __name__ == '__main__':
    main()
    