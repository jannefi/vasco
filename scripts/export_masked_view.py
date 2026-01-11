
#!/usr/bin/env python3
"""
export_masked_view.py

Derive a strict (IR-excluded + other masks) view from the optical master + IR flags sidecar.
Writes a compact Parquet file suitable for sharing/publication.

Examples:
  python scripts/export_masked_view.py \
    --input-parquet ./data/local-cats/_master_optical_parquet \
    --irflags-parquet ./data/local-cats/_master_optical_parquet_irflags/neowise_se_flags_ALL_NORMALIZED.parquet \
    --join-key NUMBER \
    --mask "exclude_ir_strict and exclude_hpm and exclude_skybot and exclude_supercosmos" \
    --ra-col ALPHAWIN_J2000 --dec-col DELTAWIN_J2000 \
    --dedupe-tol-arcsec 0.5 \
    --out ./data/vasco-candidates/post16/candidates_final_core.parquet
"""

import argparse, os, sys
from typing import Optional, Tuple

try:
    import pyarrow.dataset as ds
    import pyarrow.parquet as pq
    import pyarrow as pa
    import pandas as pd
    import numpy as np
except Exception:
    print("[ERROR] Missing required Python packages (pyarrow, pandas, numpy).", file=sys.stderr)
    raise

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--input-parquet', required=True, help='Optical master Parquet dataset root')
    p.add_argument('--irflags-parquet', required=True, help='Flags Parquet file or dataset root')
    p.add_argument('--join-key', default='source_id', help='Column present in both datasets (e.g., NUMBER, row_id, source_id)')
    p.add_argument('--mask', required=True,
                   help="Expression using tokens: exclude_ir_strict, exclude_hpm, exclude_skybot, exclude_supercosmos, exclude_spikes, exclude_morphology")
    p.add_argument('--dedupe-tol-arcsec', type=float, default=0.5)
    p.add_argument('--out', required=True, help='Output Parquet path')
    # NEW: allow forcing coordinate columns
    p.add_argument('--ra-col', default=None, help='Override RA column name (e.g., ALPHAWIN_J2000)')
    p.add_argument('--dec-col', default=None, help='Override Dec column name (e.g., DELTAWIN_J2000)')
    return p.parse_args()

def load_dataset(root: str) -> ds.Dataset:
    # Accept both a file and a directory; ds.dataset handles directories best.
    # If a single Parquet file is passed, Arrow will treat it as a dataset with one fragment.
    return ds.dataset(root, format='parquet')

def read_all_to_df(dataset: ds.Dataset, columns: Optional[list] = None) -> pd.DataFrame:
    dfs = []
    for frag in dataset.get_fragments():
        tbl = frag.to_table(columns=columns)
        dfs.append(tbl.to_pandas())
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)

def pick_coords(df: pd.DataFrame, ra_override: Optional[str], dec_override: Optional[str]) -> Tuple[str, str]:
    """Pick the best available RA/Dec pair, respecting overrides."""
    if ra_override and dec_override:
        if {ra_override, dec_override}.issubset(df.columns):
            return ra_override, dec_override
        print(f"[ERROR] Override columns '{ra_override}/{dec_override}' not found in optical dataset.", file=sys.stderr)
        sys.exit(2)

    # Preferred windowed centroids
    if {'ALPHAWIN_J2000', 'DELTAWIN_J2000'}.issubset(df.columns):
        return 'ALPHAWIN_J2000', 'DELTAWIN_J2000'

    # Fallbacks
    if {'ALPHA_J2000', 'DELTA_J2000'}.issubset(df.columns):
        return 'ALPHA_J2000', 'DELTA_J2000'
    if {'X_WORLD', 'Y_WORLD'}.issubset(df.columns):
        return 'X_WORLD', 'Y_WORLD'

    print("[ERROR] RA/Dec columns not found; please use --ra-col/--dec-col.", file=sys.stderr)
    sys.exit(2)

def approx_dedupe(df: pd.DataFrame, tol_arcsec: float, ra_col: str, dec_col: str) -> pd.DataFrame:
    """Approximate positional dedupe via tolerance-grid rounding."""
    grid = tol_arcsec / 3600.0  # arcsec -> degrees
    # Ensure numeric
    ra = pd.to_numeric(df[ra_col], errors='coerce')
    dec = pd.to_numeric(df[dec_col], errors='coerce')
    g_ra = np.round(ra / grid)
    g_dec = np.round(dec / grid)
    key = g_ra.astype('Int64').astype('string') + ':' + g_dec.astype('Int64').astype('string')
    return df.loc[~key.duplicated()].copy()

def apply_mask(df: pd.DataFrame, mask_expr: str) -> pd.DataFrame:
    """OR-combine selected exclusion tokens and return survivors."""
    # Tokens → Boolean series; default False if missing
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
    args = parse_args()
    os.makedirs(os.path.dirname(args.out) or '.', exist_ok=True)

    opt_ds = load_dataset(args.input_parquet)
    opt_df = read_all_to_df(opt_ds)
    if opt_df.empty:
        print('[WARN] Optical dataset is empty. Exiting.')
        sys.exit(0)

    # Pick coordinate columns (auto or override)
    ra_col, dec_col = pick_coords(opt_df, args.ra_col, args.dec_col)

    # Load flags
    ir_ds = load_dataset(args.irflags_parquet)
    ir_df = read_all_to_df(ir_ds)
    if ir_df.empty:
        # No flags: proceed with all False
        opt_df['has_ir_match'] = False
    else:
        # Normalize flags column name(s)
        flags_cols = set(ir_df.columns)
        if 'has_ir_match' not in flags_cols and 'ir_match_strict' in flags_cols:
            ir_df['has_ir_match'] = ir_df['ir_match_strict'].astype(bool)

        # Ensure join key exists both sides
        if args.join_key not in opt_df.columns or args.join_key not in ir_df.columns:
            print(f"[ERROR] Join key '{args.join_key}' missing in one of the datasets.", file=sys.stderr)
            sys.exit(2)

        # Slim flags to key + has_ir_match (+ optional distance)
        keep = [args.join_key, 'has_ir_match']
        for extra in ('dist_arcsec', 'sep_arcsec'):
            if extra in ir_df.columns:
                keep.append(extra)
        ir_flags = ir_df[keep].drop_duplicates()
        # If sep_arcsec exists but dist_arcsec not present, map it
        if 'sep_arcsec' in ir_flags.columns and 'dist_arcsec' not in ir_flags.columns:
            ir_flags = ir_flags.rename(columns={'sep_arcsec': 'dist_arcsec'})

        # Join
        opt_df = opt_df.merge(ir_flags, on=args.join_key, how='left')
        # Fill NA (avoid FutureWarning with infer_objects)
        s = opt_df['has_ir_match'].fillna(False)
        opt_df['has_ir_match'] = s.infer_objects(copy=False)

    # Ensure filter columns exist
    for col in ('is_morphology_bad', 'is_spike', 'is_hpm', 'is_skybot', 'is_supercosmos_artifact'):
        if col not in opt_df.columns:
            opt_df[col] = False

    # Deduplicate using the chosen RA/Dec
    opt_df = approx_dedupe(opt_df, args.dedupe_tol_arcsec, ra_col=ra_col, dec_col=dec_col)

    # Apply mask
    masked = apply_mask(opt_df, args.mask)

    # Write
    table = pa.Table.from_pandas(masked, preserve_index=False)
    pq.write_table(table, args.out)
    print(f"[OK] Strict view written: {args.out} (rows={len(masked)})")

if __name__ == '__main__':
    main()

