
#!/usr/bin/env python3
"""
final_candidates_post16.py

Hybrid IR-aware finalization:
- Annotate-only (preferred), emit MNRAS-style counts immediately (--counts-only).
- Optionally publish annotated dataset (--publish-annotated).
- Supports exporting strict view via separate "export_masked_view.py".

Enhancements:
- Auto-detect coordinate columns (prefers RA_corr/Dec_corr, falls back to common pairs).
- Optional CLI overrides: --ra-col / --dec-col to force coordinate columns.
"""

import argparse, os, sys
from typing import Optional, Tuple

try:
    import pyarrow.dataset as ds
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pandas as pd
    import numpy as np
except Exception as e:
    print("[ERROR] Missing required Python packages (pyarrow, pandas, numpy).", file=sys.stderr)
    raise

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--optical-master-parquet', required=True)
    p.add_argument('--irflags-parquet', required=True)
    p.add_argument('--annotate-ir', action='store_true')
    p.add_argument('--counts-only', action='store_true')
    p.add_argument('--publish-annotated', action='store_true')
    p.add_argument('--join-key', default='source_id',
                  help='Key column present in both optical and flags datasets')
    p.add_argument('--dedupe-tol-arcsec', type=float, default=0.5)
    p.add_argument('--out-dir', required=True)
    p.add_argument('--ir-radius-arcsec', type=float, default=5.0,
                  help='Documentary only; positional IR matching not implemented here')
    # NEW: allow forcing coordinate columns
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

    # Preferred corrected coords
    if {'RA_corr', 'Dec_corr'}.issubset(df.columns):
        return 'RA_corr', 'Dec_corr'

    # Common catalogue pairs
    candidates = [
        ('RA', 'Dec'),
        ('RAJ2000', 'DEJ2000'),
        ('RA_ICRS', 'DE_ICRS'),
        ('ra', 'dec'),
    ]
    for ra_c, dec_c in candidates:
        if {ra_c, dec_c}.issubset(df.columns):
            return ra_c, dec_c

    print("[ERROR] RA/Dec columns not found; please map your coordinate columns.", file=sys.stderr)
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

def main():
    args = parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    print('[INFO] Loading optical master parquet dataset…')
    opt_ds = load_dataset(args.optical_master_parquet)
    opt_df = read_all_to_df(opt_ds)
    if opt_df.empty:
        print('[WARN] Optical master is empty. Exiting.')
        sys.exit(0)

    # Pick coordinate columns (auto or override)
    ra_col, dec_col = pick_coords(opt_df, args.ra_col, args.dec_col)

    print('[INFO] Loading IR flags dataset…')
    ir_ds = load_dataset(args.irflags_parquet)
    ir_df = read_all_to_df(ir_ds)
    if ir_df.empty:
        print('[WARN] IR flags are empty; proceeding without IR annotations.')
        opt_df['has_ir_match'] = False
    else:
        if args.join_key not in opt_df.columns or args.join_key not in ir_df.columns:
            print(f"[ERROR] Join key '{args.join_key}' missing in one of the datasets.", file=sys.stderr)
            sys.exit(2)
        ir_flags = ir_df[[args.join_key, 'has_ir_match']].drop_duplicates()
        opt_df = opt_df.merge(ir_flags, on=args.join_key, how='left')
        opt_df['has_ir_match'] = opt_df['has_ir_match'].fillna(False)

    # Morphology/spike & HPM flags — default False if absent
    for col in ('is_morphology_bad', 'is_spike', 'is_hpm', 'is_skybot', 'is_supercosmos_artifact'):
        if col not in opt_df.columns:
            opt_df[col] = False

    # Global dedupe (approx)
    before = len(opt_df)
    opt_df = approx_dedupe(opt_df, args.dedupe_tol_arcsec, ra_col=ra_col, dec_col=dec_col)
    after = len(opt_df)

    # Compute counts for the summary
    def count(mask: pd.Series) -> int:
        return int(mask.astype(bool).sum())

    total = len(opt_df)
    ir_pos = count(opt_df['has_ir_match'])
    morph_bad = count(opt_df['is_morphology_bad'])
    spikes = count(opt_df['is_spike'])
    hpm = count(opt_df['is_hpm'])
    skybot = count(opt_df['is_skybot'])
    sc_art = count(opt_df['is_supercosmos_artifact'])

    survivors_ir_strict = total - ir_pos
    survivors_after_filters = survivors_ir_strict - (morph_bad + spikes + hpm + skybot + sc_art)

    # Write summary
    summary_path = os.path.join(args.out_dir, 'post16_match_summary.txt')
    with open(summary_path, 'w', encoding='utf-8') as f:
        f.write('POST16 SUMMARY (Hybrid, counts-only capable)\n')
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

    if args.counts_only and not args.publish_annotated:
        print('[INFO] Counts-only mode; no large dataset written.')
        return

    if args.publish_annotated:
        out_parquet = os.path.join(args.out_dir, 'annotated.parquet')
        table = pa.Table.from_pandas(opt_df, preserve_index=False)
        pq.write_table(table, out_parquet)
        print(f"[OK] Annotated dataset written: {out_parquet}")

if __name__ == '__main__':
    main()

