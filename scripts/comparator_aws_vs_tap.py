#!/usr/bin/env python3
# Compare AWS vs TAP closest outputs
# - Normalizes row_id for pair-join reporting
# - Writes diff CSVs alongside console summary
import argparse
import pandas as pd
import numpy as np
from typing import Dict, Optional

def canon_row_id(s: pd.Series) -> pd.Series:
    """Stringify + trim; collapse '123.0' -> '123' but keep other floats/sci forms."""
    t = s.astype(str).str.strip()
    return t.str.replace(r'^(-?\d+)\.0+$', r'\1', regex=True)

def norm_moon_masked(s: pd.Series) -> pd.Series:
    """Keep only digits and left-pad to 2 chars; '0' -> '00'."""
    return s.astype(str).str.replace(r'[^0-9]', '', regex=True).str.zfill(2)

def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [c.lower() for c in df.columns]
    if 'row_id' in df.columns:
        df['row_id'] = canon_row_id(df['row_id'])
    if 'moon_masked' in df.columns:
        df['moon_masked'] = norm_moon_masked(df['moon_masked'])
    return df

def gates(df: pd.DataFrame, suf: str) -> pd.Series:
    """TAP-equivalent gates on a merged frame with suffixes (_aws/_tap)."""
    req = [f'qual_frame{suf}', f'qi_fact{suf}', f'saa_sep{suf}',
           f'moon_masked{suf}', f'w1snr{suf}', f'mjd{suf}']
    if not set(req).issubset(df.columns):
        # If we don't have all columns, just return 'True' to avoid false flags
        return pd.Series([True] * len(df), index=df.index)
    return (
        (df[f'qual_frame{suf}'] > 0) &
        (df[f'qi_fact{suf}'] > 0) &
        (df[f'saa_sep{suf}'] > 0) &
        (df[f'moon_masked{suf}'] == '00') &
        (df[f'w1snr{suf}'] >= 5) &
        (df[f'mjd{suf}'] <= 59198.0)
    )

def summarize(merged: pd.DataFrame, col: str,
              suf: tuple = ('_aws', '_tap'),
              atol: float = 0.0, rtol: float = 0.0) -> Optional[Dict[str, float]]:
    a = merged.get(col + suf[0]); b = merged.get(col + suf[1])
    if a is None or b is None:
        return None
    a = a.astype(float);  b = b.astype(float)
    d = (a - b).to_numpy()
    bad = ~np.isclose(a, b, atol=atol, rtol=rtol)
    return dict(
        n=int(d.size),
        mean=float(np.nanmean(d)),
        std=float(np.nanstd(d)),
        min=float(np.nanmin(d)),
        max=float(np.nanmax(d)),
        violations=int(bad.sum()),
    )

def main():
    ap = argparse.ArgumentParser(description='Compare AWS vs TAP closest outputs')
    ap.add_argument('--tap', required=True)
    ap.add_argument('--aws', required=True)
    ap.add_argument('--out-prefix', default='./data/local-cats/tmp/positions/new/compare')
    ap.add_argument('--ra-dec-atol-arcsec', type=float, default=0.10)
    ap.add_argument('--mjd-atol', type=float, default=5e-5)
    ap.add_argument('--snr-rtol', type=float, default=1e-3)
    a = ap.parse_args()

    tap = load_csv(a.tap)
    aws = load_csv(a.aws)

    # ---- Coverage (by cntr)
    inner_c = aws.merge(tap, on='cntr', how='inner', suffixes=('_aws', '_tap'))
    tap_only_c = tap[~tap['cntr'].isin(inner_c['cntr'])]
    aws_only_c = aws[~aws['cntr'].isin(inner_c['cntr'])]

    tap_dups = int(tap['cntr'].duplicated(keep=False).sum()) if 'cntr' in tap.columns else 0
    aws_dups = int(aws['cntr'].duplicated(keep=False).sum()) if 'cntr' in aws.columns else 0

    print('=== Coverage (by "cntr") ===')
    print({
        'tap_rows': len(tap),
        'aws_rows': len(aws),
        'overlap_on_cntr': len(inner_c),
        'tap_only_on_cntr': len(tap_only_c),
        'aws_only_on_cntr': len(aws_only_c),
        'tap_cntr_duplicates': tap_dups,
        'aws_cntr_duplicates': aws_dups,
    })

    # ---- Coverage (by (row_id, cntr)) if both have row_id
    by_pair = {'row_id', 'cntr'}.issubset(aws.columns) and {'row_id', 'cntr'}.issubset(tap.columns)
    if by_pair:
        inner_p = aws.merge(tap, on=['row_id', 'cntr'], how='inner', suffixes=('_aws', '_tap'))
        tap_only_p = tap.merge(aws[['row_id', 'cntr']], on=['row_id', 'cntr'],
                               how='left', indicator=True)
        tap_only_p = tap_only_p[tap_only_p['_merge'] == 'left_only'].drop(columns=['_merge'])

        aws_only_p = aws.merge(tap[['row_id', 'cntr']], on=['row_id', 'cntr'],
                               how='left', indicator=True)
        aws_only_p = aws_only_p[aws_only_p['_merge'] == 'left_only'].drop(columns=['_merge'])

        print('\n=== Coverage (by ("row_id","cntr")) ===')
        print({
            'overlap_on_pair': len(inner_p),
            'tap_only_on_pair': len(tap_only_p),
            'aws_only_on_pair': len(aws_only_p),
        })

    # ---- Gate checks on overlap (by cntr)
    print('\n=== Gate checks on overlap (by cntr) ===')
    print({
        'aws_gate_violations': int((~gates(inner_c, '_aws')).sum()),
        'tap_gate_violations': int((~gates(inner_c, '_tap')).sum()),
    })

    # ---- Field deltas on overlap (by cntr)
    print('\n=== Field deltas (AWS - TAP) on overlap (by cntr) ===')
    ra_dec_atol_deg = a.ra_dec_atol_arcsec / 3600.0
    for col, atol, rtol in [
        ('ra',   ra_dec_atol_deg, 0.0),
        ('dec',  ra_dec_atol_deg, 0.0),
        ('mjd',  a.mjd_atol,      0.0),
        ('w1snr', 0.0,            a.snr_rtol),
        ('w2snr', 0.0,            a.snr_rtol),
    ]:
        s = summarize(inner_c, col, atol=atol, rtol=rtol)
        if s:
            print(f"{col.upper():5s}: n={s['n']:4d} mean={s['mean']:.3e} "
                  f"std={s['std']:.3e} min={s['min']:.3e} "
                  f"max={s['max']:.3e} |violations|={s['violations']}")

    # ---- Write diffs
    if a.out_prefix:
        tap_only_c.to_csv(f"{a.out_prefix}.tap_only_by_cntr.csv", index=False)
        aws_only_c.to_csv(f"{a.out_prefix}.aws_only_by_cntr.csv", index=False)
        if by_pair:
            tap_only_p.to_csv(f"{a.out_prefix}.tap_only_by_pair.csv", index=False)
            aws_only_p.to_csv(f"{a.out_prefix}.aws_only_by_pair.csv", index=False)

if __name__ == '__main__':
    main()