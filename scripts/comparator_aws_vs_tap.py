#!/usr/bin/env python3
import argparse, pandas as pd, numpy as np

def canon_row_id(s: pd.Series) -> pd.Series:
    t = s.astype(str).str.strip()
    return t.str.replace(r'^(-?\d+)\.0+$', r'', regex=True)

def norm_moon_masked(s: pd.Series) -> pd.Series:
    return s.astype(str).str.replace(r'[^0-9]','',regex=True).str.zfill(2)

def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [c.lower() for c in df.columns]
    if 'row_id' in df.columns:
        df['row_id'] = canon_row_id(df['row_id'])
    if 'moon_masked' in df.columns:
        df['moon_masked'] = norm_moon_masked(df['moon_masked'])
    return df

def gates(df: pd.DataFrame, p: str) -> pd.Series:
    req=[f"qual_frame{p}",f"qi_fact{p}",f"saa_sep{p}",f"moon_masked{p}",f"w1snr{p}",f"mjd{p}"]
    if not set(req).issubset(df.columns): return pd.Series([True]*len(df), index=df.index)
    return ((df[f"qual_frame{p}"]>0) & (df[f"qi_fact{p}"]>0) & (df[f"saa_sep{p}"]>0) &
            (df[f"moon_masked{p}"]=='00') & (df[f"w1snr{p}"]>=5) & (df[f"mjd{p}"]<=59198.0))

def summarize(merged, col, suf=("_aws","_tap"), atol=0.0, rtol=0.0):
    a,b = merged.get(col+suf[0]), merged.get(col+suf[1])
    if a is None or b is None: return None
    a,b = a.astype(float), b.astype(float)
    d=(a-b).to_numpy()
    bad = ~np.isclose(a,b,atol=atol,rtol=rtol)
    return dict(n=int(d.size), mean=float(np.nanmean(d)), std=float(np.nanstd(d)),
                min=float(np.nanmin(d)), max=float(np.nanmax(d)), violations=int(bad.sum()))

def main():
    ap=argparse.ArgumentParser(description='Compare AWS vs TAP closest outputs')
    ap.add_argument('--tap', required=True)
    ap.add_argument('--aws', required=True)
    ap.add_argument('--out-prefix', default='./data/local-cats/tmp/positions/new/compare')
    ap.add_argument('--ra-dec-atol-arcsec', type=float, default=0.10)
    ap.add_argument('--mjd-atol', type=float, default=5e-5)
    ap.add_argument('--snr-rtol', type=float, default=1e-3)
    a=ap.parse_args()

    tap, aws = load_csv(a.tap), load_csv(a.aws)

    inner_c = aws.merge(tap, on='cntr', how='inner', suffixes=('_aws','_tap'))
    tap_only_c = tap[~tap['cntr'].isin(inner_c['cntr'])]
    aws_only_c = aws[~aws['cntr'].isin(inner_c['cntr'])]

    by_pair = {'row_id','cntr'}.issubset(aws.columns) and {'row_id','cntr'}.issubset(tap.columns)
    if by_pair:
        inner_p = aws.merge(tap, on=['row_id','cntr'], how='inner', suffixes=('_aws','_tap'))
        tap_only_p = tap.merge(aws[['row_id','cntr']], on=['row_id','cntr'], how='left', indicator=True)
        tap_only_p = tap_only_p[tap_only_p['_merge']=='left_only'].drop(columns=['_merge'])
        aws_only_p = aws.merge(tap[['row_id','cntr']], on=['row_id','cntr'], how='left', indicator=True)
        aws_only_p = aws_only_p[aws_only_p['_merge']=='left_only'].drop(columns=['_merge'])

    print('=== Coverage (by "cntr") ===')
    print({
        'tap_rows': len(tap), 'aws_rows': len(aws), 'overlap_on_cntr': len(inner_c),
        'tap_only_on_cntr': len(tap_only_c), 'aws_only_on_cntr': len(aws_only_c),
        'tap_cntr_duplicates': int(tap['cntr'].duplicated(keep=False).sum() if 'cntr' in tap else 0),
        'aws_cntr_duplicates': int(aws['cntr'].duplicated(keep(False)).sum() if 'cntr' in aws else 0),
    })

    if by_pair:
        print('=== Coverage (by ("row_id","cntr")) ===')
        print({
            'overlap_on_pair': len(inner_p),
            'tap_only_on_pair': len(tap_only_p),
            'aws_only_on_pair': len(aws_only_p),
        })

    print('=== Gate checks on overlap (by cntr) ===')
    print({
        'aws_gate_violations': int((~gates(inner_c,'_aws')).sum()),
        'tap_gate_violations': int((~gates(inner_c,'_tap')).sum()),
    })

    print('=== Field deltas (AWS - TAP) on overlap (by cntr) ===')
    ra_dec_atol_deg = a.ra_dec_atol_arcsec/3600.0
    for col, atol, rtol in [('ra',ra_dec_atol_deg,0.0), ('dec',ra_dec_atol_deg,0.0),
                            ('mjd',a.mjd_atol,0.0), ('w1snr',0.0,a.snr_rtol), ('w2snr',0.0,a.snr_rtol)]:
        s = summarize(inner_c, col, atol=atol, rtol=rtol)
        if s:
            print(f"{col.upper():5s}: n={s['n']:4d} mean={s['mean']:.3e} std={s['std']:.3e} min={s['min']:.3e} max={s['max']:.3e} |violations|={s['violations']}")

    if a.out_prefix:
        tap_only_c.to_csv(f"{a.out_prefix}.tap_only_by_cntr.csv", index=False)
        aws_only_c.to_csv(f"{a.out_prefix}.aws_only_by_cntr.csv", index=False)
        if by_pair:
            tap_only_p.to_csv(f"{a.out_prefix}.tap_only_by_pair.csv", index=False)
            aws_only_p.to_csv(f"{a.out_prefix}.aws_only_by_pair.csv", index=False)

if __name__ == '__main__':
    main()
