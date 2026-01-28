#!/usr/bin/env python3
import os, re, argparse, pandas as pd

def parse_qc(path):
    d = {'file': path, 'rows': 0, 'match_rate': None, 'matches_le_5arcsec': 0}
    with open(path) as f: s = f.read()
    m = re.search(r"rows=(\d+)", s)
    if m: d['rows'] = int(m.group(1))
    m = re.search(r"match_rate=([0-9.]+)", s)
    if m: d['match_rate'] = float(m.group(1))
    m = re.search(r"matches(?:_|)\s*<=\s*5arcsec=(\d+)|matches_&lt;=5arcsec=(\d+)", s)
    if m:
        val = next(g for g in m.groups() if g)
        d['matches_le_5arcsec'] = int(val)
    return d

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--qc-dir', default='./data/local-cats/tmp/positions/new')
    ap.add_argument('--out', default='./data/local-cats/tmp/positions/new/qc_summary.csv')
    a = ap.parse_args()

    rows=[]
    for fn in os.listdir(a.qc_dir):
        if fn.endswith('_closest.qc.txt'):
            rows.append(parse_qc(os.path.join(a.qc_dir, fn)))
    df = pd.DataFrame(rows)
    if len(df):
        df['chunk'] = df['file'].str.extract(r"positions(\d+)_closest.qc.txt", expand=False)
    df.to_csv(a.out, index=False)
    print(f"[OK] wrote {a.out} with {len(df)} rows")
    if len(df):
        print(df.describe())

if __name__ == '__main__':
    main()
