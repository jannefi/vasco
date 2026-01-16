
#!/usr/bin/env python3
import argparse, time, requests, math, os, sys, json
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np, pandas as pd
import pyarrow.dataset as ds
import pyarrow.parquet as pq, pyarrow as pa

API = 'https://vo.imcce.fr/webservices/skybot/skybotconesearch_query.php'

def pick_cols(cols):
    ra = next((c for c in ("ALPHAWIN_J2000","ALPHA_J2000","X_WORLD","RA_corr","RA") if c in cols), None)
    dec= next((c for c in ("DELTAWIN_J2000","DELTA_J2000","Y_WORLD","Dec_corr","DEC","Dec") if c in cols), None)
    key= next((c for c in ("NUMBER","row_id","source_id") if c in cols), None)
    return ra, dec, key

def query_one(row, radius_arcsec, timeout_s=8):
    params = {
        '-ep': row['epoch_utc'],
        '-ra': f"{row['ra']}",
        '-dec': f"{row['dec']}",
        '-rs': f"{int(radius_arcsec)}",
        '-mime': 'text',
        '-output': 'all',
        '-loc': '500',
        '-filter': '120',
        '-objFilter': '110',
        '-refsys': 'EQJ2000',
        '-from': 'VASCO'
    }
    try:
        r = requests.get(API, params=params, timeout=timeout_s)
        r.raise_for_status()
        for ln in r.text.splitlines():
            s = ln.strip()
            if not s or s.startswith('#'): continue
            if '|' in s: return True
        return False
    except Exception:
        return False

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--master', required=True)
    ap.add_argument('--epoch-parquet', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--radius-arcsec', type=float, default=60.0)
    ap.add_argument('--max-rows', type=int, default=0)
    ap.add_argument('--workers', type=int, default=8)
    ap.add_argument('--checkpoint', default='./work/skybot_checkpoint.jsonl')
    args = ap.parse_args()

    # epochs
    ep = pq.read_table(args.epoch_parquet).to_pandas()
    if ep.empty: raise SystemExit("epoch_by_source is empty")
    ep['NUMBER'] = ep['NUMBER'].astype(str)
    ep = ep[['NUMBER','epoch_utc']].drop_duplicates('NUMBER')

    # master coords
    ds_master = ds.dataset(args.master, format='parquet')
    frames=[]
    for frag in ds_master.get_fragments():
        df = frag.to_table().to_pandas()
        ra_col, dec_col, key_col = pick_cols(df.columns)
        if not (ra_col and dec_col and key_col): continue
        part = df[[key_col, ra_col, dec_col]].dropna()
        part = part.rename(columns={key_col:'NUMBER', ra_col:'ra', dec_col:'dec'})
        part['NUMBER'] = part['NUMBER'].astype(str)
        frames.append(part)
    if not frames: raise SystemExit("no RA/Dec in master")
    det = pd.concat(frames, ignore_index=True).drop_duplicates('NUMBER')

    df = det.merge(ep, on='NUMBER', how='inner')

    # resume support
    done = set()
    if os.path.exists(args.checkpoint):
        with open(args.checkpoint, 'r', encoding='utf-8') as f:
            for ln in f:
                try:
                    j = json.loads(ln); done.add(j['NUMBER'])
                except: pass

    # trim to max-rows and skip already done
    if args.max_rows: df = df.head(args.max_rows)
    todo = df[~df['NUMBER'].isin(done)].to_dict('records')

    out_rows=[]
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(query_one, row, args.radius_arcsec): row for row in todo}
        with open(args.checkpoint, 'a', encoding='utf-8') as ck:
            for fut in as_completed(futs):
                row = futs[fut]
                ok = bool(fut.result())
                out_rows.append({'NUMBER': row['NUMBER'], 'is_skybot': ok})
                ck.write(json.dumps({'NUMBER': row['NUMBER']}) + "\n")
    # include previously done numbers as False unless present in out
    out_df = pd.DataFrame(out_rows)
    if out_df.empty:
        out_df = pd.DataFrame(columns=['NUMBER','is_skybot'])
    out_df['NUMBER'] = out_df['NUMBER'].astype(str)
    out_df = out_df.drop_duplicates('NUMBER')

    pq.write_table(pa.Table.from_pandas(out_df, preserve_index=False), args.out)
    print('[OK] SkyBoT flags ->', args.out, 'rows=', len(out_df))

if __name__ == '__main__':
    main()
