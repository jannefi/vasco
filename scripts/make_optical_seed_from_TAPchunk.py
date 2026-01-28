#!/usr/bin/env python3
import argparse, os, sys
import pandas as pd

RA_DEC_CANDIDATES = [
    ('in_ra','in_dec'), ('opt_ra_deg','opt_dec_deg'), ('ra_deg','dec_deg'),
    ('ALPHA_J2000','DELTA_J2000'), ('ALPHAWIN_J2000','DELTAWIN_J2000'), ('X_WORLD','Y_WORLD'), ('ra','dec')
]

def detect_radec(cols):
    s = set(cols)
    for ra, dec in RA_DEC_CANDIDATES:
        if ra in s and dec in s: return ra, dec
    raise KeyError(f"Could not detect RA/Dec columns from {cols}.")

def pick_source_id(df):
    if 'row_id' in df.columns:
        return 'row_id', df['row_id'].astype(str)
    elif 'source_id' in df.columns:
        return 'source_id', df['source_id'].astype(str)
    elif 'NUMBER' in df.columns and 'tile_id' in df.columns:
        return 'tile_id#NUMBER', df['tile_id'].astype(str) + '#' + df['NUMBER'].astype(str)
    elif 'NUMBER' in df.columns and 'image_id' in df.columns:
        return 'image_id#NUMBER', df['image_id'].astype(str) + '#' + df['NUMBER'].astype(str)
    elif 'NUMBER' in df.columns:
        return 'NUMBER', df['NUMBER'].astype(str)
    else:
        return 'index', df.index.astype(str)

def main():
    ap = argparse.ArgumentParser(description='Convert TAP chunk CSV to optical Parquet for sidecar')
    ap.add_argument('--tap-chunk-csv', required=True)
    ap.add_argument('--chunk-id', default='00001')
    ap.add_argument('--out-dir', required=True)
    a = ap.parse_args()

    df = pd.read_csv(a.tap_chunk_csv)
    ra_col, dec_col = detect_radec(df.columns)
    sid_name, sid = pick_source_id(df)

    out = pd.DataFrame({
        'opt_ra_deg': df[ra_col].astype(float),
        'opt_dec_deg': df[dec_col].astype(float),
        'source_id':  sid.astype(str),
        'row_id':     df['row_id'].astype(str) if 'row_id' in df.columns else sid.astype(str),
        'chunk_id':   a.chunk_id,
    })

    os.makedirs(a.out_dir, exist_ok=True)
    out_path = os.path.join(a.out_dir, f"part-{a.chunk_id}.parquet")
    out.to_parquet(out_path, index=False)
    print(f"[OK] wrote {len(out)} rows -> {out_path}")
    print(f"[INFO] Mapped RA/Dec from ({ra_col}, {dec_col}); source_id from {sid_name}; row_id preserved={ 'row_id' in df.columns }")

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr); sys.exit(1)
