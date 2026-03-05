#!/usr/bin/env python3
"""
Generate a minimal VOTable (NUMBER, ra, dec) from master Parquet for STILTS uploads.
Defaults: ALPHAWIN_J2000 / DELTAWIN_J2000.
"""
import argparse
import pyarrow.dataset as ds
import pandas as pd

VOT_HEADER = """<?xml version='1.0'?>
<VOTABLE version='1.3' xmlns='http://www.ivoa.net/xml/VOTable/v1.3'>
 <RESOURCE>
  <TABLE>
   <FIELD name='NUMBER' datatype='char' arraysize='*'/>
   <FIELD name='ra' datatype='double' unit='deg'/>
   <FIELD name='dec' datatype='double' unit='deg'/>
   <DATA>
    <TABLEDATA>
"""
VOT_FOOTER = """    </TABLEDATA>
   </DATA>
  </TABLE>
 </RESOURCE>
</VOTABLE>
"""

def pick_cols(cols):
    ra = next((c for c in ("ALPHAWIN_J2000","ALPHA_J2000","X_WORLD","RA_corr","RA") if c in cols), None)
    dec= next((c for c in ("DELTAWIN_J2000","DELTA_J2000","Y_WORLD","Dec_corr","DEC","Dec") if c in cols), None)
    key= next((c for c in ("NUMBER","row_id","source_id") if c in cols), None)
    return ra, dec, key

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--master', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--limit', type=int, default=0)
    args = ap.parse_args()
    ds_master = ds.dataset(args.master, format='parquet')
    rows_written = 0
    with open(args.out, 'w', encoding='utf-8') as f:
        f.write(VOT_HEADER)
        for frag in ds_master.get_fragments():
            df = frag.to_table().to_pandas()
            ra_col, dec_col, key_col = pick_cols(df.columns)
            if not (ra_col and dec_col and key_col):
                continue
            df = df[[key_col, ra_col, dec_col]].dropna()
            for _, r in df.iterrows():
                f.write(f"     <TR><TD>{str(r[key_col])}</TD><TD>{float(r[ra_col])}</TD><TD>{float(r[dec_col])}</TD></TR>")
                rows_written += 1
                if args.limit and rows_written >= args.limit:
                    break
            if args.limit and rows_written >= args.limit:
                break
        f.write(VOT_FOOTER)
    print(f"[OK] VOT written: {args.out} (rows={rows_written})")

if __name__ == '__main__':
    main()