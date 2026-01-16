#!/usr/bin/env python3
"""
Build epoch_by_source.parquet by mapping master Parquet rows to containing tiles.

Inputs:
  - master: ./data/local-cats/_master_optical_parquet/
  - tile_provenance: ./data/metadata/tile_provenance.parquet

Output:
  - ./data/metadata/epoch_by_source.parquet (+ _SUCCESS)

Notes:
  - RA/Dec auto-pick: prefers ALPHAWIN_J2000/DELTAWIN_J2000
  - key auto-pick: NUMBER -> row_id -> source_id
  - If multiple tiles contain a source, choose the one closest to tile center
"""
from __future__ import annotations
import math
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow as pa

EPOCH_FOOTPRINT_MARGIN_DEG = 0.0  # set >0.0 if you want a cushion around tile bounds

CAND_RA  = ("ALPHAWIN_J2000","ALPHA_J2000","X_WORLD","RA_corr","RA")
CAND_DEC = ("DELTAWIN_J2000","DELTA_J2000","Y_WORLD","Dec_corr","DEC","Dec")
CAND_KEY = ("NUMBER","row_id","source_id")

def pick_cols(df: pd.DataFrame):
    ra  = next((c for c in CAND_RA  if c in df.columns), None)
    dec = next((c for c in CAND_DEC if c in df.columns), None)
    key = next((c for c in CAND_KEY if c in df.columns), None)
    return ra, dec, key

def contains(ra, dec, row):
    # rectangular test with cos(dec_center) for RA extent + optional margin
    cosd = max(1e-6, math.cos(math.radians(float(row['dec_center']))))
    hw = float(row['half_w_deg']) + EPOCH_FOOTPRINT_MARGIN_DEG
    hh = float(row['half_h_deg']) + EPOCH_FOOTPRINT_MARGIN_DEG
    dra = min((ra - float(row['ra_center'])) % 360.0, (float(row['ra_center']) - ra) % 360.0)
    dra_eff = dra * cosd
    return (dra_eff <= hw) and (abs(dec - float(row['dec_center'])) <= hh)

def main():
    master_root = './data/local-cats/_master_optical_parquet'
    tiles_path  = Path('./data/metadata/tile_provenance.parquet')
    out_path    = Path('./data/metadata/epoch_by_source.parquet')
    out_path.parent.mkdir(parents=True, exist_ok=True)

    tiles = pd.read_parquet(tiles_path, engine='pyarrow')
    ds_master = ds.dataset(master_root, format='parquet')

    writer = None; total = 0
    for frag in ds_master.get_fragments():
        df = frag.to_table().to_pandas()
        ra_col, dec_col, key_col = pick_cols(df)
        if not (ra_col and dec_col and key_col):
            continue
        ra  = pd.to_numeric(df[ra_col], errors='coerce')
        dec = pd.to_numeric(df[dec_col], errors='coerce')
        key = df[key_col].astype(str)

        rows_out = []
        # prefilter tiles by dec band for speed
        for i in range(len(df)):
            rai = float(ra.iloc[i]); deci = float(dec.iloc[i]); keyi = key.iloc[i]
            if not (np.isfinite(rai) and np.isfinite(deci)):
                continue
            cand = tiles[(tiles['dec_center'] - deci).abs() <= (tiles['half_h_deg'] + EPOCH_FOOTPRINT_MARGIN_DEG + 1e-6)]
            if cand.empty:
                continue
            contained = []
            for _, trow in cand.iterrows():
                if contains(rai, deci, trow):
                    contained.append(trow)
            if not contained:
                continue
            best = min(contained, key=lambda tr: abs(deci - float(tr['dec_center'])) + min((rai - float(tr['ra_center'])) % 360.0, (float(tr['ra_center']) - rai) % 360.0))
            rows_out.append({
                'NUMBER': keyi,
                'tile_id': best['tile_id'],
                'epoch_utc': best['epoch_utc'],
                'epoch_mjd': float(best['epoch_mjd']),
                'region': best['region'],
                'plateid': best['plateid'],
                'pltlbl': best['pltlbl'],
                'provenance': best['provenance'],
            })
        if rows_out:
            tbl = pa.Table.from_pandas(pd.DataFrame(rows_out), preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(out_path, tbl.schema)
            writer.write_table(tbl)
            total += len(rows_out)

    if writer is None:
        print('[ERROR] No epoch rows mapped; check tile headers/paths')
        raise SystemExit(2)
    writer.close()
    (out_path.parent / '_SUCCESS').write_text('ok', encoding='utf-8')
    print(f"[OK] epoch_by_source written: {out_path} (rows={total})")

if __name__ == '__main__':
    main()