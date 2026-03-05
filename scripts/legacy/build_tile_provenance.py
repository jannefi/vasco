#!/usr/bin/env python3
"""
Build tile_provenance.parquet by scanning tile trees and per-tile header JSONs.

Discovers tiles under:
  - ./data/tiles/tile-RA<deg>-DEC<deg>/
  - ./data/tiles_by_sky/ra_bin=*/dec_bin=*/tile-RA<deg>-DEC<deg>/

For each tile (POSSI-E only):
  - center RA/Dec from header CRVAL* (fallback folder name)
  - footprint from CD matrix and NAXIS
  - epoch from DATE-OBS (sanitized), fallback to full plate header by REGION

Writes: ./data/metadata/tile_provenance.parquet (+ _SUCCESS)
"""
from __future__ import annotations
import os, re, json, math
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from utils_epoch import parse_dateobs_with_sanitize, get_epoch_from_tile_or_plate

TILE_ROOTS = [Path('./data/tiles'), Path('./data/tiles_by_sky')]
TILE_DIR_RE = re.compile(r"tile-RA(?P<ra>[+-]?\d+(?:\.\d+)?)\-DEC(?P<dec>[+-]?\d+(?:\.\d+)?)")

def parse_tile_center_from_dir(name: str):
    m = TILE_DIR_RE.search(name)
    if not m:
        return None, None
    return float(m.group('ra')), float(m.group('dec'))

def first_header_json(tile_dir: Path):
    raw = tile_dir / 'raw'
    if not raw.exists():
        return None
    for fn in raw.glob('*.fits.header.json'):
        try:
            return json.loads(fn.read_text(encoding='utf-8'))
        except Exception:
            continue
    return None

def pick_center_from_header(j: dict):
    sel = j.get('selected', {})
    hed = j.get('header', {})
    def g(d,k):
        v = d.get(k)
        try:
            return float(v)
        except Exception:
            return None
    ra = g(sel,'CRVAL1') or g(hed,'CRVAL1')
    dec= g(sel,'CRVAL2') or g(hed,'CRVAL2')
    return ra, dec

def cd_footprint_deg(j: dict):
    sel = j.get('selected', {})
    hed = j.get('header', {})
    def f(d,k,default='0'):
        v = d.get(k)
        return float(v if v is not None else default)
    nax1 = f(hed,'NAXIS1', sel.get('NAXIS1','0'))
    nax2 = f(hed,'NAXIS2', sel.get('NAXIS2','0'))
    cd11 = abs(f(hed,'CD1_1', sel.get('CD1_1','0')))
    cd12 = abs(f(hed,'CD1_2', sel.get('CD1_2','0')))
    cd21 = abs(f(hed,'CD2_1', sel.get('CD2_1','0')))
    cd22 = abs(f(hed,'CD2_2', sel.get('CD2_2','0')))
    px_ra  = math.hypot(cd11, cd12)
    px_dec = math.hypot(cd21, cd22)
    width_deg  = nax1 * px_ra
    height_deg = nax2 * px_dec
    return 0.5*width_deg, 0.5*height_deg

def main():
    out_path = Path('./data/metadata/tile_provenance.parquet')
    out_path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for root in TILE_ROOTS:
        if not root.exists():
            continue
        # walk both flat and sharded trees
        for dirpath, dirnames, filenames in os.walk(root):
            base = os.path.basename(dirpath)
            if base.startswith('tile-RA'):
                tile_dir = Path(dirpath)
                j = first_header_json(tile_dir)
                if not j:
                    continue
                hdr = j.get('header', {})
                if str(hdr.get('SURVEY','')).upper() != 'POSSI-E':
                    continue
                region = hdr.get('REGION') or ''
                plateid = hdr.get('PLATEID') or ''
                pltlbl  = hdr.get('PLTLABEL') or ''
                ra_c, dec_c = pick_center_from_header(j)
                if ra_c is None or dec_c is None:
                    # fallback to folder name
                    ra_c, dec_c = parse_tile_center_from_dir(base)
                    if ra_c is None or dec_c is None:
                        continue
                half_w, half_h = cd_footprint_deg(j)
                # epoch with sanitize + fallback to full plate header
                ep = get_epoch_from_tile_or_plate(j, region)
                if not ep:
                    continue
                epoch_utc, epoch_mjd, provenance = ep
                rows.append({
                    'tile_id': str(tile_dir),
                    'ra_center': float(ra_c),
                    'dec_center': float(dec_c),
                    'half_w_deg': float(half_w),
                    'half_h_deg': float(half_h),
                    'epoch_utc': epoch_utc,
                    'epoch_mjd': float(epoch_mjd),
                    'region': region,
                    'plateid': plateid,
                    'pltlbl': pltlbl,
                    'provenance': provenance,
                })
    if not rows:
        print('[ERROR] No tiles with POSSI-E headers discovered')
        raise SystemExit(2)
    tbl = pa.Table.from_pandas(pd.DataFrame(rows), preserve_index=False)
    pq.write_table(tbl, out_path)
    (out_path.parent / '_SUCCESS').write_text('ok', encoding='utf-8')
    print(f"[OK] tile_provenance written: {out_path} (rows={len(rows)})")

if __name__ == '__main__':
    main()