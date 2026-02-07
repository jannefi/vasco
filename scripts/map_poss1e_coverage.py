#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Map out STScI DSS POSS-I E (DSS1 Red) coverage over an RA/Dec grid.

Example:
  python map_poss1e_coverage.py \
    --center-ra 150.000 --center-dec 20.000 \
    --width-arcmin 240 --height-arcmin 240 \
    --step-arcmin 60 \
    --size-arcmin 20 \
    --v 1 \
    --sleep-ms 200 \
    --timeout 60 \
    --out coverage_poss1e
"""

import argparse
import csv
import math
import sys
import time
from pathlib import Path
from typing import Tuple
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from astropy.io import fits
from io import BytesIO

DEF_UA = 'VASCO/0.06.9 (+coverage map; STScI-only)'
STSCI_URL = 'https://archive.stsci.edu/cgi-bin/dss_search'

def build_params(ra_deg: float, dec_deg: float, size_arcmin: float, v: str = '1') -> Tuple[str, dict, dict]:
    params = {
        'v': v,
        'r': f'{ra_deg:.6f}', 'd': f'{dec_deg:.6f}', 'e': 'J2000',
        'h': f'{size_arcmin:.2f}', 'w': f'{size_arcmin:.2f}',
        'f': 'fits', 'c': 'none', 'fov': 'NONE', 'v3': ''
    }
    headers = {'User-Agent': DEF_UA}
    return STSCI_URL, params, headers

def http_get(url: str, params: dict, headers: dict, timeout: float = 60.0) -> bytes:
    s = requests.Session()
    rtry = Retry(total=4, backoff_factor=0.6, status_forcelist=[502, 503, 504, 429])
    s.mount('https://', HTTPAdapter(max_retries=rtry))
    s.mount('http://', HTTPAdapter(max_retries=rtry))
    r = s.get(url, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.content

def normalize_fits(buf: bytes) -> bytes:
    # Accept gzipped FITS or plain FITS. Minimal check.
    if len(buf) >= 2 and buf[0] == 0x1F and buf[1] == 0x8B:
        import gzip
        try:
            data = gzip.decompress(buf)
            return data
        except Exception:
            return buf
    return buf

def status_from_header(hdr) -> str:
    survey = str(hdr.get('SURVEY', '')).upper()
    # POSS-like
    if ('POSS' in survey) or ('POSS-I' in survey) or ('POSS E' in survey) or ('POSS-E' in survey):
        return 'POSS'
    if survey == 'SUPPLEMENTAL':
        return 'SUPPLEMENTAL'
    if ('SERC' in survey) or ('AAO' in survey) or ('SES' in survey) or ('ER' in survey) or ('EJ' in survey):
        return 'SERC_AAO'
    return 'UNKNOWN'

def probe_point(ra_deg: float, dec_deg: float, size_arcmin: float, v: str = '1', timeout: float = 60.0) -> Tuple[str, str, str, str]:
    url, params, headers = build_params(ra_deg, dec_deg, size_arcmin, v=v)
    try:
        buf = http_get(url, params, headers, timeout=timeout)
        data = normalize_fits(buf)
        # FITS signature check (tolerant)
        sig = data[:80]
        if (b'SIMPLE' not in sig) and (b'XTENSION' not in sig):
            return ('', '', '', 'ERROR_NONFITS')
        with fits.open(BytesIO(data), memmap=False) as hdul:
            hdr = hdul[0].header
            survey = str(hdr.get('SURVEY', '')).upper()
            origin = str(hdr.get('ORIGIN', '')).upper()
            plateid = str(hdr.get('PLATEID', '')).upper()
            status = status_from_header(hdr)
            return (survey, origin, plateid, status)
    except Exception as e:
        return ('', '', '', f'ERROR_{type(e).__name__}')

def main():
    ap = argparse.ArgumentParser(description='Map STScI DSS POSS-I E coverage over an RA/Dec grid (DSS1 via v=1).')
    ap.add_argument('--center-ra', type=float, required=True)
    ap.add_argument('--center-dec', type=float, required=True)
    ap.add_argument('--width-arcmin', type=float, required=True)
    ap.add_argument('--height-arcmin', type=float, required=True)
    ap.add_argument('--step-arcmin', type=float, default=60.0)
    ap.add_argument('--size-arcmin', type=float, default=20.0)
    ap.add_argument('--v', type=str, default='1', choices=['1', '2'])
    ap.add_argument('--out', type=str, default='coverage')
    ap.add_argument('--sleep-ms', type=int, default=200, help='Sleep ms between requests')
    ap.add_argument('--timeout', type=float, default=60.0, help='HTTP timeout per request (seconds)')
    args = ap.parse_args()

    center_ra = args.center_ra
    center_dec = args.center_dec
    w = args.width_arcmin
    h = args.height_arcmin
    step = max(1.0, args.step_arcmin)

    nx = max(1, int(math.floor(w / step)))
    ny = max(1, int(math.floor(h / step)))
    half_w_deg = (w / 60.0) / 2.0
    half_h_deg = (h / 60.0) / 2.0
    step_deg = step / 60.0

    ras = [center_ra - half_w_deg + i * step_deg for i in range(nx + 1)]
    decs = [center_dec - half_h_deg + j * step_deg for j in range(ny + 1)]

    out_csv = Path(f'{args.out}.csv')
    out_txt = Path(f'{args.out}.txt')

    print(f'[INFO] Grid: {len(ras)} x {len(decs)} points (step={step} arcmin), DSS v={args.v}, size={args.size_arcmin} arcmin')
    print(f"[INFO] Center: RA={center_ra:.3f} Dec={center_dec:.3f}; width={w:.1f}' height={h:.1f}'")

    rows = []
    for dec in decs:
        for ra in ras:
            survey, origin, plateid, status = probe_point(ra, dec, args.size_arcmin, v=args.v, timeout=args.timeout)
            rows.append({'ra_deg': ra, 'dec_deg': dec, 'v': args.v,
                         'survey': survey, 'origin': origin, 'plateid': plateid, 'status': status})
            print(f'[POINT] RA={ra:.5f} Dec={dec:.5f} -> {status} (SURVEY={survey})')
            time.sleep(args.sleep_ms / 1000.0)

    with out_csv.open('w', newline='', encoding='utf-8') as f:
        wcsv = csv.DictWriter(f, fieldnames=['ra_deg', 'dec_deg', 'v', 'survey', 'origin', 'plateid', 'status'])
        wcsv.writeheader()
        for r in rows:
            wcsv.writerow(r)

    # Build ASCII map with proper newlines
    grid_lines = []
    idx = 0
    for _j in range(len(decs)):
        line_chars = []
        for _i in range(len(ras)):
            st = rows[idx]['status']
            if st == 'POSS':
                ch = 'P'
            elif st == 'SERC_AAO':
                ch = 'S'
            elif st == 'SUPPLEMENTAL':
                ch = 'U'
            elif st.startswith('ERROR_'):
                ch = 'X'
            else:
                ch = '.'
            line_chars.append(ch)
            idx += 1
        grid_lines.append(''.join(line_chars))

    with out_txt.open('w', encoding='utf-8') as f:
        f.write('# Coverage ASCII map (north at top)\n')
        f.write('# Legend: P=POSS, S=SERC/AAO, U=SUPPLEMENTAL, X=ERROR, .=UNKNOWN\n')
        f.write(f"# Center RA={center_ra:.3f} Dec={center_dec:.3f} width={w:.1f}' height={h:.1f}' step={step:.1f}'\n")
        for line in grid_lines:
            f.write(line + '\n')

    print(f'[INFO] Wrote CSV: {out_csv}')
    print(f'[INFO] Wrote ASCII map: {out_txt}')

if __name__ == '__main__':
    sys.exit(main())