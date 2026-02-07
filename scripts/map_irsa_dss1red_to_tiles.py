#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Map VASCO tiles to DSS1-red plates (v4.4)
- Repo-first: reads canonical plate headers from metadata/plates/headers/
- REGION-first mapping (plate_id == REGION), with optional positional fallback
- Outputs:
    ./data/metadata/tile_to_dss1red.csv
    ./data/metadata/mapping_warnings.csv
"""

import argparse
import csv
import glob
import json
import os
from math import radians, sin, cos, atan2, sqrt
from pathlib import Path

# ------------------------------ helpers ------------------------------
def read_json(path: str):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def get_header_obj(data: dict):
    # tolerate nested {'header': {...}} or flat dicts
    return data.get('header', data)

def ang_distance_deg(ra1_deg: float, dec1_deg: float, ra2_deg: float, dec2_deg: float) -> float:
    r1, d1, r2, d2 = map(radians, [ra1_deg, dec1_deg, ra2_deg, dec2_deg])
    sd = sin((d2 - d1) / 2.0)**2
    sr = sin((r2 - r1) / 2.0)**2
    a = sd + cos(d1) * cos(d2) * sr
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return c * (180.0 / 3.141592653589793)

def normalize_str(x):
    return str(x).strip() if x is not None else ''

def build_irsa_rows_from_repo(headers_dir: str):
    """
    Scan repo headers; accept canonical names like dss1red_<REGION>.fits.header.json.
    """
    rows = []
    for jpath in glob.glob(os.path.join(headers_dir, '**', '*.fits.header.json'), recursive=True):
        data = read_json(jpath)
        hdr = get_header_obj(data)
        rows.append({
            'filename': os.path.basename(jpath).replace('.header.json', ''),  # keep for traceability
            'REGION': normalize_str(hdr.get('REGION')),
            'SURVEY': normalize_str(hdr.get('SURVEY')),
            'PLATEID': normalize_str(hdr.get('PLATEID')),
            'PLTLABEL': normalize_str(hdr.get('PLTLABEL')),
            'DATE-OBS': normalize_str(hdr.get('DATE-OBS')),
            'PLATERA': hdr.get('PLATERA'),
            'PLATEDEC': hdr.get('PLATEDEC'),
            'SCANNUM': normalize_str(hdr.get('SCANNUM')),
        })
    return rows

def build_irsa_rows_from_json(json_root: str):
    # keep compatibility if someone passes a non-canonical dir
    rows = []
    for jpath in glob.glob(os.path.join(json_root, '**', '*.header.json'), recursive=True):
        data = read_json(jpath)
        hdr = get_header_obj(data)
        rows.append({
            'filename': os.path.basename(jpath).replace('.header.json', ''),
            'REGION': normalize_str(hdr.get('REGION')),
            'SURVEY': normalize_str(hdr.get('SURVEY')),
            'PLATEID': normalize_str(hdr.get('PLATEID')),
            'PLTLABEL': normalize_str(hdr.get('PLTLABEL')),
            'DATE-OBS': normalize_str(hdr.get('DATE-OBS')),
            'PLATERA': hdr.get('PLATERA'),
            'PLATEDEC': hdr.get('PLATEDEC'),
            'SCANNUM': normalize_str(hdr.get('SCANNUM')),
        })
    return rows

def build_irsa_rows_from_csv(csv_path: str):
    rows = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    return rows

def build_region_index(rows):
    idx = {}
    for r in rows:
        reg = r.get('REGION', '')
        if reg:
            idx.setdefault(reg, []).append(r)
    return idx

def read_tile_header_json(tile_dir: str):
    """
    Prefer any *.header.json under <tile>/raw/. If multiple, pick the first;
    tolerate different naming schemes (e.g., <tile_fits>.header.json).
    """
    raw = os.path.join(tile_dir, 'raw')
    cand = sorted(glob.glob(os.path.join(raw, '*.header.json')))
    if not cand:
        return None, None
    target = cand[0]
    data = read_json(target)
    hdr = get_header_obj(data)
    return hdr, os.path.basename(target).replace('.header.json', '')

def iter_tile_dirs_any(tiles_dir: Path):
    if tiles_dir.exists():
        for p in sorted(tiles_dir.glob('tile-*')):
            if p.is_dir():
                yield p
    sharded = tiles_dir.parent / 'tiles_by_sky'
    if sharded.exists():
        for p in sorted(sharded.glob('ra_bin=*/dec_bin=*/tile-*')):
            if p.is_dir():
                yield p

# --------------------------------- main ---------------------------------
def main():
    ap = argparse.ArgumentParser(description='Map tiles to DSS1-red plates using repo headers (REGION-first).')
    ap.add_argument('--tiles-dir', required=True, help='Root folder of tiles (expects raw/*.header.json)')
    # Repo-first default:
    ap.add_argument('--headers-dir', default='metadata/plates/headers', help='Repo headers root (default: metadata/plates/headers)')
    # Compatibility options (optional):
    ap.add_argument('--irsa-json-dir', default='', help='Alternate JSON headers root (optional)')
    ap.add_argument('--irsa-index', default='', help='Optional CSV index (legacy)')
    ap.add_argument('--out-dir', default='./data/metadata', help='Output directory for CSV files')
    ap.add_argument('--sep-threshold-deg', type=float, default=1.0, help='Warn when positional fallback sep >= threshold')
    ap.add_argument('--no-positional-when-region-present', action='store_true', help='Skip positional fallback if tile has REGION but IRSA rows do not')
    ap.add_argument('--max-positional-sep-deg', type=float, default=4.0, help='Reject positional fallback with separation >= cap')
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    # Build IRSA plate rows (repo-first)
    if args.irsa_json_dir:
        irsa_rows = build_irsa_rows_from_json(args.irsa_json_dir)
    elif args.irsa_index:
        irsa_rows = build_irsa_rows_from_csv(args.irsa_index)
    else:
        irsa_rows = build_irsa_rows_from_repo(args.headers_dir)

    if not irsa_rows:
        raise SystemExit('[ERROR] No plate headers available (check --headers-dir or alternatives)')

    region_idx = build_region_index(irsa_rows)

    out_rows = []
    warn_rows = []

    for tile_dir in iter_tile_dirs_any(Path(args.tiles_dir)):
        tile_id = tile_dir.name
        th, tile_fits_base = read_tile_header_json(str(tile_dir))
        if th is None:
            warn_rows.append({'tile_id': tile_id, 'reason': 'no_tile_header_json', 'sep_deg': ''})
            continue

        t_region = normalize_str(th.get('REGION'))
        t_survey = normalize_str(th.get('SURVEY'))
        t_date   = normalize_str(th.get('DATE-OBS'))
        t_ra = th.get('CRVAL1') if th.get('CRVAL1') is not None else th.get('PLATERA')
        t_dec= th.get('CRVAL2') if th.get('CRVAL2') is not None else th.get('PLATEDEC')

        match = None; match_sep = None
        unresolved = False; unresolved_reason = ''

        # REGION-first mapping
        if t_region and t_region in region_idx:
            candidates = region_idx[t_region]
            match = candidates[0]
            # prefer exact SURVEY match when available
            for c in candidates:
                if t_survey and c.get('SURVEY', '') == t_survey:
                    match = c
                    break
        else:
            # Optional positional fallback
            if t_region and args.no_positional_when_region_present:
                unresolved = True; unresolved_reason = 'region_missing_in_repo_headers'
            else:
                if (t_ra is not None) and (t_dec is not None):
                    best = None; best_sep = 1e9
                    for r in irsa_rows:
                        pra = r.get('PLATERA'); pdec = r.get('PLATEDEC')
                        if pra in (None, '') or pdec in (None, ''): continue
                        try:
                            sep = ang_distance_deg(float(t_ra), float(t_dec), float(pra), float(pdec))
                        except Exception:
                            continue
                        if sep < best_sep: best_sep, best = sep, r
                    match, match_sep = best, best_sep
                    if match is None:
                        unresolved = True; unresolved_reason = 'no_positional_candidate'
                    elif (match_sep is not None) and (match_sep >= args.max_positional_sep_deg):
                        unresolved = True; unresolved_reason = 'positional_sep_exceeds_max'
                else:
                    unresolved = True; unresolved_reason = 'no_region_no_position'

        row = {
            'tile_id': tile_id,
            'plate_id': t_region,   # canonical (== REGION)
            'tile_region': t_region,
            'tile_survey': t_survey,
            'tile_date_obs': t_date,
            'tile_fits': tile_fits_base if tile_fits_base else '',
            'irsa_region': '' if unresolved else (match.get('REGION', '') if match else ''),
            'irsa_filename': '' if unresolved else (match.get('filename', '') if match else ''),
            'irsa_survey': '' if unresolved else (match.get('SURVEY', '') if match else ''),
            'irsa_platelabel': '' if unresolved else (match.get('PLTLABEL', '') if match else ''),
            'irsa_plateid': '' if unresolved else (match.get('PLATEID', '') if match else ''),
            'irsa_date_obs': '' if unresolved else (match.get('DATE-OBS', '') if match else ''),
            'irsa_scannum': '' if unresolved else (match.get('SCANNUM', '') if match else ''),
            'irsa_center_sep_deg': '' if unresolved else (f'{match_sep:.4f}' if match_sep is not None else ''),
        }
        out_rows.append(row)

        if unresolved:
            warn_rows.append({'tile_id': tile_id, 'reason': unresolved_reason,
                              'sep_deg': f'{match_sep:.4f}' if match_sep is not None else ''})
        elif (match_sep is not None) and (match_sep >= args.sep_threshold_deg):
            warn_rows.append({'tile_id': tile_id, 'reason': 'positional_fallback_large_sep',
                              'sep_deg': f'{match_sep:.4f}'})

    if not out_rows:
        raise SystemExit('[ERROR] No tile mappings produced')

    out_path = Path(args.out_dir) / 'tile_to_dss1red.csv'
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=list(out_rows[0].keys()))
        w.writeheader(); w.writerows(out_rows)
    print(f'[OK] Wrote {out_path} with {len(out_rows)} rows')

    if warn_rows:
        warn_path = Path(args.out_dir) / 'mapping_warnings.csv'
        with open(warn_path, 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=list(warn_rows[0].keys()))
            w.writeheader(); w.writerows(warn_rows)
        print(f'[OK] Wrote {warn_path} with {len(warn_rows)} warnings')
    return 0

if __name__ == '__main__':
    raise SystemExit(main())