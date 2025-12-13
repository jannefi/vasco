
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Map VASCO tiles to IRSA DSS1-red plates using local headers (JSON sidecars preferred).

Priority of data sources:
1) Tile JSON sidecar:   data/tiles/<tile>/raw/*.fits.header.json
2) IRSA JSON sidecar:   ./data/dss1red_headers/.../*.fits.header.json (or as passed via --irsa-json-dir)
3) IRSA index CSV:      irsa_dss1red_index.csv (as passed via --irsa-index)
4) IRSA FITS headers:   ./data/possi-red-plates/.../*.fits  (fallback only if needed)

Strict mode options:
- --no-positional-when-region-present
    If the tile has REGION but IRSA rows do not, skip positional fallback and mark unresolved.
- --max-positional-sep-deg <float> (default 4.0)
    Reject positional fallback with separation >= cap; mark unresolved.

Outputs (default to ./data/metadata):
- tile_to_dss1red.csv
- mapping_warnings.csv
"""

import argparse
import csv
import glob
import json
import os
from math import radians, sin, cos, atan2, sqrt
from pathlib import Path

# astropy is only required when FITS header fallback is actually used
try:
    from astropy.io import fits
except Exception:
    fits = None

# Recognized FITS extensions
VALID_EXTS = ('.fit', '.fits', '.fit.gz', '.fits.gz', '.fz', '.fz.gz', '.fit.fz', '.fits.fz')


# -------------------------- utilities --------------------------
def read_json(path: str):
    """Read a JSON file and return dict."""
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_header_obj(data: dict):
    """
    Tile/IRSA sidecars store keys verbatim under 'header'.
    If the JSON is flat, tolerate that by returning the dict itself.
    """
    return data.get('header', data)


def ang_distance_deg(ra1_deg: float, dec1_deg: float, ra2_deg: float, dec2_deg: float) -> float:
    """Spherical distance in degrees (ICRS) using haversine."""
    r1, d1, r2, d2 = map(radians, [ra1_deg, dec1_deg, ra2_deg, dec2_deg])
    sd = sin((d2 - d1) / 2.0) ** 2
    sr = sin((r2 - r1) / 2.0) ** 2
    a = sd + cos(d1) * cos(d2) * sr
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return c * (180.0 / 3.141592653589793)


def normalize_str(x):
    return str(x).strip() if x is not None else ''


# --------------------- IRSA rows builders ----------------------
def build_irsa_rows_from_json(json_root: str):
    """Collect IRSA plate rows from JSON sidecars."""
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
    """Collect IRSA plate rows from a CSV index."""
    rows = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    return rows


def build_irsa_rows_from_fits(irsa_root: str):
    """Fallback: collect IRSA plate rows by opening FITS headers."""
    if fits is None:
        raise SystemExit('astropy is required to read IRSA FITS headers. Install via "pip install astropy".')

    rows = []
    for ext in VALID_EXTS:
        for plate in glob.glob(os.path.join(irsa_root, '**', f'*{ext}'), recursive=True):
            try:
                with fits.open(plate, memmap=False) as hdul:
                    h = hdul[0].header
                rows.append({
                    'filename': os.path.basename(plate),
                    'REGION': normalize_str(h.get('REGION')),
                    'SURVEY': normalize_str(h.get('SURVEY')),
                    'PLATEID': normalize_str(h.get('PLATEID')),
                    'PLTLABEL': normalize_str(h.get('PLTLABEL')),
                    'DATE-OBS': normalize_str(h.get('DATE-OBS')),
                    'PLATERA': h.get('PLATERA'),
                    'PLATEDEC': h.get('PLATEDEC'),
                    'SCANNUM': normalize_str(h.get('SCANNUM')),
                })
            except Exception as e:
                print(f'[WARN] IRSA FITS read failed: {plate} :: {e}')
    return rows


def build_region_index(rows):
    """Map REGION → list of candidate IRSA rows."""
    idx = {}
    for r in rows:
        reg = r.get('REGION', '')
        if reg:
            idx.setdefault(reg, []).append(r)
    return idx


# ------------------------ tile readers ------------------------
def read_tile_header_json(tile_dir: str):
    """Pick a tile header JSON (prefer dss1-red_*.fits.header.json)."""
    raw = os.path.join(tile_dir, 'raw')
    cand = sorted(glob.glob(os.path.join(raw, '*.fits.header.json')))
    if not cand:
        return None, None
    preferred = [c for c in cand if os.path.basename(c).startswith('dss1-red_')]
    target = preferred[0] if preferred else cand[0]
    data = read_json(target)
    hdr = get_header_obj(data)
    return hdr, os.path.basename(target).replace('.header.json', '')


# ---------------------------- main ----------------------------
def main():
    ap = argparse.ArgumentParser(description='Map tiles to IRSA DSS1-red plates using local headers.')
    ap.add_argument('--tiles-dir', required=True, help='Root folder of tiles (expects raw/*.fits.header.json)')
    ap.add_argument('--irsa-dir', default='', help='Root folder of IRSA DSS1-red FITS plates (fallback only)')
    ap.add_argument('--irsa-json-dir', default='', help='Root folder of IRSA JSON sidecars (preferred)')
    ap.add_argument('--irsa-index', default='', help='Path to irsa_dss1red_index.csv (optional)')
    ap.add_argument('--out-dir', default='./data/metadata', help='Output directory for CSV files')
    ap.add_argument('--sep-threshold-deg', type=float, default=1.0,
                    help='Flag positional matches with separation >= threshold (for warnings)')
    ap.add_argument('--no-positional-when-region-present', action='store_true',
                    help='Skip positional fallback if tile has REGION but IRSA rows do not')
    ap.add_argument('--max-positional-sep-deg', type=float, default=4.0,
                    help='Reject positional fallback with separation >= cap (plate radius ~3.25 deg; default 4.0)')
    args = ap.parse_args()

    # Ensure output directory exists
    os.makedirs(args.out_dir, exist_ok=True)

    # Build IRSA rows from best available source
    if args.irsa_json_dir:
        irsa_rows = build_irsa_rows_from_json(args.irsa_json_dir)
    elif args.irsa_index:
        irsa_rows = build_irsa_rows_from_csv(args.irsa_index)
    elif args.irsa_dir:
        irsa_rows = build_irsa_rows_from_fits(args.irsa_dir)
    else:
        raise SystemExit('Provide one of: --irsa-json-dir, --irsa-index, or --irsa-dir')

    if not irsa_rows:
        raise SystemExit('[ERROR] No IRSA rows available for mapping')

    region_idx = build_region_index(irsa_rows)

    out_rows = []
    warn_rows = []

    # Walk tile folders
    for tile_dir in sorted(glob.glob(os.path.join(args.tiles_dir, 'tile-*'))):
        if not os.path.isdir(tile_dir):
            continue
        tile_id = os.path.basename(tile_dir)

        th, tile_fits_base = read_tile_header_json(tile_dir)
        if th is None:
            warn_rows.append({'tile_id': tile_id, 'reason': 'no_tile_header_json', 'sep_deg': ''})
            continue

        t_region = normalize_str(th.get('REGION'))
        t_survey = normalize_str(th.get('SURVEY'))
        t_date = normalize_str(th.get('DATE-OBS'))
        t_ra = th.get('CRVAL1') if th.get('CRVAL1') is not None else th.get('PLATERA')
        t_dec = th.get('CRVAL2') if th.get('CRVAL2') is not None else th.get('PLATEDEC')

        match = None
        match_sep = None
        unresolved = False
        unresolved_reason = ''

        # REGION-based match first
        if t_region and t_region in region_idx:
            candidates = region_idx[t_region]
            match = candidates[0]
            # Prefer same SURVEY if duplicates
            for c in candidates:
                if t_survey and c.get('SURVEY', '') == t_survey:
                    match = c
                    break
        else:
            # Strict rule: if tile has REGION but IRSA rows don't, skip positional fallback
            if t_region and args.no_positional_when_region_present:
                unresolved = True
                unresolved_reason = 'region_missing_in_irsa_index'
            else:
                # Positional fallback if tile has RA/Dec
                if (t_ra is not None) and (t_dec is not None):
                    best = None
                    best_sep = 1e9
                    for r in irsa_rows:
                        pra = r.get('PLATERA')
                        pdec = r.get('PLATEDEC')
                        if pra in (None, '') or pdec in (None, ''):
                            continue
                        try:
                            sep = ang_distance_deg(float(t_ra), float(t_dec), float(pra), float(pdec))
                        except Exception:
                            continue
                        if sep < best_sep:
                            best_sep, best = sep, r
                    match, match_sep = best, best_sep
                    if match is None:
                        unresolved = True
                        unresolved_reason = 'no_positional_candidate'
                    elif (match_sep is not None) and (match_sep >= args.max_positional_sep_deg):
                        unresolved = True
                        unresolved_reason = 'positional_sep_exceeds_max'
                else:
                    unresolved = True
                    unresolved_reason = 'no_region_no_position'

        # Compose output row (leave IRSA fields empty if unresolved)
        row = {
            'tile_id': tile_id,
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
            warn_rows.append({
                'tile_id': tile_id,
                'reason': unresolved_reason,
                'sep_deg': f'{match_sep:.4f}' if match_sep is not None else ''
            })
        elif (match_sep is not None) and (match_sep >= args.sep_threshold_deg):
            # Log flagged (but accepted) positional matches
            warn_rows.append({
                'tile_id': tile_id,
                'reason': 'positional_fallback_large_sep',
                'sep_deg': f'{match_sep:.4f}'
            })

    if not out_rows:
        raise SystemExit('[ERROR] No tile mappings produced')

    # Write outputs
    out_path = Path(args.out_dir) / 'tile_to_dss1red.csv'
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=list(out_rows[0].keys()))
        w.writeheader()
        w.writerows(out_rows)
    print(f'[OK] Wrote {out_path} with {len(out_rows)} rows')

    if warn_rows:
        warn_path = Path(args.out_dir) / 'mapping_warnings.csv'
        with open(warn_path, 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=list(warn_rows[0].keys()))
            w.writeheader()
            w.writerows(warn_rows)
        print(f'[OK] Wrote {warn_path} with {len(warn_rows)} warnings')

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
