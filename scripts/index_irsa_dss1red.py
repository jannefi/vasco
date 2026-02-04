
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Index local IRSA DSS1-red plates and (optionally) export verbatim FITS headers
as JSON sidecars in a separate metadata folder.

Outputs (defaults to ./data/metadata):
- ./data/metadata/irsa_dss1red_index.csv
- ./data/dss1red_headers/**.fits.header.json (when --export-json-headers)
"""
import argparse
import csv
import glob
import json
import os
from datetime import datetime, timezone
from pathlib import Path

try:
    from astropy.io import fits
except Exception:
    fits = None

VALID_EXTS = ('.fit', '.fits', '.fit.gz', '.fits.gz', '.fz', '.fz.gz', '.fit.fz', '.fits.fz')

DEF_FIELDS = [
    'filename', 'REGION', 'SURVEY', 'PLATEID', 'PLTLABEL', 'DATE-OBS',
    'PLATERA', 'PLATEDEC', 'SCANNUM', 'json_header_path'
]

def find_fits_files(root: str):
    for ext in VALID_EXTS:
        for fn in glob.glob(os.path.join(root, '**', f'*{ext}'), recursive=True):
            yield fn


def read_header(fn: str):
    if fits is None:
        raise RuntimeError('astropy is required to read FITS headers. Install via "pip install astropy".')
    with fits.open(fn, memmap=False) as hdul:
        return hdul[0].header


def serialize_header(header):
    # Convert FITS Header to JSON-serializable dict (verbatim keys)
    out = {}
    for k, v in header.items():
        try:
            if isinstance(v, (str, int, float, bool)) or v is None:
                out[k] = v
            else:
                out[k] = float(v) if hasattr(v, '__float__') else str(v)
        except Exception:
            out[k] = str(v)
    return out


def export_json_sidecar(irsa_dir: str, json_dir: str, plate_path: str, header):
    rel = os.path.relpath(plate_path, irsa_dir)
    rel_dir = os.path.dirname(rel)
    base = os.path.basename(plate_path)
    side_name = base + '.header.json'
    target_dir = os.path.join(json_dir, rel_dir)
    os.makedirs(target_dir, exist_ok=True)
    target = os.path.join(target_dir, side_name)

    meta = {
        'source_file': plate_path,
        'exported_utc': datetime.now(timezone.utc).isoformat(),
        'size_bytes': os.path.getsize(plate_path),
    }
    payload = {
        'meta': meta,
        'header': serialize_header(header),
    }
    with open(target, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return target


def main():
    ap = argparse.ArgumentParser(description='Index IRSA DSS1-red plates; optionally export JSON headers.')
    ap.add_argument('--irsa-dir', required=True)
    ap.add_argument('--export-json-headers', action='store_true')
    ap.add_argument('--json-placement', choices=['metadata-dir', 'alongside'], default='metadata-dir')
    ap.add_argument('--json-dir', default='./data/dss1red_headers')
    ap.add_argument('--out-dir', default='./data/metadata')
    ap.add_argument('--overwrite', choices=['true', 'false'], default='false')
    args = ap.parse_args()

    irsa_dir = args.irsa_dir
    export_json = args.export_json_headers
    placement = args.json_placement
    overwrite = (args.overwrite.lower() == 'true')

    out_dir = args.out_dir
    os.makedirs(out_dir, exist_ok=True)

    if export_json and placement == 'metadata-dir' and not args.json_dir:
        raise SystemExit('--json-dir is required when json-placement=metadata-dir')

    json_dir = args.json_dir if placement == 'metadata-dir' else ''

    rows = []
    exported = 0
    skipped = 0

    for plate in find_fits_files(irsa_dir):
        try:
            hdr = read_header(plate)
        except Exception as e:
            print(f'[WARN] Cannot read FITS: {plate} :: {e}')
            continue

        reg = str(hdr.get('REGION', '')).strip()
        surv = str(hdr.get('SURVEY', '')).strip()
        pid = str(hdr.get('PLATEID', '')).strip()
        plbl = str(hdr.get('PLTLABEL', '')).strip()
        date_obs = str(hdr.get('DATE-OBS', '')).strip()
        pra = hdr.get('PLATERA')
        pdec = hdr.get('PLATEDEC')
        scn = str(hdr.get('SCANNUM', '')).strip()

        json_header_path = ''
        if export_json:
            if placement == 'metadata-dir':
                rel = os.path.relpath(plate, irsa_dir)
                target_dir = os.path.join(json_dir, os.path.dirname(rel))
                os.makedirs(target_dir, exist_ok=True)
                target = os.path.join(target_dir, os.path.basename(plate) + '.header.json')
            else:
                target = plate + '.header.json'

            if os.path.exists(target) and not overwrite:
                json_header_path = os.path.abspath(target)
                skipped += 1
            else:
                try:
                    json_header_path = export_json_sidecar(irsa_dir, json_dir if placement=='metadata-dir' else os.path.dirname(plate), plate, hdr)
                    exported += 1
                except Exception as e:
                    print(f'[WARN] JSON export failed: {plate} :: {e}')

        rows.append({
            'filename': os.path.relpath(plate, irsa_dir),
            'REGION': reg,
            'SURVEY': surv,
            'PLATEID': pid,
            'PLTLABEL': plbl,
            'DATE-OBS': date_obs,
            'PLATERA': pra if pra is not None else '',
            'PLATEDEC': pdec if pdec is not None else '',
            'SCANNUM': scn,
            'json_header_path': json_header_path,
        })

    if not rows:
        print(f'[ERROR] No FITS plates found under {irsa_dir}')
        return 2

    idx_path = Path(out_dir) / 'irsa_dss1red_index.csv'
    with open(idx_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=DEF_FIELDS)
        w.writeheader()
        w.writerows(rows)
    print(f'[OK] Wrote {idx_path} with {len(rows)} entries')
    if export_json:
        print(f'[OK] JSON sidecars: exported={exported}, skipped={skipped}')
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
