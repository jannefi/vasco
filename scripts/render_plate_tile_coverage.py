#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
render_plate_tile_coverage.py — Per-plate overlays of all tiles (v4.3)

What's new in v4.3
------------------
• Robust plate-header resolver (accepts *_30arcmin.fits in sidecars, normalizes
  to full-plate header JSON).
• Orientation enforcement (auto East-left, North-up) so plate overlays match
  Aladin/DSS display conventions regardless of header omissions.
• Keeps robust JSON loader; retains v4.2 geometry (no plate WCS required).
"""

import argparse
import csv
import json
import math
import re
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt
from astropy.io.fits import Header
from astropy.wcs import WCS

RAD2AS = 206264.80624709636
SIZE_TAG_RE = re.compile(r'_(\d+(?:\.\d+)?)arcmin', flags=re.IGNORECASE)

# ----------------------- Robust JSON loader ----------------------

def robust_json_load(path: Path):
    try:
        b = path.read_bytes()
    except Exception as e:
        return None, f'read_bytes failed: {e}'
    for enc in ('utf-8', 'utf-8-sig', 'latin-1'):
        try:
            s = b.decode(enc)
            try:
                return json.loads(s), None
            except Exception:
                try:
                    return json.loads(s, strict=False), None
                except Exception:
                    pass
        except Exception:
            pass
    try:
        s2 = ''.join(ch for ch in b.decode('latin-1', errors='ignore') if (ch >= ' ' or ch in '\r\n\t'))
        return json.loads(s2, strict=False), None
    except Exception as e:
        return None, f'json decode failed after fallbacks: {e}'

# ---------------------------- Helpers ----------------------------

PATTERNS = ['tile-RA*-DEC*','tile_RA*_DEC*','tile-RA*_DEC*','tile_RA*-DEC*']

def iter_tile_dirs(tiles_root: Path):
    if tiles_root.exists():
        for pat in PATTERNS:
            for p in sorted(tiles_root.glob(pat)):
                if p.is_dir():
                    yield p
    sharded = tiles_root.parent / 'tiles_by_sky'
    if sharded.exists():
        for pat in PATTERNS:
            for p in sorted(sharded.glob(f'ra_bin=*/dec_bin=*/{pat}')):
                if p.is_dir():
                    yield p


def read_title_sidecar(raw_dir: Path):
    sidecar = raw_dir / 'dss1red_title.txt'
    out = {}
    if not sidecar.exists():
        return out
    try:
        for line in sidecar.read_text(encoding='utf-8', errors='ignore').splitlines():
            if ':' not in line:
                continue
            k, v = line.split(':', 1)
            out[k.strip().upper()] = v.strip()
    except Exception:
        pass
    return out


def pick_header_dict(any_json: dict):
    if isinstance(any_json, dict):
        if 'header' in any_json and isinstance(any_json['header'], dict):
            return any_json['header']
        if 'selected' in any_json and isinstance(any_json['selected'], dict):
            return any_json['selected']
    return any_json


def _candidate_plate_header_names(base_name: str):
    base = Path(base_name).name
    no_size = SIZE_TAG_RE.sub('', base)
    cands = []
    if no_size.endswith('.fits'):
        cands += [f"{no_size}.header.json", f"{no_size[:-5]}.header.json", f"{no_size}.fits.header.json"]
    else:
        cands += [f"{no_size}.header.json", f"{no_size}.fits.header.json"]
    cands.append(f"{base}.header.json")
    out, seen = [], set()
    for n in cands:
        if n and n not in seen:
            out.append(n); seen.add(n)
    return out


def load_tile_wcs(tile_dir: Path):
    raw = tile_dir / 'raw'
    cands = sorted(list(raw.glob('*.header.json')))
    if not cands:
        return None, None, None, 'no .header.json under raw/'
    pref = [p for p in cands if '30arcmin' in p.name]
    tj = (pref[0] if pref else cands[0])
    data, err = robust_json_load(tj)
    if data is None:
        return None, None, None, f'robust_json_load failed for {tj.name}: {err}'
    hdr = pick_header_dict(data)
    try:
        H = Header(); H['NAXIS']=2
        H['NAXIS1']=int((hdr.get('NAXIS1',0) or 0))
        H['NAXIS2']=int((hdr.get('NAXIS2',0) or 0))
        for k in ('CRPIX1','CRPIX2','CRVAL1','CRVAL2'):
            v = hdr.get(k, None)
            if v is not None: H[k]=float(v)
        cd_keys=('CD1_1','CD1_2','CD2_1','CD2_2')
        if all((hdr.get(k, None) is not None) for k in cd_keys):
            for k in cd_keys: H[k]=float(hdr[k])
        else:
            v1=hdr.get('CDELT1', None); v2=hdr.get('CDELT2', None); vr=hdr.get('CROTA2', None)
            if v1 is not None: H['CDELT1']=float(v1)
            if v2 is not None: H['CDELT2']=float(v2)
            if vr is not None: H['CROTA2']=float(vr)
        H['CTYPE1']=hdr.get('CTYPE1','RA---TAN')
        H['CTYPE2']=hdr.get('CTYPE2','DEC--TAN')
        w = WCS(H)
        return w, H['NAXIS1'], H['NAXIS2'], None
    except Exception as e:
        return None, int(hdr.get('NAXIS1',0) or 0), int(hdr.get('NAXIS2',0) or 0), f'tile WCS build failed: {e}'


def resolve_plate_json_from_fits(tile_dir: Path, roots: list[Path]):
    raw = tile_dir / 'raw'
    meta = read_title_sidecar(raw)
    fits_name = (meta.get('FITS','') or '').strip()
    if not fits_name:
        return None
    base = Path(fits_name).name
    header_names = _candidate_plate_header_names(base)
    for root in roots:
        for hn in header_names:
            cand = root / hn
            if cand.exists():
                return cand
        nb = SIZE_TAG_RE.sub('', base)
        nb = nb[:-5] if nb.endswith('.fits') else nb
        matches = list(root.glob(f"{nb}*.header.json"))
        if len(matches) == 1:
            return matches[0]
    for hn in header_names:
        cand2 = raw / hn
        if cand2.exists():
            return cand2
    return None

# ----------------------------- geometry -----------------------------

def radec_to_plate_pixels_gnomonic(ra_deg: np.ndarray, dec_deg: np.ndarray, plate: dict) -> np.ndarray:
    ra0 = math.radians(plate['center_ra'])
    de0 = math.radians(plate['center_dec'])
    as_px_x = plate['as_per_px_x']; as_px_y = plate['as_per_px_y']
    cx, cy = plate['cx'], plate['cy']
    ra = np.radians(ra_deg); de = np.radians(dec_deg)
    dra = (ra - ra0 + np.pi) % (2*np.pi) - np.pi
    sin_de = np.sin(de); cos_de = np.cos(de)
    sin_de0= math.sin(de0); cos_de0= math.cos(de0)
    cos_dra= np.cos(dra); sin_dra= np.sin(dra)
    denom = (sin_de0*sin_de + cos_de0*cos_de*cos_dra)
    denom = np.where(np.abs(denom) < 1e-12, np.nan, denom)
    xi = (cos_de * sin_dra) / denom
    eta = (cos_de0*sin_de - sin_de0*cos_de*cos_dra) / denom
    xi_as = xi * RAD2AS; eta_as = eta * RAD2AS
    x = cx + (xi_as / as_px_x)
    y = cy + (eta_as / as_px_y)
    return np.stack([x, y], axis=1)


def enforce_east_left_orientation(plate: dict):
    eps_arcsec = 5.0
    eps_deg = eps_arcsec / 3600.0
    ra0 = plate['center_ra']; dec0 = plate['center_dec']
    xy_east = radec_to_plate_pixels_gnomonic(np.array([ra0 + eps_deg]), np.array([dec0]), plate)[0]
    xy_north = radec_to_plate_pixels_gnomonic(np.array([ra0]), np.array([dec0 + eps_deg]), plate)[0]
    cx, cy = plate['cx'], plate['cy']
    flip_x = (xy_east[0] > cx)
    flip_y = (xy_north[1] < cy)
    return {'flip_x': bool(flip_x), 'flip_y': bool(flip_y)}


def min_edge_distance_px(points_xy: np.ndarray, plate_nx: int, plate_ny: int) -> float:
    if points_xy.size == 0:
        return float('nan')
    dvals = []
    for x, y in points_xy:
        if np.isnan(x) or np.isnan(y):
            continue
        if x < 1 or x > plate_nx or y < 1 or y > plate_ny:
            d = -min(abs(x-1), abs(plate_nx-x), abs(y-1), abs(plate_ny-y))
        else:
            d = min(x-1, plate_nx-x, y-1, plate_ny-y)
        dvals.append(d)
    if not dvals:
        return float('nan')
    return float(min(dvals))

# --------------------------------- main ---------------------------------

def main():
    ap = argparse.ArgumentParser(description='Render per-plate coverage overlays of all tiles (v4.3, robust resolver + orientation)')
    ap.add_argument('--tiles-root', default='./data/tiles')
    ap.add_argument('--dss-headers', default='./data/dss1red-headers,./data/dss1red_headers')
    ap.add_argument('--out-dir', default='./data/metadata/plate_coverage')
    ap.add_argument('--fast-square', action='store_true')
    ap.add_argument('--label', action='store_true', help='label tiles with tile_id')
    ap.add_argument('--threshold-px', type=float, default=200.0)
    ap.add_argument('--threshold-frac', type=float, default=0.02)
    ap.add_argument('--max-plates', type=int, default=0, help='limit number of plates rendered (0 = all)')
    args = ap.parse_args()

    tiles_root = Path(args.tiles_root)
    dss_roots = [Path(s.strip()) for s in args.dss_headers.split(',') if s.strip()]
    out_dir = Path(args.out_dir)

    plate_to_tiles = {}
    for td in iter_tile_dirs(tiles_root):
        pj = resolve_plate_json_from_fits(td, dss_roots)
        if pj is None:
            continue
        plate_to_tiles.setdefault(pj, []).append(td)

    # index CSV
    out_dir.mkdir(parents=True, exist_ok=True)
    index_csv = out_dir/'plates_with_tiles.csv'
    with index_csv.open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['plate_json','tiles_count'])
        w.writeheader()
        for pj, tiles in sorted(plate_to_tiles.items(), key=lambda kv: kv[0].name):
            w.writerow({'plate_json': pj.name, 'tiles_count': len(tiles)})

    # overlays per plate
    count = 0
    for pj, tiles in sorted(plate_to_tiles.items(), key=lambda kv: kv[0].name):
        if args.max_plates and count >= args.max_plates:
            break
        plate, perr = (None, None)
        data, err = robust_json_load(pj)
        if data is None:
            print(f'[SKIP] plate {pj.name}: robust_json_load failed: {err}')
            continue
        hdr = pick_header_dict(data)
        try:
            nax1 = int(hdr.get('NAXIS1',0) or 0)
            nax2 = int(hdr.get('NAXIS2',0) or 0)
            pl_ra = float(hdr['PLATERA']); pl_de = float(hdr['PLATEDEC'])
            pltscale = float(hdr['PLTSCALE'])
            xp_um = float(hdr['XPIXELSZ']); yp_um = float(hdr['YPIXELSZ'])
            plate = {
                'nax1': nax1, 'nax2': nax2,
                'center_ra': pl_ra, 'center_dec': pl_de,
                'cx': nax1/2.0, 'cy': nax2/2.0,
                'as_per_px_x': pltscale*(xp_um/1000.0),
                'as_per_px_y': pltscale*(yp_um/1000.0)
            }
        except Exception as e:
            print(f'[SKIP] plate {pj.name}: plate core build failed: {e}')
            continue

        orient = enforce_east_left_orientation(plate)

        name = pj.stem.replace('.header','')
        fig_path = out_dir / f'{name}.png'
        fig, ax = plt.subplots(figsize=(9, 9))
        ax.set_title(f'{name} — tiles: {len(tiles)}', fontsize=12)
        ax.plot([1, plate['nax1'], plate['nax1'], 1, 1], [1, 1, plate['nax2'], plate['nax2'], 1], 'k-', lw=1.0, alpha=0.8)
        ax.set_xlim(0, plate['nax1']+1); ax.set_ylim(0, plate['nax2']+1)
        ax.set_aspect('equal', adjustable='box'); ax.grid(ls=':', alpha=0.3)

        colors = {'edge_touch':'tab:red','near_edge':'tab:orange','core':'tab:blue'}
        for td in tiles:
            twcs, tnx, tny, terr = load_tile_wcs(td)
            if twcs is None or not tnx or not tny:
                print(f'[SKIP] tile {td.name}: {terr}')
                continue
            # polygon from 4 corners + close
            corners = np.array([[1,1],[tnx,1],[tnx,tny],[1,tny],[1,1]], dtype=float)
            mids = np.array([[tnx/2,1],[tnx,tny/2],[tnx/2,tny],[1,tny/2]], dtype=float)
            samples = np.vstack([corners, mids])
            world = twcs.all_pix2world(samples, 1)
            poly_world = world[:5,:]
            poly = radec_to_plate_pixels_gnomonic(poly_world[:,0], poly_world[:,1], plate)
            if orient['flip_x']:
                poly[:,0] = 2.0*plate['cx'] - poly[:,0]
            if orient['flip_y']:
                poly[:,1] = 2.0*plate['cy'] - poly[:,1]

            px_margin = min_edge_distance_px(poly, plate['nax1'], plate['nax2'])
            if math.isnan(px_margin):
                cls = 'near_edge'
            elif px_margin < 0:
                cls = 'edge_touch'
            else:
                thresh = max(args.threshold_px, args.threshold_frac * min(plate['nax1'], plate['nax2']))
                cls = 'core' if px_margin >= thresh else 'near_edge'

            ax.plot(poly[:,0], poly[:,1], '-', color=colors[cls], lw=1.2, alpha=0.85)
            if args.label:
                ax.text(poly[0,0], poly[0,1], td.name, fontsize=7, color=colors[cls])

        fig.tight_layout(); fig.savefig(fig_path, dpi=140); plt.close(fig)
        count += 1
        print(f'[OK] wrote {fig_path} (tiles={len(tiles)}) orient: flip_x={orient["flip_x"]} flip_y={orient["flip_y"]}')

    print(f'[OK] wrote index: {index_csv} plates={len(plate_to_tiles)}')

if __name__ == '__main__':
    main()
