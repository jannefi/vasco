#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
check_tile_plate_edge.py (v4.4 — REGION-first resolver + repo headers)

Changes in v4.4
- Repo-first header resolution by REGION (plate_id) via:
    metadata/plates/headers/dss1red_{REGION}.fits.header.json
- Legacy roots are OFF by default; can be enabled via --legacy-roots
- CSV now includes plate_id for direct referencing
- Geometry/orientation logic kept from v4.3
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
from astropy.coordinates import SkyCoord
import astropy.units as u

TILE_ID_RE = re.compile(r'^tile-RA([+\-]?\d+(?:\.\d+)?)\-DEC([+\-]?\d+(?:\.\d+)?)$')
RAD2AS = 206264.80624709636  # arcsec per radian
SIZE_TAG_RE = re.compile(r'_(\d+(?:\.\d+)?)arcmin', flags=re.IGNORECASE)

# --------------------------- robust JSON loader ---------------------------
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
        s2 = ''.join(ch for ch in b.decode('latin-1', errors='ignore')
                     if (ch >= ' ' or ch in '\r\n\t'))
        return json.loads(s2, strict=False), None
    except Exception as e:
        return None, f'json decode failed after fallbacks: {e}'

# ------------------------------- helpers ---------------------------------
def parse_tile_id(name: str):
    m = TILE_ID_RE.match(name)
    if not m:
        return None
    return float(m.group(1)), float(m.group(2))

def sky_bins(ra_deg: float, dec_deg: float, bin_deg: float = 5.0):
    ra = ra_deg % 360.0
    dec = max(-90.0, min(90.0, dec_deg))
    rb = int(math.floor(ra / bin_deg))
    db = int(math.floor((dec + 90.0) / bin_deg))
    return rb, db

# Discover flat and sharded layouts, hyphen/underscore variants
PATTERNS = ['tile-RA*-DEC*', 'tile_RA*_DEC*', 'tile-RA*_DEC*', 'tile_RA*-DEC*'.replace('-', '_')]

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

# ------------------------- sidecar / header readers -----------------------
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

def resolve_repo_plate_json_by_region(tile_dir: Path, repo_headers: Path):
    """
    REGION-first (plate_id). Build canonical path:
      metadata/plates/headers/dss1red_{REGION}.fits.header.json
    """
    meta = read_title_sidecar(tile_dir / 'raw')
    region = (meta.get('REGION', '') or '').strip()
    if not region:
        return None, None, 'missing REGION in dss1red_title.txt'
    cand = repo_headers / f'dss1red_{region}.fits.header.json'
    if cand.exists():
        return cand, region, ''
    return None, region, f'not found: {cand}'

def resolve_plate_json_from_fits_legacy(tile_dir: Path, legacy_roots: list[Path]):
    """
    Legacy fallback: use FITS basename patterns against legacy roots.
    """
    raw = tile_dir / 'raw'
    meta = read_title_sidecar(raw)
    fits_name = (meta.get('FITS', '') or '').strip()
    if not fits_name:
        return None, 'missing FITS in sidecar'
    base = Path(fits_name).name
    no_size = SIZE_TAG_RE.sub('', base)
    cand_names = []
    if no_size.endswith('.fits'):
        cand_names += [f"{no_size}.header.json",
                       f"{no_size[:-5]}.header.json",
                       f"{no_size}.fits.header.json"]
    else:
        cand_names += [f"{no_size}.header.json", f"{no_size}.fits.header.json"]
    cand_names.append(f"{base}.header.json")
    for root in legacy_roots:
        for hn in cand_names:
            cand = root / hn
            if cand.exists():
                return cand, ''
        # relax to wildcard
        nb = no_size[:-5] if no_size.endswith('.fits') else no_size
        matches = list(root.glob(f"{nb}*.header.json"))
        if len(matches) == 1:
            return matches[0], ''
    # final: check in raw
    for hn in cand_names:
        cand2 = raw / hn
        if cand2.exists():
            return cand2, ''
    return None, f'not found: tried patterns in {",".join(str(r) for r in legacy_roots)} and raw/'

# -------------------------- tile WCS (from tile raw) ----------------------
def load_tile_wcs_from_json(tile_dir: Path):
    raw = tile_dir / 'raw'
    cands = sorted(list(raw.glob('*.header.json')))
    if not cands:
        return None, None, None, '', 'no .header.json under raw/'
    pref = [p for p in cands if '30arcmin' in p.name]
    tj = (pref[0] if pref else cands[0])
    data, err = robust_json_load(tj)
    if data is None:
        return None, None, None, tj.name, f'cannot parse JSON: {err}'
    hdr = pick_header_dict(data)
    try:
        H = Header()
        H['NAXIS'] = 2
        H['NAXIS1'] = int((hdr.get('NAXIS1', 0) or 0))
        H['NAXIS2'] = int((hdr.get('NAXIS2', 0) or 0))
        for k in ('CRPIX1', 'CRPIX2', 'CRVAL1', 'CRVAL2'):
            v = hdr.get(k, None)
            if v is not None:
                H[k] = float(v)
        cd_keys = ('CD1_1', 'CD1_2', 'CD2_1', 'CD2_2')
        if all((hdr.get(k, None) is not None) for k in cd_keys):
            for k in cd_keys:
                H[k] = float(hdr[k])
        else:
            v1 = hdr.get('CDELT1', None); v2 = hdr.get('CDELT2', None); vr = hdr.get('CROTA2', None)
            if v1 is not None: H['CDELT1'] = float(v1)
            if v2 is not None: H['CDELT2'] = float(v2)
            if vr is not None: H['CROTA2'] = float(vr)
        H['CTYPE1'] = hdr.get('CTYPE1', 'RA---TAN')
        H['CTYPE2'] = hdr.get('CTYPE2', 'DEC--TAN')
        w = WCS(H)
        return w, H['NAXIS1'], H['NAXIS2'], tj.name, ''
    except Exception as e:
        return None, int(hdr.get('NAXIS1', 0) or 0), int(hdr.get('NAXIS2', 0) or 0), tj.name, f'WCS build failed: {e}'

# --------------------------- plate core (no WCS) --------------------------
def load_plate_core_from_json(path: Path):
    data, err = robust_json_load(path)
    if data is None:
        return None, 'cannot parse plate JSON: ' + str(err)
    hdr = pick_header_dict(data)
    try:
        nax1 = int(hdr.get('NAXIS1', 0) or 0)
        nax2 = int(hdr.get('NAXIS2', 0) or 0)
        pl_ra = float(hdr['PLATERA'])
        pl_de = float(hdr['PLATEDEC'])
        pltscale = float(hdr['PLTSCALE'])  # arcsec/mm
        xp_um = float(hdr['XPIXELSZ'])     # microns
        yp_um = float(hdr['YPIXELSZ'])
    except Exception:
        return None, 'missing required plate keys (NAXIS*, PLATERA/DEC, PLTSCALE, X/YPIXELSZ)'
    cx = nax1 / 2.0; cy = nax2 / 2.0
    as_per_px_x = pltscale * (xp_um / 1000.0)
    as_per_px_y = pltscale * (yp_um / 1000.0)
    return {
        'nax1': nax1, 'nax2': nax2,
        'center_ra': pl_ra, 'center_dec': pl_de,
        'cx': cx, 'cy': cy,
        'as_per_px_x': as_per_px_x, 'as_per_px_y': as_per_px_y
    }, ''

# ------------------------------- geometry ---------------------------------
def radec_to_plate_pixels_gnomonic(ra_deg: np.ndarray, dec_deg: np.ndarray, plate: dict) -> np.ndarray:
    ra0 = math.radians(plate['center_ra'])
    de0 = math.radians(plate['center_dec'])
    as_px_x = plate['as_per_px_x']; as_px_y = plate['as_per_px_y']
    cx, cy = plate['cx'], plate['cy']
    ra = np.radians(ra_deg); de = np.radians(dec_deg)
    dra = (ra - ra0 + np.pi) % (2*np.pi) - np.pi
    sin_de = np.sin(de); cos_de = np.cos(de)
    sin_de0 = math.sin(de0); cos_de0 = math.cos(de0)
    cos_dra = np.cos(dra); sin_dra = np.sin(dra)
    denom = (sin_de0*sin_de + cos_de0*cos_de*cos_dra)
    denom = np.where(np.abs(denom) < 1e-12, np.nan, denom)
    xi  = (cos_de * sin_dra) / denom
    eta = (cos_de0*sin_de - sin_de0*cos_de*cos_dra) / denom
    xi_as, eta_as = xi*RAD2AS, eta*RAD2AS
    x = cx + (xi_as / as_px_x)
    y = cy + (eta_as / as_px_y)
    return np.stack([x, y], axis=1)

def enforce_east_left_orientation(plate: dict):
    """Return flags dict {'flip_x':bool,'flip_y':bool} ensuring East-left, North-up."""
    eps_deg = 5.0 / 3600.0
    ra0 = plate['center_ra']; dec0 = plate['center_dec']
    xy_east  = radec_to_plate_pixels_gnomonic(np.array([ra0 + eps_deg]), np.array([dec0]), plate)[0]
    xy_north = radec_to_plate_pixels_gnomonic(np.array([ra0]), np.array([dec0 + eps_deg]), plate)[0]
    cx, cy = plate['cx'], plate['cy']
    flip_x = (xy_east[0]  > cx)  # East should move left
    flip_y = (xy_north[1] < cy)  # North should move up
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

# ------------------------------- overlays ---------------------------------
def render_overlay_png(out_png: Path, plate_nx: int, plate_ny: int, tile_poly_xy: np.ndarray,
                       title: str, subtitle: str, px_margin: float, as_margin: float):
    fig, ax = plt.subplots(figsize=(7.5, 7.5))
    ax.set_title(title, fontsize=11)
    if subtitle:
        ax.text(0.02, 0.98, subtitle, transform=ax.transAxes, va='top', ha='left',
                fontsize=9, color='gray')
    ax.plot([1, plate_nx, plate_nx, 1, 1], [1, 1, plate_ny, plate_ny, 1], 'k-', lw=1.0, alpha=0.8)
    if tile_poly_xy is not None and len(tile_poly_xy) > 0 and not np.any(np.isnan(tile_poly_xy)):
        ax.plot(tile_poly_xy[:,0], tile_poly_xy[:,1], '-', color='tab:blue', lw=1.5, alpha=0.9, label='tile polygon')
        ax.legend(loc='lower right', fontsize=8, frameon=False)
    ax.set_xlim(0, plate_nx+1); ax.set_ylim(0, plate_ny+1)
    ax.set_aspect('equal', adjustable='box'); ax.grid(ls=':', alpha=0.35)
    msg = f"min_edge: {px_margin:.1f} px ({as_margin:.1f} arcsec)"
    ax.text(0.02, 0.02, msg, transform=ax.transAxes, fontsize=9, ha='left', va='bottom')
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout(); fig.savefig(out_png, dpi=140); plt.close(fig)

# --------------------------------- main -----------------------------------
def main():
    ap = argparse.ArgumentParser(
        description='Tile vs Plate Edge (v4.4): REGION-first resolver + repo headers'
    )
    ap.add_argument('--tiles-root', default='./data/tiles')
    ap.add_argument('--headers-dir', default='metadata/plates/headers',
                    help='Repo headers root (default: metadata/plates/headers)')
    ap.add_argument('--legacy-roots', default='',
                    help='Optional comma-separated legacy header roots (disabled by default)')
    ap.add_argument('--out-csv', default='./data/metadata/tile_plate_edge_report.csv')
    ap.add_argument('--threshold-px', type=float, default=200.0)
    ap.add_argument('--threshold-frac', type=float, default=0.02)
    ap.add_argument('--threshold-arcsec', type=float, default=60.0)
    ap.add_argument('--plot', action='store_true')
    ap.add_argument('--plot-all', action='store_true')
    ap.add_argument('--plot-dir', default='./data/metadata/edge_plots_by_sky')
    ap.add_argument('--fast-square', action='store_true')
    ap.add_argument('--debug', action='store_true')
    args = ap.parse_args()

    tiles_root = Path(args.tiles_root)
    repo_headers = Path(args.headers_dir)
    legacy_roots = [Path(s.strip()) for s in args.legacy_roots.split(',') if s.strip()]
    out_csv = Path(args.out_csv)

    rows_out = []
    for td in iter_tile_dirs(tiles_root):
        tile_id = td.name
        tile_cent = parse_tile_id(tile_id)

        # Tile WCS (from raw JSON)
        tile_wcs, tnx, tny, tile_json_name, tile_err = load_tile_wcs_from_json(td)

        # Plate header (REGION-first)
        plate_json, plate_id, p_err = resolve_repo_plate_json_by_region(td, repo_headers)
        if plate_json is None and legacy_roots:
            # try legacy fallback only if explicitly provided
            legacy_json, legacy_err = resolve_plate_json_from_fits_legacy(td, legacy_roots)
            plate_json = legacy_json
            p_err = legacy_err

        if plate_json is None:
            rows_out.append({
                'tile_id': tile_id, 'plate_id': plate_id or '',
                'plate_filename': '', 'plate_nx': '', 'plate_ny': '',
                'tile_center_ra_deg': f"{tile_cent[0]:.6f}" if tile_cent else '',
                'tile_center_dec_deg': f"{tile_cent[1]:.6f}" if tile_cent else '',
                'plate_center_ra_deg': '', 'plate_center_dec_deg': '', 'sep_arcmin': '',
                'min_edge_dist_px': '', 'min_edge_dist_arcsec': '',
                'class_px': 'off_plate', 'class_arcsec': 'off_plate',
                'notes': p_err
            })
            continue

        plate_info, core_err = load_plate_core_from_json(plate_json)
        if core_err:
            rows_out.append({
                'tile_id': tile_id, 'plate_id': plate_id or '',
                'plate_filename': plate_json.name,
                'tile_center_ra_deg': f"{tile_cent[0]:.6f}" if tile_cent else '',
                'tile_center_dec_deg': f"{tile_cent[1]:.6f}" if tile_cent else '',
                'plate_center_ra_deg': '', 'plate_center_dec_deg': '', 'sep_arcmin': '',
                'plate_nx': '', 'plate_ny': '',
                'min_edge_dist_px': '', 'min_edge_dist_arcsec': '',
                'class_px': 'off_plate', 'class_arcsec': 'off_plate',
                'notes': core_err
            })
            continue

        orient = enforce_east_left_orientation(plate_info)

        # tile center via WCS if missing in name
        if tile_cent is None and tile_wcs is not None and tnx and tny:
            cx, cy = tnx/2.0, tny/2.0
            ra_dec = tile_wcs.all_pix2world(np.array([[cx, cy]]), 1)[0]
            tile_cent = (float(ra_dec[0]), float(ra_dec[1]))

        # separation on sky
        if tile_cent is not None:
            c_tile  = SkyCoord(tile_cent[0]*u.deg, tile_cent[1]*u.deg, frame='icrs')
            c_plate = SkyCoord(plate_info['center_ra']*u.deg, plate_info['center_dec']*u.deg, frame='icrs')
            sep_arcmin = float(c_tile.separation(c_plate).arcmin)
        else:
            sep_arcmin = float('nan')

        if tile_wcs is None or not tnx or not tny:
            rows_out.append({
                'tile_id': tile_id, 'plate_id': plate_id or '',
                'plate_filename': plate_json.name,
                'plate_nx': plate_info['nax1'], 'plate_ny': plate_info['nax2'],
                'tile_center_ra_deg': f"{tile_cent[0]:.6f}" if tile_cent else '',
                'tile_center_dec_deg': f"{tile_cent[1]:.6f}" if tile_cent else '',
                'plate_center_ra_deg': f"{plate_info['center_ra']:.6f}",
                'plate_center_dec_deg': f"{plate_info['center_dec']:.6f}",
                'sep_arcmin': f"{sep_arcmin:.3f}" if not math.isnan(sep_arcmin) else '',
                'min_edge_dist_px': '', 'min_edge_dist_arcsec': '',
                'class_px': 'off_plate', 'class_arcsec': 'off_plate',
                'notes': 'invalid tile WCS'
            })
            continue

        # sample 8 points (4 corners + 4 mids)
        corners = np.array([[1,1],[tnx,1],[tnx,tny],[1,tny],[1,1]], dtype=float)
        mids    = np.array([[tnx/2,1],[tnx,tny/2],[tnx/2,tny],[1,tny/2]], dtype=float)
        samples = np.vstack([corners, mids])
        world   = tile_wcs.all_pix2world(samples, 1)
        plate_xy = radec_to_plate_pixels_gnomonic(world[:,0], world[:,1], plate_info)

        # orientation enforcement
        if orient['flip_x']:
            plate_xy[:,0] = 2.0*plate_info['cx'] - plate_xy[:,0]
        if orient['flip_y']:
            plate_xy[:,1] = 2.0*plate_info['cy'] - plate_xy[:,1]

        px_margin    = min_edge_distance_px(plate_xy, plate_info['nax1'], plate_info['nax2'])
        as_per_px    = 0.5*(plate_info['as_per_px_x'] + plate_info['as_per_px_y'])
        as_margin    = px_margin * as_per_px if not math.isnan(px_margin) else float('nan')

        # classify
        def classify_px(mind_px: float, nx: int, ny: int, th_px: float, th_frac: float) -> str:
            if math.isnan(mind_px): return 'off_plate'
            if mind_px < 0:         return 'edge_touch'
            thresh = max(float(th_px), float(th_frac) * float(min(nx, ny)))
            return 'core' if mind_px >= thresh else 'near_edge'

        def classify_arcsec(mind_as: float, th_as: float) -> str:
            if mind_as is None or math.isnan(mind_as): return 'off_plate'
            if mind_as < 0: return 'edge_touch'
            return 'core' if mind_as >= th_as else 'near_edge'

        c_px = classify_px(px_margin, plate_info['nax1'], plate_info['nax2'],
                           args.threshold_px, args.threshold_frac)
        c_as = classify_arcsec(as_margin, args.threshold_arcsec)

        rows_out.append({
            'tile_id': tile_id, 'plate_id': plate_id or '',
            'plate_filename': plate_json.name,
            'plate_nx': plate_info['nax1'], 'plate_ny': plate_info['nax2'],
            'tile_center_ra_deg': f"{tile_cent[0]:.6f}" if tile_cent else '',
            'tile_center_dec_deg': f"{tile_cent[1]:.6f}" if tile_cent else '',
            'plate_center_ra_deg': f"{plate_info['center_ra']:.6f}",
            'plate_center_dec_deg': f"{plate_info['center_dec']:.6f}",
            'sep_arcmin': f"{sep_arcmin:.3f}" if not math.isnan(sep_arcmin) else '',
            'min_edge_dist_px': f"{px_margin:.2f}" if not math.isnan(px_margin) else '',
            'min_edge_dist_arcsec': f"{as_margin:.1f}" if not math.isnan(as_margin) else '',
            'class_px': c_px, 'class_arcsec': c_as,
            'notes': f"orient flip_x={orient['flip_x']} flip_y={orient['flip_y']}"
        })

        # overlays
        if args.plot and (args.plot_all or c_px in ('near_edge','edge_touch') or c_as in ('near_edge','edge_touch')):
            poly_world = world[:5,:]
            poly_plate = radec_to_plate_pixels_gnomonic(poly_world[:,0], poly_world[:,1], plate_info)
            if orient['flip_x']: poly_plate[:,0] = 2.0*plate_info['cx'] - poly_plate[:,0]
            if orient['flip_y']: poly_plate[:,1] = 2.0*plate_info['cy'] - poly_plate[:,1]
            rb, db = sky_bins(tile_cent[0], tile_cent[1]) if tile_cent else (0,0)
            out_png = Path(args.plot_dir) / f"ra_bin={rb}" / f"dec_bin={db}" / f"{tile_id}.png"
            title = f"{tile_id} on {plate_json.stem.replace('.header','')}"
            subtitle = (f"plate: {plate_json.name} | "
                        f"min_edge: {px_margin:.1f}px / {as_margin:.1f}\" | "
                        f"sep: {sep_arcmin:.2f}' | "
                        f"orient: x={'L' if orient['flip_x'] else 'R'} "
                        f"y={'U' if not orient['flip_y'] else 'D'}")
            render_overlay_png(out_png, plate_info['nax1'], plate_info['nax2'], poly_plate,
                               title, subtitle,
                               px_margin if not math.isnan(px_margin) else float('nan'),
                               as_margin if not math.isnan(as_margin) else float('nan'))

    # write CSV
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open('w', newline='', encoding='utf-8') as f:
        fieldnames = [
            'tile_id','plate_id','plate_filename','plate_nx','plate_ny',
            'tile_center_ra_deg','tile_center_dec_deg',
            'plate_center_ra_deg','plate_center_dec_deg',
            'sep_arcmin','min_edge_dist_px','min_edge_dist_arcsec',
            'class_px','class_arcsec','notes'
        ]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader(); w.writerows(rows_out)
    print(f"[OK] wrote {out_csv} rows={len(rows_out)}")

if __name__ == '__main__':
    main()