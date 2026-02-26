#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import os
import time
from pathlib import Path
from typing import List, Tuple, Dict, Any

import pandas as pd


def _match_nearest_sky(ra_deg, dec_deg, cra_deg, cdec_deg):
    """Return (idx, sep_arcsec) for each point to nearest catalog point.

    Uses astropy if available, else sklearn BallTree haversine.
    """
    try:
        from astropy.coordinates import SkyCoord
        import astropy.units as u
        c1 = SkyCoord(ra=ra_deg.values * u.deg, dec=dec_deg.values * u.deg, frame='icrs')
        c2 = SkyCoord(ra=cra_deg.values * u.deg, dec=cdec_deg.values * u.deg, frame='icrs')
        idx, sep2d, _ = c1.match_to_catalog_sky(c2)
        return idx.astype('int64'), sep2d.arcsec
    except Exception:
        # Fallback: BallTree in radians (haversine)
        try:
            from sklearn.neighbors import BallTree
        except Exception as e:
            raise RuntimeError(
                "Need astropy or scikit-learn installed for spherical matching. "
                f"astropy failed and sklearn BallTree unavailable: {e}"
            )
        import numpy as np
        # BallTree expects lat,lon in radians for haversine
        lat = np.deg2rad(dec_deg.values)
        lon = np.deg2rad(ra_deg.values)
        clat = np.deg2rad(cdec_deg.values)
        clon = np.deg2rad(cra_deg.values)
        X = np.column_stack([lat, lon])
        C = np.column_stack([clat, clon])
        bt = BallTree(C, metric='haversine')
        dist, ind = bt.query(X, k=1)
        # dist is in radians; convert to arcsec
        sep_arcsec = dist[:, 0] * (180.0 / np.pi) * 3600.0
        return ind[:, 0].astype('int64'), sep_arcsec


def _atomic_write(path: Path, write_fn):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    write_fn(tmp)
    tmp.replace(path)


def _read_det(tile_dir: Path) -> pd.DataFrame:
    p = tile_dir / 'final_catalog_wcsfix.csv'
    if not p.exists():
        raise FileNotFoundError(f"missing {p}")
    df = pd.read_csv(p, engine='python', on_bad_lines='skip')
    if 'RA_corr' not in df.columns or 'Dec_corr' not in df.columns:
        raise ValueError(f"{p} missing RA_corr/Dec_corr")
    df['RA_corr'] = pd.to_numeric(df['RA_corr'], errors='coerce')
    df['Dec_corr'] = pd.to_numeric(df['Dec_corr'], errors='coerce')
    df = df.dropna(subset=['RA_corr','Dec_corr']).reset_index(drop=True)
    return df


def _read_gaia(tile_dir: Path) -> pd.DataFrame:
    p = tile_dir / 'catalogs' / 'gaia_neighbourhood.csv'
    if not p.exists():
        raise FileNotFoundError(f"missing {p}")
    df = pd.read_csv(p, engine='python', on_bad_lines='skip')
    # fetcher normalizes RA_ICRS/DE_ICRS -> ra/dec
    for col in ['ra','dec']:
        if col not in df.columns:
            raise ValueError(f"{p} missing '{col}'")
    df['ra'] = pd.to_numeric(df['ra'], errors='coerce')
    df['dec'] = pd.to_numeric(df['dec'], errors='coerce')
    df = df.dropna(subset=['ra','dec']).reset_index(drop=True)
    return df


def _write_pairs_csv(path: Path, out_rows: List[Dict[str, Any]]):
    if not out_rows:
        def _w(tmp):
            tmp.write_text('')
        _atomic_write(path, _w)
        return
    fieldnames = list(out_rows[0].keys())
    def _w(tmp):
        with tmp.open('w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in out_rows:
                w.writerow(r)
    _atomic_write(path, _w)


def run_one_tile(tile_dir: Path, radii_arcsec: List[float], overwrite: bool=False) -> Dict[str, Any]:
    tile_id = tile_dir.name
    out_dir = tile_dir / 'xmatch'
    out_dir.mkdir(parents=True, exist_ok=True)

    # We always write the NN file; if exists and overwrite False, we reuse it.
    nn_path = out_dir / 'gaia_xmatch_local_wcsfix_nearest.csv'

    if nn_path.exists() and not overwrite:
        nn = pd.read_csv(nn_path)
    else:
        det = _read_det(tile_dir)
        gaia = _read_gaia(tile_dir)

        idx, sep_arcsec = _match_nearest_sky(det['RA_corr'], det['Dec_corr'], gaia['ra'], gaia['dec'])

        nn = pd.DataFrame({
            'det_idx': det.index.astype('int64'),
            'gaia_idx': idx,
            'sep_arcsec': sep_arcsec,
        })
        # Optional IDs
        for col in ['NUMBER','RA_corr','Dec_corr']:
            if col in det.columns:
                nn[col] = det[col].values
        # Add a few Gaia columns if present
        for col in ['Gmag','BPmag','RPmag','Plx','pmRA','pmDE','_r']:
            if col in gaia.columns:
                nn[col] = gaia[col].iloc[idx].values
        nn['gaia_ra'] = gaia['ra'].iloc[idx].values
        nn['gaia_dec'] = gaia['dec'].iloc[idx].values

        def _w(tmp):
            nn.to_csv(tmp, index=False)
        _atomic_write(nn_path, _w)

    summary = {
        'tile_id': tile_id,
        'det_rows': int(len(nn)),
        'nn_path': str(nn_path),
        'radii_arcsec': radii_arcsec,
        'counts': {},
        'pairs_files': {},
    }

    # For each radius, write a pairs file (subset) and counts.
    for r in radii_arcsec:
        r = float(r)
        mask = pd.to_numeric(nn['sep_arcsec'], errors='coerce').le(r).fillna(False)
        matched = nn.loc[mask].copy()
        summary['counts'][str(r)] = {
            'matched': int(mask.sum()),
            'unmatched': int(len(nn) - mask.sum()),
            'match_rate': float(mask.mean()) if len(nn) else 0.0,
        }
        out_path = out_dir / f'gaia_xmatch_local_wcsfix_within{int(r)}arcsec.csv'
        if out_path.exists() and not overwrite:
            summary['pairs_files'][str(r)] = str(out_path)
            continue
        def _w(tmp):
            matched.to_csv(tmp, index=False)
        _atomic_write(out_path, _w)
        summary['pairs_files'][str(r)] = str(out_path)

    # Write per-tile summary
    sum_path = out_dir / 'gaia_xmatch_local_wcsfix_summary.json'
    def _w(tmp):
        tmp.write_text(json.dumps(summary, indent=2), encoding='utf-8')
    _atomic_write(sum_path, _w)
    summary['summary_path'] = str(sum_path)
    return summary


def iter_tiles(tile_root: Path):
    # pilot root is flat with symlinks: tile-*
    for p in sorted(tile_root.glob('tile-*')):
        if p.is_dir() or p.is_symlink():
            yield p


def main():
    import argparse

    ap = argparse.ArgumentParser(description='Local Gaia xmatch using RA_corr/Dec_corr vs cached gaia_neighbourhood.csv; write within2" and within5" outputs.')
    ap.add_argument('--tiles-root', default='./work/wcsfix_pilot_tiles', help='Root containing tile-* directories (default: ./work/wcsfix_pilot_tiles)')
    ap.add_argument('--radii-arcsec', nargs='+', type=float, default=[2.0, 5.0], help='Radii to evaluate (default: 2 5)')
    ap.add_argument('--overwrite', action='store_true', help='Overwrite outputs if they exist')
    ap.add_argument('--out-summary', default='./work/wcsfix_pilot_tiles/GAIA_XMATCH_LOCAL_WCSFIX_COMPARISON.json', help='Write an aggregate summary JSON here')
    args = ap.parse_args()

    root = Path(args.tiles_root)
    radii = [float(x) for x in args.radii_arcsec]

    started = time.strftime('%Y-%m-%d %H:%M:%S')
    agg = {
        'started_at': started,
        'tiles_root': str(root),
        'radii_arcsec': radii,
        'tiles': [],
        'aggregate': {str(r): {'det_rows': 0, 'matched': 0} for r in radii},
    }

    for tile_dir in iter_tiles(root):
        try:
            s = run_one_tile(tile_dir, radii_arcsec=radii, overwrite=args.overwrite)
            agg['tiles'].append(s)
            for r in radii:
                rr = str(r)
                agg['aggregate'][rr]['det_rows'] += int(s['det_rows'])
                agg['aggregate'][rr]['matched'] += int(s['counts'][rr]['matched'])
            print(f"[OK] {s['tile_id']} det={s['det_rows']} matched2={s['counts'][str(radii[0])]['matched']} matched5={s['counts'][str(radii[-1])]['matched']}")
        except Exception as e:
            print(f"[FAIL] {tile_dir.name}: {e}")

    # Add match rates
    for r in radii:
        rr = str(r)
        det = agg['aggregate'][rr]['det_rows']
        m = agg['aggregate'][rr]['matched']
        agg['aggregate'][rr]['match_rate'] = float(m) / float(det) if det else 0.0

    out = Path(args.out_summary)
    def _w(tmp):
        tmp.write_text(json.dumps(agg, indent=2), encoding='utf-8')
    _atomic_write(out, _w)
    print(f"[DONE] wrote {out}")


if __name__ == '__main__':
    main()
