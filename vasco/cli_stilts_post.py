
"""Standalone STILTS post-xmatch runner.

Usage:
  python -m vasco.cli_stilts_post                 # uses latest data/runs/run-*
  python -m vasco.cli_stilts_post --run <path>    # explicit run dir
  python -m vasco.cli_stilts_post --radius 2.0    # match radius (arcsec)

Assumptions (degrees):
  SExtractor CSV : ALPHA_J2000, DELTA_J2000
  Gaia/PS1  CSV  : ra, dec

This module avoids touching cli_pipeline.py and can be kept even if CLI flags change.
"""
from __future__ import annotations
from pathlib import Path
import argparse
from vasco.mnras.xmatch_stilts import (
    xmatch_sextractor_with_gaia,
    xmatch_sextractor_with_ps1,
)


def _latest_run() -> Path:
    runs = Path('data/runs')
    cands = sorted((d for d in runs.glob('run-*') if d.is_dir()),
                   key=lambda p: p.stat().st_mtime, reverse=True)
    if not cands:
        raise SystemExit('[ERROR] No runs found under data/runs')
    return cands[0]


def main(argv=None):
    ap = argparse.ArgumentParser(description='STILTS post-xmatch for latest/selected run')
    ap.add_argument('--run', type=str, default=None, help='Run directory (default: latest under data/runs)')
    ap.add_argument('--radius', type=float, default=2.0, help='Match radius in arcsec (default: 2.0)')
    ap.add_argument('--join', type=str, default='1and2', help="STILTS join type: 1and2|1or2|... (default: 1and2)")
    args = ap.parse_args(argv)

    run_dir = Path(args.run) if args.run else _latest_run()
    sex_csv  = run_dir / 'catalogs' / 'sextractor_pass2.csv'
    gaia_csv = run_dir / 'catalogs' / 'gaia_neighbourhood.csv'
    ps1_csv  = run_dir / 'catalogs' / 'ps1_neighbourhood.csv'

    for p in (sex_csv, gaia_csv, ps1_csv):
        if not p.exists():
            raise SystemExit(f'[ERROR] Missing expected catalog: {p}')

    xdir = run_dir / 'xmatch'
    xdir.mkdir(parents=True, exist_ok=True)

    x_gaia = xdir / 'sex_gaia_xmatch.csv'
    x_ps1  = xdir / 'sex_ps1_xmatch.csv'

    xmatch_sextractor_with_gaia(sex_csv, gaia_csv, x_gaia, radius_arcsec=args.radius, join_type=args.join)
    xmatch_sextractor_with_ps1(sex_csv, ps1_csv,  x_ps1,  radius_arcsec=args.radius, join_type=args.join)

    print('[OK] STILTS post-xmatch on:', run_dir)
    print('     Gaia ->', x_gaia)
    print('     PS1  ->', x_ps1)


if __name__ == '__main__':
    raise SystemExit(main())
