from __future__ import annotations
import argparse, csv, json, math, random
from pathlib import Path
from typing import List, Dict, Any, Tuple

from .utils.coords import parse_ra, parse_dec

# --- geometry helpers ---
def _arcmin_to_deg(x: float) -> float:
    return x/60.0

def _tile_grid_centers(ra0_deg: float, dec0_deg: float, size_deg: float, tile_arcmin: int, overlap_arcmin: int) -> List[Tuple[float,float]]:
    step_arcmin = max(1, int(tile_arcmin) - int(overlap_arcmin))
    step_deg = _arcmin_to_deg(step_arcmin)
    half = float(size_deg)/2.0
    nx = int(math.ceil((float(size_deg))/step_deg))
    ny = int(math.ceil((float(size_deg))/step_deg))
    ras: List[float] = []
    decs: List[float] = []
    for iy in range(-ny//2, ny//2 + 1):
        dec = dec0_deg + iy*step_deg
        cosd = max(0.1, math.cos(math.radians(dec0_deg)))
        ra_step = step_deg / cosd
        for ix in range(-nx//2, nx//2 + 1):
            ra = ra0_deg + ix*ra_step
            if abs(ra - ra0_deg) <= half/cosd and abs(dec - dec0_deg) <= half:
                ras.append(ra)
                decs.append(dec)
    return list(zip(ras, decs))

# --- sampling ---
def _sample_positions(positions: List[Tuple[float,float]], fraction: float, seed: int=42) -> List[Tuple[float,float]]:
    if fraction >= 0.999:
        return positions
    rnd = random.Random(seed)
    k = max(1, int(round(len(positions)*fraction)))
    return rnd.sample(positions, k)

# --- main API ---
def _first_present(d: Dict[str, Any], keys: List[str], default=None):
    for k in keys:
        if k in d and d[k] not in (None, ''):
            return d[k]
    return default

def build_tiles(plates_json: Path, out_csv: Path, *, default_fraction=0.2, seed: int=42) -> int:
    pdata = json.loads(Path(plates_json).read_text(encoding='utf-8'))
    rows: List[Dict[str,Any]] = []
    for p in pdata:
        plate_id = p.get('plate_id','unknown')
        ra_val  = _first_present(p, ['center_ra_deg','center_ra','ra','center_ra_hms'])
        dec_val = _first_present(p, ['center_dec_deg','center_dec','dec','center_dec_dms'])
        if ra_val is None or dec_val is None:
            raise ValueError(f"Missing center RA/Dec in plate entry: {plate_id}")
        ra0 = parse_ra(ra_val)
        dec0 = parse_dec(dec_val)
        size_deg = float(p.get('footprint_deg', 6.5))
        tile_arcmin = int(p.get('tile_size_arcmin', 60))
        overlap_arcmin = int(p.get('tile_overlap_arcmin', 2))
        cov = str(p.get('coverage_mode','sample')).lower()
        frac = float(p.get('sample_fraction', default_fraction))
        positions = _tile_grid_centers(ra0, dec0, size_deg, tile_arcmin, overlap_arcmin)
        if cov == 'sample':
            positions = _sample_positions(positions, frac, seed=seed)
        for (ra,dec) in positions:
            rows.append({
                'plate_id': plate_id,
                'ra_deg': f"{ra:.6f}",
                'dec_deg': f"{dec:.6f}",
                'size_arcmin': tile_arcmin,
                'overlap_arcmin': overlap_arcmin
            })
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['plate_id','ra_deg','dec_deg','size_arcmin','overlap_arcmin'])
        w.writeheader()
        w.writerows(rows)
    return len(rows)


def make_runner_script(tiles_csv: Path, script_path: Path, *, retry_after: int=4):
    script = [
        '#!/usr/bin/env bash',
        'set -euo pipefail',
        f'TILES_CSV="{tiles_csv}"',
        'echo "Running tiles from $TILES_CSV"',
        'tail -n +2 "$TILES_CSV" | while IFS="," read -r plate ra dec size overlap; do',
        '  echo "==> $plate  RA=$ra  Dec=$dec  size=$size arcmin"',
        f'  ./run.sh --one --ra "$ra" --dec "$dec" --size-arcmin "$size" --retry-after {retry_after}',
        'done'
    ]
    script_path.write_text('\n'.join(script)+'\n', encoding='utf-8')
    script_path.chmod(0o755)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog='vasco.cli_plate_tess', description='Plate-aware tessellation (sample or full cover) for DSS/POSS cutouts; accepts sexagesimal or decimal plate centers.')
    sub = p.add_subparsers(dest='cmd')

    b = sub.add_parser('build', help='Build a CSV of tile centers from a plates JSON')
    b.add_argument('--plates-json', required=True)
    b.add_argument('--out-csv', default='plate_tiles.csv')
    b.add_argument('--seed', type=int, default=42)
    b.add_argument('--default-fraction', type=float, default=0.2)
    b.add_argument('--emit-runner', action='store_true')
    b.add_argument('--runner-path', default='run_plate_tiles.sh')

    args = p.parse_args(argv)
    if args.cmd == 'build':
        n = build_tiles(Path(args.plates_json), Path(args.out_csv), default_fraction=float(args.default_fraction), seed=int(args.seed))
        print(f"Wrote {n} tiles -> {args.out_csv}")
        if args.emit_runner:
            make_runner_script(Path(args.out_csv), Path(args.runner_path))
            print(f"Runner script: {args.runner_path}")
        return 0

    p.print_help()
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
