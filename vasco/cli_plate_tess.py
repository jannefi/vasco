from __future__ import annotations
import argparse, csv, json, math, random
from pathlib import Path
from typing import List, Dict, Any, Tuple

# --- geometry helpers ---
def _deg_to_arcmin(x: float) -> float:
    return x*60.0

def _arcmin_to_deg(x: float) -> float:
    return x/60.0

def _tile_grid_centers(ra0: float, dec0: float, size_deg: float, tile_arcmin: int, overlap_arcmin: int) -> List[Tuple[float,float]]:
    """
    Build a simple RA/Dec grid over a square footprint centered at (ra0, dec0).
    Uses a naive small-angle approximation suitable for ~few-degree scales.
    """
    # Effective step in arcmin (tile size minus overlap)
    step_arcmin = max(1, tile_arcmin - overlap_arcmin)
    step_deg = _arcmin_to_deg(step_arcmin)
    half = size_deg/2.0
    # Number of steps from center in each direction
    nx = int(math.ceil((size_deg)/step_deg))
    ny = int(math.ceil((size_deg)/step_deg))
    ras: List[float] = []
    decs: List[float] = []
    for iy in range(-ny//2, ny//2 + 1):
        dec = dec0 + iy*step_deg
        # cos(dec) correction for RA separation
        cosd = max(0.1, math.cos(math.radians(dec0)))
        ra_step = step_deg / cosd
        for ix in range(-nx//2, nx//2 + 1):
            ra = ra0 + ix*ra_step
            # clip roughly to square footprint
            if abs(ra - ra0) <= half/cosd and abs(dec - dec0) <= half:
                ras.append(ra)
                decs.append(dec)
    return list(zip(ras,decs))

# --- sampling ---
def _sample_positions(positions: List[Tuple[float,float]], fraction: float, seed: int=42) -> List[Tuple[float,float]]:
    if fraction >= 0.999:
        return positions
    rnd = random.Random(seed)
    k = max(1, int(round(len(positions)*fraction)))
    return rnd.sample(positions, k)

# --- main API ---
def build_tiles(plates_json: Path, out_csv: Path, *, default_fraction=0.2, seed: int=42) -> int:
    pdata = json.loads(Path(plates_json).read_text(encoding='utf-8'))
    rows: List[Dict[str,Any]] = []
    for p in pdata:
        plate_id = p.get('plate_id','unknown')
        ra0 = float(p['center_ra_deg'])
        dec0 = float(p['center_dec_deg'])
        size_deg = float(p.get('footprint_deg', 6.5))
        tile_arcmin = int(p.get('tile_size_arcmin', 60))
        overlap_arcmin = int(p.get('tile_overlap_arcmin', 2))
        cov = str(p.get('coverage_mode','sample')).lower()
        frac = float(p.get('sample_fraction', default_fraction))
        # Build grid
        positions = _tile_grid_centers(ra0, dec0, size_deg, tile_arcmin, overlap_arcmin)
        if cov == 'sample':
            positions = _sample_positions(positions, frac, seed=seed)
        # Emit rows
        for (ra,dec) in positions:
            rows.append({
                'plate_id': plate_id,
                'ra_deg': f"{ra:.6f}",
                'dec_deg': f"{dec:.6f}",
                'size_arcmin': tile_arcmin,
                'overlap_arcmin': overlap_arcmin
            })
    # Write CSV
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['plate_id','ra_deg','dec_deg','size_arcmin','overlap_arcmin'])
        w.writeheader()
        w.writerows(rows)
    return len(rows)


def make_runner_script(tiles_csv: Path, script_path: Path, *, retry_after: int=4):
    """
    Emit a shell script that loops over tiles and calls your existing run.sh in --one mode.
    """
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


# --- CLI ---
def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog='vasco.cli_plate_tess', description='Plate-aware tessellation (sample or full cover) for DSS/POSS cutouts')
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
