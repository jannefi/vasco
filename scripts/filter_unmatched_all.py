
#!/usr/bin/env python3
"""
VASCO — Post-step unmatched/union generator (FAST v3, CDS + local)

This version aligns with your STILTS build behavior:
  • Uses non-interactive `values1/values2` for tmatch2 (exact ID join on NUMBER)
  • Drops `ifmt=` arguments (lets STILTS auto-detect CSV)
  • Sets `progress=log` for visibility
  • Keeps the fast path (project detection IDs → union → Python de-dup → final anti-join)

Per tile outputs under xmatch/:
  - gaia_ids.csv, ps1_ids.csv
  - matched_any_ids.csv
  - matched_any_ids_unique.csv
  - sex_gaia_unmatched_cdss.csv
  - sex_ps1_unmatched_cdss.csv
  - no_optical_counterparts.csv
"""

import argparse
import csv
import subprocess
import sys
from pathlib import Path
from typing import List

# ------------------------- subprocess helper -------------------------

def run(cmd: List[str], dry: bool=False) -> int:
    pretty = ' '.join(cmd)
    if dry:
        print(f"[DRY] {pretty}")
        return 0
    print(f"[CMD] {pretty}")
    try:
        res = subprocess.run(cmd, check=False, capture_output=True, text=True)
        if res.stdout:
            sys.stdout.write(res.stdout)
        if res.stderr:
            sys.stderr.write(res.stderr)
        if res.returncode != 0:
            print(f"[WARN] command exited with {res.returncode}")
        return res.returncode
    except FileNotFoundError as e:
        print("[ERROR] Is 'stilts' on PATH?", e)
        return 127

# ------------------------- helpers -------------------------

def choose_best_cds_match(xm_dir: Path, base: str) -> Path | None:
    preferred = [xm_dir / f"{base}_within5arcsec.csv", xm_dir / f"{base}.csv"]
    for p in preferred:
        if p.exists():
            return p
    cand = sorted(xm_dir.glob(f"{base}_*.csv"))
    return cand[0] if cand else None

# STILTS wrappers

def stilts_keepcols_number(in_csv: Path, out_csv: Path, dry: bool=False) -> bool:
    if not in_csv.exists():
        return False
    cmd = [
        'stilts', 'tpipe',
        f'in={str(in_csv)}',
        'cmd=keepcols NUMBER',
        f'out={str(out_csv)}', 'ofmt=csv'
    ]
    return run(cmd, dry) == 0

def stilts_concat(inputs: List[Path], out_csv: Path, dry: bool=False) -> bool:
    if not inputs:
        return False
    cmd = ['stilts', 'tcat']
    for p in inputs:
        cmd.append(f'in={str(p)}')
    cmd += [f'out={str(out_csv)}', 'ofmt=csv']
    return run(cmd, dry) == 0


def stilts_exact_id_antijoin(det_csv: Path, ids_csv: Path, out_csv: Path, dry: bool=False) -> bool:
    if not (det_csv.exists() and ids_csv.exists()):
        return False
    cmd = [
        'stilts', 'tmatch2',
        f'in1={str(det_csv)}',
        f'in2={str(ids_csv)}',
        'matcher=exact', "values1=NUMBER", "values2=NUMBER",
        'join=1not2', f'out={str(out_csv)}', 'ofmt=csv', 'progress=none'
    ]
    return run(cmd, dry) == 0


def stilts_positional_unmatched(det_csv: Path, other_csv: Path, out_csv: Path,
                                ra2: str, dec2: str, radius_arcsec: float, dry: bool=False) -> bool:
    if not (det_csv.exists() and other_csv.exists()):
        return False
    cmd = [
        'stilts', 'tskymatch2',
        f'in1={str(det_csv)}', 'ra1=ALPHA_J2000', 'dec1=DELTA_J2000',
        f'in2={str(other_csv)}', f'ra2={ra2}', f'dec2={dec2}',
        'matcher=sky', f'params={radius_arcsec}', 'units=arcsec',
        'join=1not2', 'find=best', f'out={str(out_csv)}', 'ofmt=csv', 'progress=none'
    ]
    return run(cmd, dry) == 0

# Python-based de-dup of NUMBER

def dedup_ids(in_csv: Path, out_csv: Path) -> int:
    if not in_csv.exists():
        return 0
    seen = set()
    n_out = 0
    with open(in_csv, newline='', encoding='utf-8', errors='ignore') as fin,          open(out_csv, 'w', newline='', encoding='utf-8') as fout:
        rin = csv.reader(fin)
        w = csv.writer(fout)
        hdr = next(rin, [])
        if hdr:
            w.writerow(hdr)
        idx = None
        if hdr:
            for i,h in enumerate(hdr):
                if h.strip() == 'NUMBER':
                    idx = i; break
        for row in rin:
            if idx is None or idx >= len(row):
                continue
            key = row[idx]
            if key not in seen:
                seen.add(key)
                w.writerow(row)
                n_out += 1
    return n_out

# ------------------------ per-tile processing -------------------------

def process_tile(tile_dir: Path, tol_cdss: float, positional: bool, dry: bool) -> int:
    catalogs = tile_dir / 'catalogs'
    xmatch = tile_dir / 'xmatch'
    if not (catalogs.exists() and xmatch.exists()):
        return 0
    det_csv = catalogs / 'sextractor_pass2.csv'
    if not det_csv.exists():
        return 0

    wrote = 0

    # Locate CDS match files
    gaia_match = choose_best_cds_match(xmatch, 'sex_gaia_xmatch_cdss')
    ps1_match  = choose_best_cds_match(xmatch, 'sex_ps1_xmatch_cdss')

    # FAST: project to IDs
    gaia_ids = xmatch / 'gaia_ids.csv'
    ps1_ids  = xmatch / 'ps1_ids.csv'
    any_ids  = xmatch / 'matched_any_ids.csv'
    any_ids_u= xmatch / 'matched_any_ids_unique.csv'

    if gaia_match and stilts_keepcols_number(gaia_match, gaia_ids, dry):
        wrote += 1
    if ps1_match and stilts_keepcols_number(ps1_match, ps1_ids, dry):
        wrote += 1

    id_inputs = [p for p in (gaia_ids, ps1_ids) if p.exists()]
    if id_inputs and stilts_concat(id_inputs, any_ids, dry):
        wrote += 1
        # Python de-dup
        out_count = dedup_ids(any_ids, any_ids_u)
        print(f"[INFO] Python de-dup: {any_ids.name} -> {any_ids_u.name} ({out_count} unique)")
        wrote += 1

    # Per-catalog unmatched via ID anti-join
    if gaia_ids.exists() and stilts_exact_id_antijoin(det_csv, gaia_ids, xmatch/'sex_gaia_unmatched_cdss.csv', dry):
        wrote += 1
    if ps1_ids.exists() and stilts_exact_id_antijoin(det_csv, ps1_ids, xmatch/'sex_ps1_unmatched_cdss.csv', dry):
        wrote += 1

    # Final no-optical list (ID anti-join against union)
    target_union = any_ids_u if any_ids_u.exists() else any_ids
    if target_union.exists() and stilts_exact_id_antijoin(det_csv, target_union, xmatch/'no_optical_counterparts.csv', dry):
        wrote += 1

    # Optional positional fallback
    if positional:
        if gaia_match and stilts_positional_unmatched(det_csv, gaia_match, xmatch/'sex_gaia_unmatched_cdss_pos.csv', 'RAJ2000','DEJ2000', tol_cdss, dry):
            wrote += 1
        if ps1_match and stilts_positional_unmatched(det_csv, ps1_match, xmatch/'sex_ps1_unmatched_cdss_pos.csv', 'RAJ2000','DEJ2000', tol_cdss, dry):
            wrote += 1

    return wrote

# ------------------------------ main ----------------------------------

def find_tile_dirs(data_dir: Path):
    modern = data_dir / 'tiles'
    if modern.exists():
        yield from sorted(modern.glob('*/'))
        return
    legacy = data_dir / 'runs'
    if legacy.exists():
        yield from sorted(legacy.glob('run-*/tiles/*/'))
        return


def main():
    ap = argparse.ArgumentParser(description='FAST v3 unmatched/union generator for VASCO tiles (CDS + local, Python de-dup, non-interactive tmatch2).')
    ap.add_argument('--data-dir', default='./data')
    ap.add_argument('--tol-cdss', type=float, default=5.0)
    ap.add_argument('--positional', action='store_true', help='Also write positional CDS unmatched (5")')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    total = 0
    for tile in find_tile_dirs(Path(args.data_dir)):
        wrote = process_tile(tile, args.tol_cdss, args.positional, args.dry_run)
        if wrote:
            print(f"[INFO] {tile.name}: wrote {wrote} output file(s)")
        total += wrote
    print(f"[INFO] Done. Total files {'to be written' if args.dry_run else 'written'}: {total}")
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
