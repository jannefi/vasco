#!/usr/bin/env python3
# FAST v3 unmatched/union generator — layout-aware, robust to empty CSVs, with resume/overwrite
import argparse
import csv
import shutil
import subprocess
import sys
from pathlib import Path
from typing import List, Iterable, Optional, Tuple

# ----- layout helpers -----
def iter_tile_dirs_any(data_dir: Path) -> Iterable[Path]:
    flat = data_dir / "tiles"
    sharded = data_dir / "tiles_by_sky"
    if flat.exists():
        for p in sorted(flat.glob("tile-*")):
            if p.is_dir():
                yield p
    if sharded.exists():
        for p in sorted(sharded.glob("ra_bin=*/dec_bin=*/tile-*")):
            if p.is_dir():
                yield p

# ----- tiny CSV helpers -----
def csv_header(path: Path) -> Optional[List[str]]:
    if not path.exists() or path.stat().st_size == 0:
        return None
    try:
        with open(path, newline='', encoding='utf-8', errors='ignore') as f:
            r = csv.reader(f)
            hdr = next(r, None)
            if hdr and any(cell.strip() for cell in hdr):
                return [h.strip() for h in hdr]
            return None
    except Exception:
        return None

def csv_has_data_rows(path: Path) -> bool:
    """True if CSV has at least one data row beyond header."""
    if not path.exists() or path.stat().st_size == 0:
        return False
    try:
        with open(path, newline='', encoding='utf-8', errors='ignore') as f:
            r = csv.reader(f)
            next(r, None)  # header
            for _ in r:
                return True
        return False
    except Exception:
        return False

def write_empty_number_csv(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['NUMBER'])

def copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dst)

# ----- subprocess helper -----
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

# ----- STILTS wrappers (robust) -----
def stilts_keepcols_number(in_csv: Path, out_csv: Path, dry: bool=False) -> bool:
    """
    Keep only NUMBER column. If input is missing/empty or lacks header/NUMBER,
    write an empty NUMBER CSV and continue (treated as 'no matches').
    """
    hdr = csv_header(in_csv)
    if not hdr:
        print(f"[WARN] Input missing/empty or headerless: {in_csv} → writing empty {out_csv.name}")
        write_empty_number_csv(out_csv)
        return True
    if "NUMBER" not in [h.strip() for h in hdr]:
        print(f"[WARN] Column NUMBER missing in {in_csv} → writing empty {out_csv.name}")
        write_empty_number_csv(out_csv)
        return True

    cmd = [
        'stilts', 'tpipe',
        f'in={str(in_csv)}',
        'cmd=keepcols NUMBER',
        f'out={str(out_csv)}', 'ofmt=csv'
    ]
    return run(cmd, dry) == 0

def stilts_concat(inputs: List[Path], out_csv: Path, dry: bool=False) -> bool:
    """
    Concatenate inputs. If none have data rows, write an empty NUMBER CSV and return True.
    If exactly one has data, copy it (avoid calling tcat).
    """
    valid = [p for p in inputs if csv_has_data_rows(p)]
    if not valid:
        print(f"[INFO] No non-empty ID inputs for union → writing empty {out_csv.name}")
        write_empty_number_csv(out_csv)
        return True
    if len(valid) == 1:
        print(f"[INFO] Single ID input with data → copying {valid[0].name} → {out_csv.name}")
        if not dry:
            copy_file(valid[0], out_csv)
        else:
            print(f"[DRY] copy {valid[0]} {out_csv}")
        return True

    cmd = ['stilts', 'tcat']
    for p in valid:
        cmd.append(f'in={str(p)}')
    cmd += [f'out={str(out_csv)}', 'ofmt=csv']
    return run(cmd, dry) == 0

def stilts_exact_id_antijoin(det_csv: Path, ids_csv: Path, out_csv: Path, dry: bool=False) -> bool:
    if not (det_csv.exists() and ids_csv.exists()):
        return False
    # If ids_csv has no data rows, 1not2 == everything in det_csv → but avoid generating
    # huge copies unless the user wants it. We will let STILTS do it only if ids has rows.
    if not csv_has_data_rows(ids_csv):
        print(f"[INFO] IDs empty in {ids_csv.name} → skipping exact 1not2 (no-op).")
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
                                ra1: str, dec1: str, ra2: str, dec2: str,
                                radius_arcsec: float, dry: bool=False) -> bool:
    if not (det_csv.exists() and other_csv.exists()):
        return False
    # If other_csv has no rows, positional 1not2 == everything; skip as no-op.
    if not csv_has_data_rows(other_csv):
        print(f"[INFO] Positional ref empty in {other_csv.name} → skipping positional 1not2 (no-op).")
        return False
    cmd = [
        'stilts', 'tskymatch2',
        f'in1={str(det_csv)}', f'ra1={ra1}', f'dec1={dec1}',
        f'in2={str(other_csv)}', f'ra2={ra2}', f'dec2={dec2}',
        'matcher=sky', f'params={radius_arcsec}', 'units=arcsec',
        'join=1not2', 'find=best', f'out={str(out_csv)}', 'ofmt=csv', 'progress=none'
    ]
    return run(cmd, dry) == 0

# ----- catalog picker -----
def pick_detection_catalog(tile_dir: Path) -> Tuple[Path, str, str]:
    import pandas as pd
    wcsfix = tile_dir / "final_catalog_wcsfix.csv"
    if wcsfix.exists():
        df = pd.read_csv(wcsfix, engine="python", on_bad_lines="skip")
        if {"RA_corr", "Dec_corr"}.issubset(df.columns):
            return wcsfix, "RA_corr", "Dec_corr"
    base_final = tile_dir / "final_catalog.csv"
    if base_final.exists():
        df = pd.read_csv(base_final, engine="python", on_bad_lines="skip")
        if {"ALPHAWIN_J2000","DELTAWIN_J2000"}.issubset(df.columns):
            return base_final, "ALPHAWIN_J2000", "DELTAWIN_J2000"
        elif {"ALPHA_J2000","DELTA_J2000"}.issubset(df.columns):
            return base_final, "ALPHA_J2000", "DELTA_J2000"
    det_csv = tile_dir / "catalogs" / "sextractor_pass2.csv"
    if det_csv.exists():
        df = pd.read_csv(det_csv, engine="python", on_bad_lines="skip")
        if {"ALPHAWIN_J2000","DELTAWIN_J2000"}.issubset(df.columns):
            return det_csv, "ALPHAWIN_J2000", "DELTAWIN_J2000"
        elif {"ALPHA_J2000","DELTA_J2000"}.issubset(df.columns):
            return det_csv, "ALPHA_J2000", "DELTA_J2000"
    # Fallback to SExtractor legacy names
    return det_csv, "ALPHA_J2000", "DELTA_J2000"

def choose_best_cds_match(xm_dir: Path, base: str) -> Optional[Path]:
    preferred = [xm_dir / f"{base}_within5arcsec.csv", xm_dir / f"{base}.csv"]
    for p in preferred:
        if p.exists():
            return p
    cand = sorted(xm_dir.glob(f"{base}_*.csv"))
    return cand[0] if cand else None

# ----- Python de-dup -----
def dedup_ids(in_csv: Path, out_csv: Path) -> int:
    if not in_csv.exists():
        return 0
    seen = set()
    n_out = 0
    with open(in_csv, newline='', encoding='utf-8', errors='ignore') as fin, \
         open(out_csv, 'w', newline='', encoding='utf-8') as fout:
        rin = csv.reader(fin)
        w = csv.writer(fout)
        hdr = next(rin, [])
        if hdr:
            w.writerow(hdr)
        idx = None
        if hdr:
            for i, h in enumerate(hdr):
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

# ----- per-tile processing -----
def tile_done_marker(tile_dir: Path) -> Path:
    return tile_dir / 'xmatch' / '.filter_unmatched_done'

def is_tile_done(tile_dir: Path) -> bool:
    # Consider done if final union exists OR marker exists
    xmatch = tile_dir / 'xmatch'
    return (xmatch / 'no_optical_counterparts.csv').exists() or tile_done_marker(tile_dir).exists()

def process_tile(tile_dir: Path, tol_cdss: float, positional: bool, overwrite: bool, dry: bool) -> int:
    catalogs = tile_dir / 'catalogs'
    xmatch = tile_dir / 'xmatch'
    if not (catalogs.exists() and xmatch.exists()):
        return 0

    if not overwrite and is_tile_done(tile_dir):
        print(f"[INFO] {tile_dir.name}: already processed (resume mode) → skipping")
        return 0

    det_csv, ra1_col, dec1_col = pick_detection_catalog(tile_dir)
    if not det_csv.exists():
        return 0

    wrote = 0
    gaia_match = choose_best_cds_match(xmatch, 'sex_gaia_xmatch_cdss')
    ps1_match  = choose_best_cds_match(xmatch, 'sex_ps1_xmatch_cdss')

    gaia_ids   = xmatch / 'gaia_ids.csv'
    ps1_ids    = xmatch / 'ps1_ids.csv'
    any_ids    = xmatch / 'matched_any_ids.csv'
    any_ids_u  = xmatch / 'matched_any_ids_unique.csv'

    # Keep only NUMBER from GAIA/PS1; robust against empty/bad inputs
    if gaia_match and stilts_keepcols_number(gaia_match, gaia_ids, dry): wrote += 1
    if ps1_match  and stilts_keepcols_number(ps1_match,  ps1_ids,  dry): wrote += 1

    # Build union, resiliently
    id_inputs = [p for p in (gaia_ids, ps1_ids) if p.exists()]
    if id_inputs and stilts_concat(id_inputs, any_ids, dry): wrote += 1
    if any_ids.exists() or dry:
        out_count = dedup_ids(any_ids, any_ids_u)
        print(f"[INFO] Python de-dup: {any_ids.name} -> {any_ids_u.name} ({out_count} unique)")
        wrote += 1

    # Exact ID-based unmatched (only if we truly have some matched IDs)
    if gaia_ids.exists() and stilts_exact_id_antijoin(det_csv, gaia_ids, xmatch/'sex_gaia_unmatched_cdss.csv', dry): wrote += 1
    if ps1_ids.exists()  and stilts_exact_id_antijoin(det_csv, ps1_ids,  xmatch/'sex_ps1_unmatched_cdss.csv', dry): wrote += 1

    # Final unmatched against the union (only if union has any data)
    target_union = any_ids_u if any_ids_u.exists() else any_ids
    if target_union.exists() and csv_has_data_rows(target_union):
        if stilts_exact_id_antijoin(det_csv, target_union, xmatch/'no_optical_counterparts.csv', dry):
            wrote += 1
            # mark done when final artifact is produced
            if not dry:
                tile_done_marker(tile_dir).touch()

    # Optional positional unmatched (safe no-ops if refs are empty)
    if positional:
        if gaia_match and stilts_positional_unmatched(
            det_csv, gaia_match, xmatch/'sex_gaia_unmatched_cdss_pos.csv',
            ra1_col, dec1_col, 'RAJ2000','DEJ2000', tol_cdss, dry): wrote += 1
        if ps1_match and stilts_positional_unmatched(
            det_csv, ps1_match, xmatch/'sex_ps1_unmatched_cdss_pos.csv',
            ra1_col, dec1_col, 'RAJ2000','DEJ2000', tol_cdss, dry): wrote += 1

    return wrote

# ----- CLI -----
def main():
    ap = argparse.ArgumentParser(description='FAST v3 unmatched/union (layout-aware, resume by default).')
    ap.add_argument('--data-dir', default='./data')
    ap.add_argument('--tol-cdss', type=float, default=5.0)
    ap.add_argument('--positional', action='store_true', help='Also write positional CDS unmatched (radius=tol).')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--overwrite', action='store_true', help='Recompute tiles even if outputs exist (disable resume).')
    ap.add_argument('--only-tile', default='', help='Process only this tile directory name (e.g., tile-RA359.987-DEC-26.435)')
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    total = 0
    for tile in iter_tile_dirs_any(data_dir):
        if args.only_tile and tile.name != args.only_tile:
            continue
        wrote = process_tile(tile, args.tol_cdss, args.positional, args.overwrite, args.dry_run)
        if wrote:
            print(f"[INFO] {tile.name}: wrote {wrote} output file(s)")
            total += wrote
    print(f"[INFO] Done. Total files {'to be written' if args.dry_run else 'written'}: {total}")
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
