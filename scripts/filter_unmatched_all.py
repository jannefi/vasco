
#!/usr/bin/env python3
"""
Generate per-tile 'unmatched' CSVs for the modern VASCO layout.

Layout: <DATA_DIR>/tiles/<tile_id>/{catalogs,xmatch}/...
Outputs under each tile's xmatch/:
  - Local backend (neighbourhood CSVs):
    sex_gaia_unmatched.csv, sex_ps1_unmatched.csv, sex_usnob_unmatched.csv
  - CDS backend (cdsskymatch CSVs):
    sex_gaia_unmatched_cdss.csv, sex_ps1_unmatched_cdss.csv

Notes:
  - Uses STILTS tskymatch2 with join=1not2 (keeps SExtractor detections not present in the other table)
  - Local tolerance defaults to 5.0", CDS tolerance defaults to 0.05" to survive rounding
  - Robust file detection: supports *_cdss_within5arcsec.csv and *_cdss.csv, plus local fallbacks
"""
import argparse
import subprocess
from pathlib import Path
import csv

# ----------------------------- helpers -----------------------------
def detect_radec_columns(csv_path: Path, candidates):
    """Return (RA, DEC) column names if found, else (None, None)."""
    try:
        with open(csv_path, newline='', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            hdr = next(reader, [])
            cols = {h.strip() for h in hdr}
            for ra, dec in candidates:
                if ra in cols and dec in cols:
                    return ra, dec
    except Exception:
        pass
    return None, None

def run_stilts_unmatched(
    sex_csv: Path,
    other_csv: Path,
    out_csv: Path,
    ra1: str,
    dec1: str,
    ra2: str,
    dec2: str,
    radius_arcsec: float,
    join: str = '1not2',
    dry_run: bool = False,
):
    if not (sex_csv.exists() and other_csv.exists()):
        return False
    cmd = [
        'stilts', 'tskymatch2',
        f'in1={str(sex_csv)}', f'in2={str(other_csv)}',
        f'ra1={ra1}', f'dec1={dec1}',
        f'ra2={ra2}', f'dec2={dec2}',
        f'error={radius_arcsec}', f'join={join}',
        f'out={str(out_csv)}', 'ofmt=csv'
    ]
    if dry_run:
        print('[DRY] ' + ' '.join(cmd))
        return True
    try:
        subprocess.run(cmd, check=True)
        print('[INFO] STILTS unmatched ->', out_csv)
        return True
    except subprocess.CalledProcessError as e:
        print('[WARN] STILTS unmatched failed:', e)
        return False

def unmatched_from_cdss(
    sex_csv: Path,
    cdss_xmatch_csv: Path,
    out_csv: Path,
    tol_arcsec: float,
    dry_run: bool = False,
):
    # Detect sextractor RA/Dec robustly
    ra1, dec1 = detect_radec_columns(
        sex_csv,
        [
            ('ALPHA_J2000', 'DELTA_J2000'),
            ('RAJ2000', 'DEJ2000'),
            ('RA_ICRS', 'DE_ICRS'),
            ('ra', 'dec'),
            ('RA', 'DEC'),
        ],
    )
    if not (ra1 and dec1):
        print('[WARN] SExtractor CSV lacks recognizable RA/Dec:', sex_csv)
        return False

    # Detect xmatch RA/Dec robustly
    ra2, dec2 = detect_radec_columns(
        cdss_xmatch_csv,
        [
            ('ALPHA_J2000', 'DELTA_J2000'),
            ('RAJ2000', 'DEJ2000'),
            ('RA_ICRS', 'DE_ICRS'),
            ('ra', 'dec'),
            ('RA', 'DEC'),
        ],
    )
    if not (ra2 and dec2):
        print('[WARN] CDS xmatch lacks recognizable RA/Dec:', cdss_xmatch_csv)
        return False

    return run_stilts_unmatched(
        sex_csv,
        cdss_xmatch_csv,
        out_csv,
        ra1=ra1,
        dec1=dec1,
        ra2=ra2,
        dec2=dec2,
        radius_arcsec=tol_arcsec,
        dry_run=dry_run,
    )

# -------------------------- per-tile processor --------------------------
def process_tile(
    tile_dir: Path,
    tol_local: float,
    tol_cdss: float,
    backend_mode: str,
    dry_run: bool,
) -> int:
    """
    Process one tile directory that contains catalogs/ and xmatch/.
    backend_mode: 'auto' (default), 'local', 'cds', or 'both'
    Returns number of outputs written (or would write in dry-run).
    """
    catalogs = tile_dir / 'catalogs'
    xmatch = tile_dir / 'xmatch'
    if not (catalogs.exists() and xmatch.exists()):
        return 0

    wrote = 0
    sex_csv = catalogs / 'sextractor_pass2.csv'

    # ---------- Local backend (no heuristics on xmatch presence) ----------
    # Inputs:
    #   catalogs/gaia_neighbourhood.csv, ps1_neighbourhood.csv, usnob_neighbourhood.csv
    if backend_mode in ('auto', 'local', 'both'):
        # GAIA local
        gaia_loc = catalogs / 'gaia_neighbourhood.csv'
        if gaia_loc.exists():
            out_csv = xmatch / 'sex_gaia_unmatched.csv'
            ra2, dec2 = detect_radec_columns(
                gaia_loc,
                [
                    ('RA_ICRS', 'DE_ICRS'),
                    ('RAJ2000', 'DEJ2000'),
                    ('ra', 'dec'),
                    ('RA', 'DEC'),
                ],
            )
            if ra2 and dec2:
                if run_stilts_unmatched(
                    sex_csv,
                    gaia_loc,
                    out_csv,
                    ra1='ALPHA_J2000',
                    dec1='DELTA_J2000',
                    ra2=ra2,
                    dec2=dec2,
                    radius_arcsec=tol_local,
                    dry_run=dry_run,
                ):
                    wrote += 1

        # PS1 local
        ps1_loc = catalogs / 'ps1_neighbourhood.csv'
        if ps1_loc.exists():
            out_csv = xmatch / 'sex_ps1_unmatched.csv'
            # PS1 mean columns
            if run_stilts_unmatched(
                sex_csv,
                ps1_loc,
                out_csv,
                ra1='ALPHA_J2000',
                dec1='DELTA_J2000',
                ra2='raMean',
                dec2='decMean',
                radius_arcsec=tol_local,
                dry_run=dry_run,
            ):
                wrote += 1

        # USNOB local
        usnob_loc = catalogs / 'usnob_neighbourhood.csv'
        if usnob_loc.exists():
            out_csv = xmatch / 'sex_usnob_unmatched.csv'
            if run_stilts_unmatched(
                sex_csv,
                usnob_loc,
                out_csv,
                ra1='ALPHA_J2000',
                dec1='DELTA_J2000',
                ra2='RAJ2000',
                dec2='DEJ2000',
                radius_arcsec=tol_local,
                dry_run=dry_run,
            ):
                wrote += 1

    # ---------- CDS backend ----------
    # Inputs (already xmatched via CDS):
    #   xmatch/sex_gaia_xmatch_cdss_within5arcsec.csv OR sex_gaia_xmatch_cdss.csv
    #   xmatch/sex_ps1_xmatch_cdss_within5arcsec.csv OR sex_ps1_xmatch_cdss.csv
    if backend_mode in ('auto', 'cds', 'both'):
        # GAIA CDS
        gaia_candidates = [
            xmatch / 'sex_gaia_xmatch_cdss_within5arcsec.csv',
            xmatch / 'sex_gaia_xmatch_cdss.csv',
        ]
        gaia_cdss = next((p for p in gaia_candidates if p.exists()), None)
        if gaia_cdss:
            out_csv = xmatch / 'sex_gaia_unmatched_cdss.csv'
            if unmatched_from_cdss(sex_csv, gaia_cdss, out_csv, tol_cdss, dry_run=dry_run):
                wrote += 1

        # PS1 CDS
        ps1_candidates = [
            xmatch / 'sex_ps1_xmatch_cdss_within5arcsec.csv',
            xmatch / 'sex_ps1_xmatch_cdss.csv',
        ]
        ps1_cdss = next((p for p in ps1_candidates if p.exists()), None)
        if ps1_cdss:
            out_csv = xmatch / 'sex_ps1_unmatched_cdss.csv'
            if unmatched_from_cdss(sex_csv, ps1_cdss, out_csv, tol_cdss, dry_run=dry_run):
                wrote += 1

    return wrote

# --------------------------- tile discovery ---------------------------
def find_tile_dirs(data_dir: Path):
    """Yield tile directories for modern or legacy layout."""
    modern = data_dir / 'tiles'
    if modern.exists():
        for p in sorted(modern.glob('*/')):
            yield p
        return
    legacy_root = data_dir / 'runs'
    if legacy_root.exists():
        for p in sorted(legacy_root.glob('run-*/tiles/*/')):
            yield p
        return
    return

# ------------------------------ main ------------------------------
def main():
    ap = argparse.ArgumentParser(
        description='Generate per-tile unmatched CSVs for VASCO tiles layout.'
    )
    ap.add_argument('--data-dir', default='./data',
                    help='Path to data directory that contains tiles/')
    ap.add_argument('--backend', choices=['auto', 'local', 'cds', 'both'],
                    default='auto', help='Which backend(s) to process')
    ap.add_argument('--tol-local', type=float, default=5.0,
                    help='Match radius in arcsec for LOCAL neighbourhood comparisons (default: 5.0)')
    ap.add_argument('--tol-cdss', type=float, default=0.05,
                    help='Match radius in arcsec for CDS xmatch comparisons (default: 0.05)')
    ap.add_argument('--dry-run', action='store_true',
                    help='Print what would be executed without running STILTS')
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    tiles = list(find_tile_dirs(data_dir))
    if not tiles:
        print('[ERROR] Could not find tiles under:', data_dir / 'tiles')
        print(' Expected <data>/tiles/<tile>/... (or legacy <data>/runs/run-*/tiles/<tile>/...)')
        return 2

    total = 0
    for tile_dir in tiles:
        wrote = process_tile(tile_dir, args.tol_local, args.tol_cdss, args.backend, args.dry_run)
        if wrote:
            print(f'[INFO] {tile_dir}: wrote {wrote} unmatched file(s)')
        total += wrote

    print(f'[INFO] Done. Total unmatched files {"to be written" if args.dry_run else "written"}: {total}')
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
