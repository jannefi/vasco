
#!/usr/bin/env python3
"""
Generate per-tile 'unmatched' CSVs.
- Local backend (neighbourhood CSVs): writes sex_gaia_unmatched.csv, sex_ps1_unmatched.csv, sex_usnob_unmatched.csv
- CDS backend (cdsskymatch CSVs):    writes sex_gaia_unmatched_cdss.csv, sex_ps1_unmatched_cdss.csv

Notes:
- Uses STILTS tskymatch2 with join=1not2 (keeps SExtractor detections not present in the other table)
- For CDS files, RA/Dec columns are detected; tolerance defaults to 0.05" to survive rounding.
"""
import sys, subprocess
from pathlib import Path

RUN_ROOT = Path(sys.argv[1]) if len(sys.argv) > 1 else Path('data/runs')

# --- helpers ---
def detect_radec_columns(csv_path: Path, candidates):
    import csv
    try:
        with open(csv_path, newline='') as f:
            hdr = next(csv.reader(f))
        cols = set(h.strip() for h in hdr)
        for ra, dec in candidates:
            if ra in cols and dec in cols:
                return ra, dec
    except Exception:
        pass
    return None, None

def make_unmatched_with_stilts(sex_csv: Path, other_csv: Path, out_csv: Path,
                               ra1: str, dec1: str, ra2: str, dec2: str,
                               radius_arcsec: float = 5.0, join: str = '1not2'):
    if not (sex_csv.exists() and other_csv.exists()):
        return
    cmd = [
        'stilts','tskymatch2',
        f'in1={str(sex_csv)}', f'in2={str(other_csv)}',
        f'ra1={ra1}', f'dec1={dec1}', f'ra2={ra2}', f'dec2={dec2}',
        f'error={radius_arcsec}', f'join={join}',
        f'out={str(out_csv)}', 'ofmt=csv'
    ]
    try:
        subprocess.run(cmd, check=True)
        print('[INFO] STILTS unmatched ->', out_csv)
    except subprocess.CalledProcessError as e:
        print('[WARN] STILTS unmatched failed:', e)

# --- local neighbourhood unmatched ---
for xmatch in RUN_ROOT.glob('run-*/tiles/*/xmatch/sex_gaia_xmatch.csv'):
    tile_dir = xmatch.parent.parent
    sex_csv = tile_dir / 'catalogs' / 'sextractor_pass2.csv'
    gaia_csv = tile_dir / 'catalogs' / 'gaia_neighbourhood.csv'
    out_csv  = xmatch.parent / 'sex_gaia_unmatched.csv'
    ra2, dec2 = detect_radec_columns(gaia_csv, [
        ('RA_ICRS','DE_ICRS'),('RAJ2000','DEJ2000'),('ra','dec'),('RA','DEC')
    ])
    if ra2 and dec2:
        make_unmatched_with_stilts(sex_csv, gaia_csv, out_csv,
                                   ra1='ALPHA_J2000', dec1='DELTA_J2000',
                                   ra2=ra2, dec2=dec2, radius_arcsec=5.0, join='1not2')

for xmatch in RUN_ROOT.glob('run-*/tiles/*/xmatch/sex_ps1_xmatch.csv'):
    tile_dir = xmatch.parent.parent
    sex_csv = tile_dir / 'catalogs' / 'sextractor_pass2.csv'
    ps1_csv = tile_dir / 'catalogs' / 'ps1_neighbourhood.csv'
    out_csv  = xmatch.parent / 'sex_ps1_unmatched.csv'
    # PS1 neighbourhood uses mean positions
    make_unmatched_with_stilts(sex_csv, ps1_csv, out_csv,
                               ra1='ALPHA_J2000', dec1='DELTA_J2000',
                               ra2='raMean', dec2='decMean', radius_arcsec=5.0, join='1not2')

for xmatch in RUN_ROOT.glob('run-*/tiles/*/xmatch/sex_usnob_xmatch.csv'):
    tile_dir = xmatch.parent.parent
    sex_csv = tile_dir / 'catalogs' / 'sextractor_pass2.csv'
    usnob_csv= tile_dir / 'catalogs' / 'usnob_neighbourhood.csv'
    out_csv  = xmatch.parent / 'sex_usnob_unmatched.csv'
    make_unmatched_with_stilts(sex_csv, usnob_csv, out_csv,
                               ra1='ALPHA_J2000', dec1='DELTA_J2000',
                               ra2='RAJ2000',  dec2='DEJ2000', radius_arcsec=5.0, join='1not2')

# --- CDS unmatched with distinct filenames and RA/Dec detection ---
def make_unmatched_from_cdss(sex_csv: Path, cdss_xmatch_csv: Path, out_csv: Path, tol_arcsec: float = 0.05):
    ra2, dec2 = detect_radec_columns(cdss_xmatch_csv, [
        ('ALPHA_J2000','DELTA_J2000'),
        ('RAJ2000','DEJ2000'),
        ('RA_ICRS','DE_ICRS'),
        ('ra','dec'), ('RA','DEC')
    ])
    if not (ra2 and dec2):
        print('[WARN] CDS xmatch lacks recognizable RA/Dec:', cdss_xmatch_csv)
        return
    make_unmatched_with_stilts(sex_csv, cdss_xmatch_csv, out_csv,
                               ra1='ALPHA_J2000', dec1='DELTA_J2000',
                               ra2=ra2, dec2=dec2, radius_arcsec=tol_arcsec, join='1not2')

# Gaia CDS
for cdss in RUN_ROOT.glob('run-*/tiles/*/xmatch/sex_gaia_xmatch_cdss.csv'):
    tile_dir = cdss.parent.parent
    sex_csv = tile_dir / 'catalogs' / 'sextractor_pass2.csv'
    out_csv = cdss.parent / 'sex_gaia_unmatched_cdss.csv'
    make_unmatched_from_cdss(sex_csv, cdss, out_csv)

# PS1 CDS
for cdss in RUN_ROOT.glob('run-*/tiles/*/xmatch/sex_ps1_xmatch_cdss.csv'):
    tile_dir = cdss.parent.parent
    sex_csv = tile_dir / 'catalogs' / 'sextractor_pass2.csv'
    out_csv = cdss.parent / 'sex_ps1_unmatched_cdss.csv'
    make_unmatched_from_cdss(sex_csv, cdss, out_csv)
