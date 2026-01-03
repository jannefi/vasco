#!/usr/bin/env python3
"""
Tile repair + selective rerun helper (v1.3)

Adds:
- `--report-only`      : don't change anything; emit candidate list & counts
- `--resume-stage1`    : for tiles with no pass2.ldac (stage 1), run Step 2 & Step 3 before Step 4/5/6
- retains v1.2 features (progress print, tmp summary with flush & safe finalize)
- flags: `--only-missing`, `--only-damaged`, `--skip-if-step6`, `--dry-run`

Outputs:
- `./data/repair_rerun_summary.tmp.csv` -> finalized to `./data/repair_rerun_summary.csv`
- if `--report-only`: `./data/repair_rerun_candidates.csv` with columns:
  tile,status,header_has_radec,has_pass2_ldac,action
"""
import argparse
import csv
import json
import os
import sys
import subprocess
from pathlib import Path

RADEC_PAIRS = [
    ('ALPHA_J2000','DELTA_J2000'),
    ('ALPHAWIN_J2000','DELTAWIN_J2000'),
    ('X_WORLD','Y_WORLD'),
    ('RAJ2000','DEJ2000'),
    ('RA_ICRS','DE_ICRS'),
    ('RA','DEC'),
    ('ra','dec'),
]
HDU_TRIES = ['#LDAC_OBJECTS', '#2', '#1', '#0', '#3', '#4', '#5', '#6', '#7', '#8']


def sh(cmd: list[str]) -> int:
    try:
        return subprocess.run(cmd, check=True).returncode
    except subprocess.CalledProcessError as e:
        return e.returncode if e.returncode else 1


def has_stilts() -> bool:
    return subprocess.run(['bash','-lc','command -v stilts'], capture_output=True).returncode == 0


def header_line(path: Path) -> str:
    try:
        with path.open('r', encoding='utf-8', newline='') as f:
            return f.readline().strip()
    except Exception:
        return ''


def header_has_radec(header: str) -> bool:
    for a,b in RADEC_PAIRS:
        if a in header and b in header:
            return True
    return False


def iter_tile_dirs() -> list[Path]:
    tiles = []
    flat = Path('./data/tiles')
    if flat.exists():
        tiles += [p for p in sorted(flat.glob('tile-RA*-DEC*')) if p.is_dir()]
    sharded = Path('./data/tiles_by_sky')
    if sharded.exists():
        tiles += [p for p in sorted(sharded.glob('ra_bin=*/dec_bin=*/tile-RA*-DEC*')) if p.is_dir()]
    return tiles


def ensure_sextractor_csv(tile: Path) -> tuple[bool, str]:
    """Ensure catalogs/sextractor_pass2.csv exists and has RA/Dec columns.
    Returns (fixed: bool, radec_pair: 'RA,DEC' or '')."""
    cat = tile / 'catalogs'; cat.mkdir(parents=True, exist_ok=True)
    sex = cat / 'sextractor_pass2.csv'
    hdr = header_line(sex)
    if sex.exists() and header_has_radec(hdr):
        for a,b in RADEC_PAIRS:
            if a in hdr and b in hdr:
                return False, f'{a},{b}'
        return False, ''
    ldac = tile / 'pass2.ldac'
    if not ldac.exists():
        return False, ''
    tmp = cat / '_probe.csv'
    for ext in HDU_TRIES:
        cmd = ['stilts','tcopy', f'in={str(ldac)}{ext}', f'out={str(tmp)}','ofmt=csv']
        try:
            subprocess.run(cmd, check=True, capture_output=True)
        except subprocess.CalledProcessError:
            continue
        hdr = header_line(tmp)
        if header_has_radec(hdr) and len(hdr.split(',')) > 2:
            tmp.replace(sex)
            for a,b in RADEC_PAIRS:
                if a in hdr and b in hdr:
                    return True, f'{a},{b}'
            return True, ''
    # last resort plain
    try:
        subprocess.run(['stilts','tcopy', f'in={str(ldac)}', f'out={str(tmp)}','ofmt=csv'], check=True, capture_output=True)
        hdr = header_line(tmp)
        if header_has_radec(hdr):
            tmp.replace(sex)
            for a,b in RADEC_PAIRS:
                if a in hdr and b in hdr:
                    return True, f'{a},{b}'
            return True, ''
    except subprocess.CalledProcessError:
        pass
    return False, ''


def clean_triage(tile: Path) -> None:
    xdir = tile / 'xmatch'
    for fn in ['test_gaia.csv','test_gaia_within5arcsec.csv','test_ps1.csv','test_ps1_within5arcsec.csv','CDS_manual.log']:
        p = xdir / fn
        try:
            if p.exists(): p.unlink()
        except Exception:
            pass
    p = tile / 'catalogs' / 'sextractor_spike_rejected.csv'
    try:
        if p.exists(): p.unlink()
    except Exception:
        pass


def have_step4_outputs(tile: Path) -> bool:
    xdir = tile / 'xmatch'
    if not xdir.exists(): return False
    return any(xdir.glob('sex_*_xmatch.csv')) or any(xdir.glob('sex_*_xmatch_cdss.csv'))


def need_step5(tile: Path) -> bool:
    xdir = tile / 'xmatch'
    if not xdir.exists(): return False
    for src in list(xdir.glob('sex_*_xmatch.csv')) + list(xdir.glob('sex_*_xmatch_cdss.csv')):
        out = src.with_name(src.stem + '_within5arcsec.csv')
        if not out.exists():
            return True
    return False


def have_step6(tile: Path) -> bool:
    return (tile / 'RUN_SUMMARY.md').exists()


def status_for_tile(tile: Path) -> dict:
    ldac = tile / 'pass2.ldac'
    sex = tile / 'catalogs' / 'sextractor_pass2.csv'
    hdr = header_line(sex)
    st = {
        'tile': tile.name,
        'has_pass2_ldac': ldac.exists(),
        'has_sex_csv': sex.exists(),
        'header_has_radec': bool(hdr and header_has_radec(hdr)),
        'status': '',
        'action': ''
    }
    if not st['has_pass2_ldac']:
        st['status'] = 'stage1_no_pass2'
        st['action'] = 'resume_step2_3'
    elif not st['header_has_radec']:
        st['status'] = 'csv_missing_or_header_only'
        st['action'] = 'repair_csv_then_step4_5_6'
    else:
        st['status'] = 'ok'
        st['action'] = 'step4_5_6_if_missing'
    return st


def main():
    ap = argparse.ArgumentParser(description='Repair header-only SExtractor CSVs and selectively rerun Step4+5+6 (CDS-aware).')
    ap.add_argument('--backend', choices=['cds','local'], default='cds')
    ap.add_argument('--gaia', default=os.getenv('VASCO_CDS_GAIA_TABLE','I/350/gaiaedr3'))
    ap.add_argument('--ps1',  default=os.getenv('VASCO_CDS_PS1_TABLE','II/389/ps1_dr2'))
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--only-missing', action='store_true')
    ap.add_argument('--only-damaged', action='store_true')
    ap.add_argument('--skip-if-step6', action='store_true')
    ap.add_argument('--report-only', action='store_true')
    ap.add_argument('--resume-stage1', action='store_true')
    args = ap.parse_args()

    if not has_stilts():
        print('[ERROR] stilts not found in PATH'); return

    tiles = iter_tile_dirs()
    total = len(tiles)
    print(f'[INFO] Found {total} tiles to scan')

    # Report-only mode: write candidate list and exit
    if args.report_only:
        out = Path('./data/repair_rerun_candidates.csv')
        out.parent.mkdir(parents=True, exist_ok=True)
        cnt_stage1 = cnt_csv_bad = cnt_ok = 0
        with out.open('w', newline='', encoding='utf-8') as fo:
            w = csv.DictWriter(fo, fieldnames=['tile','status','header_has_radec','has_pass2_ldac','action'])
            w.writeheader()
            for tile in tiles:
                st = status_for_tile(tile)
                w.writerow({'tile': st['tile'], 'status': st['status'],
                            'header_has_radec': st['header_has_radec'],
                            'has_pass2_ldac': st['has_pass2_ldac'], 'action': st['action']})
                if st['status'] == 'stage1_no_pass2':
                    cnt_stage1 += 1
                elif st['status'] == 'csv_missing_or_header_only':
                    cnt_csv_bad += 1
                else:
                    cnt_ok += 1
        print(f"[REPORT] stage1(no pass2): {cnt_stage1} | csv_missing/header_only: {cnt_csv_bad} | ok: {cnt_ok}")
        print(f"[REPORT] Candidates saved -> {out}")
        return

    final_csv = Path('./data/repair_rerun_summary.csv')
    tmp_csv   = Path('./data/repair_rerun_summary.tmp.csv')
    tmp_csv.parent.mkdir(parents=True, exist_ok=True)

    def _safe_rename():
        try:
            tmp_csv.replace(final_csv)
            print(f'[OK] Summary saved -> {final_csv}')
        except Exception as e:
            print(f'[WARN] Could not finalize summary: {e}')

    try:
        with tmp_csv.open('w', newline='', encoding='utf-8') as fo:
            w = csv.DictWriter(fo, fieldnames=['tile','fixed_csv','radec','step4_backend','gaia_status','gaia_rows','ps1_status','ps1_rows','step5','step6'])
            w.writeheader(); fo.flush()
            for idx, tile in enumerate(tiles, 1):
                print(f"[RUN] ({idx}/{total}) tile: {tile.name}")
                st = status_for_tile(tile)
                fixed, radec = False, ''

                # Optional resume for stage1 tiles
                if args.resume_stage1 and not st['has_pass2_ldac']:
                    # run step2, step3 to produce pass2.ldac
                    step2 = ['python','-m','vasco.cli_pipeline','step2-pass1','--workdir', str(tile)]
                    step3 = ['python','-m','vasco.cli_pipeline','step3-psf-and-pass2','--workdir', str(tile)]
                    sh(step2); sh(step3)
                    # refresh status
                    st = status_for_tile(tile)

                # Ensure CSV (may repair)
                fixed, radec = ensure_sextractor_csv(tile)
                clean_triage(tile)

                row = {
                    'tile': tile.name,
                    'fixed_csv': str(fixed),
                    'radec': radec,
                    'step4_backend': args.backend,
                    'gaia_status': '', 'gaia_rows': '', 'ps1_status': '', 'ps1_rows': '',
                    'step5': '', 'step6': ''
                }

                if args.dry_run:
                    w.writerow(row); fo.flush(); continue

                if args.only_damaged and not fixed:
                    w.writerow(row); fo.flush(); continue

                run_step4 = True
                if args.only_missing and have_step4_outputs(tile):
                    run_step4 = False
                if run_step4:
                    step4 = ['python','-m','vasco.cli_pipeline','step4-xmatch','--workdir', str(tile),
                             '--xmatch-backend', args.backend,
                             '--cds-gaia-table', args.gaia, '--cds-ps1-table', args.ps1]
                    sh(step4)
                    status_json = tile / 'xmatch' / 'STEP4_XMATCH_STATUS.json'
                    try:
                        if status_json.exists():
                            st_json = json.loads(status_json.read_text(encoding='utf-8'))
                            row['gaia_status'] = st_json.get('gaia','')
                            row['gaia_rows']  = st_json.get('gaia_rows','')
                            row['ps1_status'] = st_json.get('ps1','')
                            row['ps1_rows']   = st_json.get('ps1_rows','')
                    except Exception:
                        pass

                run_step5 = True
                if args.only_missing and not need_step5(tile):
                    run_step5 = False
                if run_step5:
                    step5 = ['python','-m','vasco.cli_pipeline','step5-filter-within5','--workdir', str(tile)]
                    rc5 = sh(step5); row['step5'] = 'ok' if rc5==0 else f'rc={rc5}'

                run_step6 = True
                if args.skip_if_step6 and have_step6(tile):
                    run_step6 = False
                if run_step6:
                    step6 = ['python','-m','vasco.cli_pipeline','step6-summarize','--workdir', str(tile)]
                    rc6 = sh(step6); row['step6'] = 'ok' if rc6==0 else f'rc={rc6}'

                w.writerow(row); fo.flush()
                if idx % 50 == 0:
                    print(f'[PROGRESS] {idx}/{total} processed')
        _safe_rename()
    except KeyboardInterrupt:
        print('\n[INTERRUPTED] Ctrl+C received — finalizing summary...')
        _safe_rename()
        sys.exit(130)

if __name__ == '__main__':
    main()
