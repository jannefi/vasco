from __future__ import annotations
import argparse, json, time, subprocess, os
from pathlib import Path
from typing import List, Dict, Any, Tuple

from . import downloader as dl
from .pipeline import run_psf_two_pass, ToolMissingError, _ensure_tool
from .exporter3 import export_and_summarize

from vasco.external_fetch_online import (fetch_gaia_neighbourhood, fetch_ps1_neighbourhood)
from vasco.external_fetch_usnob_vizier import fetch_usnob_neighbourhood
from vasco.mnras.xmatch_stilts import (xmatch_sextractor_with_gaia, xmatch_sextractor_with_ps1)
from vasco.utils.cdsskymatch import cdsskymatch, StiltsNotFound
from .utils.coords import parse_ra as _parse_ra, parse_dec as _parse_dec

# Helpers
from pathlib import Path as _Path
def _ensure_tool_cli(tool: str) -> None:
    import shutil as _sh
    if _sh.which(tool) is None:
        raise RuntimeError(f"Required tool '{tool}' not found in PATH.")

def _validate_within_5_arcsec_unit_tolerant(xmatch_csv: Path) -> Path:
    """Create <stem>_within5arcsec.csv keeping only rows within 5 arcsec.
    Logic:
    - If 'angDist' exists: try treating it as ARCSECONDS (angDist<=5). If that yields 0 rows, fallback to DEGREES (3600*angDist<=5).
    - Else: compute separation via skyDistanceDegrees(ALPHA_J2000,DELTA_J2000,<ext_ra>,<ext_dec>) and select <=5 arcsec."""
    _ensure_tool_cli('stilts')
    import csv, subprocess
    xmatch_csv = Path(xmatch_csv)
    out = xmatch_csv.with_name(xmatch_csv.stem + '_within5arcsec.csv')
    with open(xmatch_csv, newline='') as f:
        header = next(csv.reader(f), [])
    cols = set(header)
    def _write_empty():
        subprocess.run(['stilts','tpipe', f'in={str(xmatch_csv)}', 'cmd=select false', f'out={str(out)}', 'ofmt=csv'], check=True)
        return out
    if 'angDist' in cols:
        p = subprocess.run(['stilts','tpipe', f'in={str(xmatch_csv)}', 'cmd=select angDist<=5', 'omode=count'], capture_output=True, text=True)
        try:
            cnt_text = (p.stdout or '0').strip().split()
            c = int(cnt_text[-1]) if cnt_text else 0
        except Exception:
            c = 0
        if c > 0:
            subprocess.run(['stilts','tpipe', f'in={str(xmatch_csv)}', 'cmd=select angDist<=5', f'out={str(out)}', 'ofmt=csv'], check=True)
            return out
        subprocess.run(['stilts','tpipe', f'in={str(xmatch_csv)}', 'cmd=select 3600*angDist<=5', f'out={str(out)}', 'ofmt=csv'], check=True)
        return out
    for a,b in [('ra','dec'), ('RAJ2000','DEJ2000'), ('RA_ICRS','DE_ICRS'), ('RA','DEC')]:
        if a in cols and b in cols:
            cmd = ("cmd=addcol angDist_arcsec " + f"3600*skyDistanceDegrees(ALPHA_J2000,DELTA_J2000,{a},{b}); " + "select angDist_arcsec<=5")
            subprocess.run(['stilts','tpipe', f'in={str(xmatch_csv)}', cmd, f'out={str(out)}', 'ofmt=csv'], check=True)
            return out
    return _write_empty()

def _build_run_dir(base: str | Path | None = None) -> Path:
    base = Path(base) if base else Path('data') / 'runs'
    base.mkdir(parents=True, exist_ok=True)
    rd = base
    rd.mkdir(parents=True, exist_ok=True)
    return rd

def _write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding='utf-8')
def _write_json(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, indent=2), encoding='utf-8')
def _read_json(path: Path):
    return json.loads(Path(path).read_text(encoding='utf-8'))

def _ensure_sextractor_csv(tile_dir: Path, pass2_ldac: str | Path) -> Path:
    tile_dir = Path(tile_dir)
    pass2_ldac = Path(pass2_ldac)
    cat_dir = tile_dir / 'catalogs'
    cat_dir.mkdir(parents=True, exist_ok=True)
    sex_csv = cat_dir / 'sextractor_pass2.csv'
    if sex_csv.exists():
        return sex_csv
    try:
        from astropy.io import fits
        import csv as pycsv
        with fits.open(pass2_ldac, memmap=False) as hdul:
            hdu = next((h for h in hdul if getattr(h, 'name', '') == 'LDAC_OBJECTS'), None)
            if hdu is None:
                hdu = next((h for h in hdul if getattr(h, 'columns', None)), None)
            if hdu is None:
                raise RuntimeError('No table HDU with columns found in LDAC')
            names = [c.name for c in hdu.columns]
            rows = hdu.data
            with sex_csv.open('w', newline='') as f:
                w = pycsv.writer(f)
                w.writerow(names)
                for r in rows:
                    w.writerow([r[n] for n in names])
        return sex_csv
    except Exception:
        pass
    try:
        cmd = ['stilts', 'tcopy', f'in={str(pass2_ldac)}+2', f'out={str(sex_csv)}', 'ofmt=csv']
        subprocess.run(cmd, check=True)
        return sex_csv
    except Exception as e:
        raise RuntimeError(f'Failed to export LDAC to CSV via Astropy or STILTS: {e}')

def _csv_has_radec(csv_path: Path) -> bool:
    import csv
    try:
        with open(csv_path, newline='') as f:
            hdr = next(csv.reader(f))
        cols = {h.strip() for h in hdr}
        for a, b in [('ra','dec'), ('RA_ICRS','DE_ICRS'), ('RAJ2000','DEJ2000'), ('RA','DEC'), ('lon','lat'), ('raMean','decMean'), ('RAMean','DecMean'), ('ALPHA_J2000','DELTA_J2000')]:
            if a in cols and b in cols:
                return True
        return False
    except Exception:
        return False

def _detect_radec_columns(csv_path: Path) -> Tuple[str, str] | None:
    import csv
    try:
        with open(csv_path, newline='') as f:
            hdr = next(csv.reader(f))
        cols = [h.strip() for h in hdr]
        pairs = [('ALPHA_J2000','DELTA_J2000'), ('RAJ2000','DEJ2000'), ('RA_ICRS','DE_ICRS'), ('ra','dec'), ('RA','DEC'), ('lon','lat'), ('raMean','decMean'), ('RAMean','DecMean')]
        for a,b in pairs:
            if a in cols and b in cols:
                return a,b
        return None
    except Exception:
        return None

def _to_float_ra(val: str | float) -> float:
    try:
        return float(val)
    except Exception:
        return float(_parse_ra(str(val)))
def _to_float_dec(val: str | float) -> float:
    try:
        return float(val)
    except Exception:
        return float(_parse_dec(str(val)))

def _expected_stem(ra: float, dec: float, survey: str, size_arcmin: float) -> str:
    sv_name = dl.SURVEY_ALIASES.get(survey.lower(), survey)
    tag = sv_name.lower().replace(' ', '-')
    return f"{tag}_{ra:.6f}_{dec:.6f}_{int(round(size_arcmin))}arcmin"

def _write_overview(run_dir: Path, counts: dict, results: list, missing: list[dict] | None = None) -> None:
    nl = ''
    lines = ['# Run Overview', '', f"**Planned**: {counts.get('planned', 0)}", f"**Downloaded**: {counts.get('downloaded', 0)}", f"**Processed**: {counts.get('processed', 0)}", '']
    if results:
        lines.append('## Tiles (first 10)')
        for rec in results[:10]:
            t = rec.get('tile', '?')
            p2 = Path(rec.get('pass2', 'pass2.ldac')).name
            lines.append(f"- `{t}` → `{p2}`")
        if len(results) > 10:
            lines.append(f"… and {len(results)-10} more tiles.")
        lines.append('')
    if missing:
        lines.append('## Missing tiles (planned but not processed) — first 15')
        for rec in missing[:15]:
            ra = rec.get('ra'); dec = rec.get('dec'); stem = rec.get('expected_stem')
            lines.append(f"- RA={ra:.6f} Dec={dec:.6f} → expected `{stem}`")
        if len(missing) > 15:
            lines.append(f"… and {len(missing)-15} more missing tiles.")
        lines.append('')
    _write_text(run_dir / 'RUN_OVERVIEW.md', nl.join(lines) + nl)

def cmd_one(args: argparse.Namespace) -> int:
    run_dir = _build_run_dir(Path(args.workdir) if args.workdir else None)
    lg = dl.configure_logger(run_dir / 'logs')
    out_raw = run_dir / 'raw'; out_raw.mkdir(parents=True, exist_ok=True)
    ra = _to_float_ra(args.ra)
    dec = _to_float_dec(args.dec)
    try:
        fits = dl.fetch_skyview_dss(ra, dec, size_arcmin=args.size_arcmin, survey=args.survey, pixel_scale_arcsec=args.pixel_scale_arcsec, out_dir=out_raw, logger=lg)
    except RuntimeError as e:
        if 'Non-POSS plate returned by STScI' in str(e):
            print('[SKIP]', f'RA={ra:.6f}', f'Dec={dec:.6f}', '-> non-POSS plate; tile omitted to preserve strict provenance.')
            results = []
            counts = {'planned': 1, 'downloaded': 0, 'processed': 0}
            missing = [{'ra': float(ra), 'dec': float(dec), 'expected_stem': _expected_stem(ra, dec, args.survey, args.size_arcmin)}]
            _write_json(run_dir / 'RUN_INDEX.json', results)
            _write_json(run_dir / 'RUN_COUNTS.json', counts)
            _write_json(run_dir / 'RUN_MISSING.json', missing)
            _write_overview(run_dir, counts, results, missing)
            print('Run directory:', run_dir)
            print('Planned tiles: 1 Downloaded: 0 Processed: 0 (non-POSS skipped)')
            return 0
        else:
            raise
    td = run_dir
    try:
        p1, psf, p2 = run_psf_two_pass(fits, td, config_root='configs')
    except ToolMissingError as e:
        print('[ERROR]', e)
        return 2
    export_and_summarize(p2, td, export=args.export, histogram_col=args.hist_col)
    radius_arcmin = args.size_arcmin * (2 ** 0.5) * 0.5
    backend = args.xmatch_backend
    if backend == 'local':
        try:
            fetch_gaia_neighbourhood(td, ra, dec, radius_arcmin)
        except Exception as e:
            print('[POST][WARN]', td.name, 'Gaia fetch failed:', e)
        try:
            if os.getenv('VASCO_DISABLE_PS1'):
                print('[POST][INFO]', td.name, 'PS1 disabled by env — skipping fetch')
            else:
                fetch_ps1_neighbourhood(td, ra, dec, radius_arcmin)
        except Exception as e:
            print('[POST][WARN]', td.name, 'PS1 fetch failed:', e)
        try:
            if os.getenv('VASCO_DISABLE_USNOB'):
                print('[POST][INFO]', td.name, 'USNO-B disabled by env — skipping fetch')
            else:
                fetch_usnob_neighbourhood(td, ra, dec, radius_arcmin)
            print('[POST]', td.name, 'USNO-B (VizieR) -> catalogs/usnob_neighbourhood.csv')
        except Exception as e:
            print('[POST][WARN]', td.name, 'USNO-B fetch failed:', e)
        try:
            _post_xmatch_tile(td, p2, radius_arcsec=float(args.xmatch_radius_arcsec))
        except Exception as e:
            print('[POST][WARN] xmatch failed for', td.name, ':', e)
    elif backend == 'cds':
        try:
            _cds_xmatch_tile(td, p2, radius_arcsec=float(args.xmatch_radius_arcsec), cds_gaia_table=args.cds_gaia_table, cds_ps1_table=args.cds_ps1_table)
        except Exception as e:
            print('[POST][WARN] CDS xmatch failed for', td.name, ':', e)
    else:
        print('[POST][WARN]', td.name, 'Unknown xmatch backend:', backend)
    results = [{'tile': Path(fits).stem, 'pass1': p1, 'psf': psf, 'pass2': p2}]
    counts = {'planned': 1, 'downloaded': 1, 'processed': 1}
    _write_json(run_dir / 'RUN_INDEX.json', results)
    _write_json(run_dir / 'RUN_COUNTS.json', counts)
    _write_json(run_dir / 'RUN_MISSING.json', [])
    _write_overview(run_dir, counts, results, [])
    print('Run directory:', run_dir)
    print('Planned tiles:', counts['planned'], 'Downloaded:', counts['downloaded'], 'Processed:', counts['processed'])
    return 0

def cmd_step1_download(args: argparse.Namespace) -> int:
    run_dir = _build_run_dir(Path(args.workdir) if args.workdir else None)
    lg = dl.configure_logger(run_dir / 'logs')
    out_raw = run_dir / 'raw'; out_raw.mkdir(parents=True, exist_ok=True)
    ra = _to_float_ra(args.ra)
    dec = _to_float_dec(args.dec)
    try:
        fits = dl.fetch_skyview_dss(ra, dec, size_arcmin=args.size_arcmin, survey=args.survey, pixel_scale_arcsec=args.pixel_scale_arcsec, out_dir=out_raw, logger=lg)
    except RuntimeError as e:
        if 'Non-POSS plate returned by STScI' in str(e):
            print('[SKIP]', f'RA={ra:.6f}', f'Dec={dec:.6f}', '-> non-POSS plate; tile omitted.')
            counts = {'planned': 1, 'downloaded': 0, 'processed': 0}
            missing = [{'ra': float(ra), 'dec': float(dec), 'expected_stem': _expected_stem(ra, dec, args.survey, args.size_arcmin)}]
            _write_json(run_dir / 'RUN_COUNTS.json', counts)
            _write_json(run_dir / 'RUN_MISSING.json', missing)
            _write_json(run_dir / 'RUN_INDEX.json', [])
            _write_overview(run_dir, counts, [], missing)
            return 0
        else:
            raise
    counts = {'planned': 1, 'downloaded': 1, 'processed': 0}
    _write_json(run_dir / 'RUN_COUNTS.json', counts)
    _write_json(run_dir / 'RUN_INDEX.json', [{'tile': Path(fits).stem}])
    _write_json(run_dir / 'RUN_MISSING.json', [])
    _write_overview(run_dir, counts, [{'tile': Path(fits).stem}], [])
    print('[STEP1] Downloaded FITS ->', fits)
    return 0

def cmd_step2_pass1(args: argparse.Namespace) -> int:
    run_dir = _build_run_dir(Path(args.workdir) if args.workdir else None)
    out_raw = run_dir / 'raw'
    fits_list = sorted(p for p in out_raw.glob('*.fits'))
    if not fits_list:
        print('[STEP2][ERROR] No FITS found in raw/. Run step1-download first.')
        return 2
    fits = str(fits_list[0])
    try:
        p1, psf, p2 = run_psf_two_pass(fits, run_dir, config_root='configs')
        print('[STEP2] Completed pass1 (transitional two-pass ran).')
        rec = {'tile': Path(fits).stem, 'pass1': p1}
        _write_json(run_dir / 'RUN_INDEX.json', [rec])
        return 0
    except ToolMissingError as e:
        print('[STEP2][ERROR]', e)
        return 2

def cmd_step3_psf_and_pass2(args: argparse.Namespace) -> int:
    run_dir = _build_run_dir(Path(args.workdir) if args.workdir else None)
    out_raw = run_dir / 'raw'
    fits_list = sorted(p for p in out_raw.glob('*.fits'))
    if not fits_list:
        print('[STEP3][ERROR] No FITS found in raw/. Run step1-download first.')
        return 2
    fits = str(fits_list[0])
    p2_path = run_dir / 'pass2.ldac'
    if p2_path.exists():
        print('[STEP3] pass2.ldac already exists; skipping.')
        return 0
    try:
        _, _, p2 = run_psf_two_pass(fits, run_dir, config_root='configs')
        print('[STEP3] Completed PSFEx + pass2 ->', p2)
        return 0
    except ToolMissingError as e:
        print('[STEP3][ERROR]', e)
        return 2

def cmd_step4_xmatch(args: argparse.Namespace) -> int:
    run_dir = _build_run_dir(Path(args.workdir) if args.workdir else None)
    p2 = run_dir / 'pass2.ldac'
    if not p2.exists():
        print('[STEP4][ERROR] pass2.ldac not found. Run step3-psf-and-pass2 first.')
        return 2
    backend = args.xmatch_backend
    if backend == 'local':
        try:
            stem = Path(_read_json(run_dir / 'RUN_INDEX.json')[0]['tile']).name
            parts = stem.split('_')
            ra_t = float(parts[1]); dec_t = float(parts[2])
        except Exception:
            print('[STEP4][WARN] Could not derive RA/Dec from index; using 0,0.')
            ra_t, dec_t = 0.0, 0.0
        radius_arcmin = args.size_arcmin * (2 ** 0.5) * 0.5
        try:
            fetch_gaia_neighbourhood(run_dir, ra_t, dec_t, radius_arcmin)
        except Exception as e:
            print('[STEP4][WARN]', run_dir.name, 'Gaia fetch failed:', e)
        try:
            if os.getenv('VASCO_DISABLE_PS1'):
                print('[STEP4][INFO]', run_dir.name, 'PS1 disabled by env — skipping fetch')
            else:
                fetch_ps1_neighbourhood(run_dir, ra_t, dec_t, radius_arcmin)
        except Exception as e:
            print('[STEP4][WARN]', run_dir.name, 'PS1 fetch failed:', e)
        try:
            if os.getenv('VASCO_DISABLE_USNOB'):
                print('[STEP4][INFO]', run_dir.name, 'USNO-B disabled by env — skipping fetch')
            else:
                fetch_usnob_neighbourhood(run_dir, ra_t, dec_t, radius_arcmin)
            print('[STEP4]', run_dir.name, 'USNO-B (VizieR) -> catalogs/usnob_neighbourhood.csv')
        except Exception as e:
            print('[STEP4][WARN]', run_dir.name, 'USNO-B fetch failed:', e)
        try:
            _post_xmatch_tile(run_dir, p2, radius_arcsec=float(args.xmatch_radius_arcsec))
        except Exception as e:
            print('[STEP4][WARN] xmatch failed for', run_dir.name, ':', e)
    elif backend == 'cds':
        try:
            _cds_xmatch_tile(run_dir, p2, radius_arcsec=float(args.xmatch_radius_arcsec), cds_gaia_table=args.cds_gaia_table, cds_ps1_table=args.cds_ps1_table)
        except Exception as e:
            print('[STEP4][WARN] CDS xmatch failed for', run_dir.name, ':', e)
    else:
        print('[STEP4][WARN]', run_dir.name, 'Unknown xmatch backend:', backend)
    return 0

def cmd_step5_within5(args: argparse.Namespace) -> int:
    run_dir = _build_run_dir(Path(args.workdir) if args.workdir else None)
    xdir = run_dir / 'xmatch'
    if not xdir.exists():
        print('[STEP5][ERROR] xmatch/ not found. Run step4-xmatch first.')
        return 2
    wrote = 0
    for csv in xdir.glob('*.csv'):
        try:
            _validate_within_5_arcsec_unit_tolerant(csv)
            wrote += 1
        except Exception as e:
            print('[STEP5][WARN] within5 failed for', csv.name, ':', e)
    print(f'[STEP5] Wrote within5 CSVs for {wrote} xmatch files.')
    return 0

def cmd_step6_summarize(args: argparse.Namespace) -> int:
    run_dir = _build_run_dir(Path(args.workdir) if args.workdir else None)
    p2 = run_dir / 'pass2.ldac'
    if not p2.exists():
        print('[STEP6][ERROR] pass2.ldac not found. Run step3-psf-and-pass2 first.')
        return 2
    try:
        export_and_summarize(p2, run_dir, export=args.export, histogram_col=args.hist_col)
        _write_text(run_dir / 'RUN_SUMMARY.md', '# Summary written
')
        print('[STEP6] Summary + exports written.')
        return 0
    except Exception as e:
        print('[STEP6][ERROR]', e)
        return 2

def cmd_tess(args: argparse.Namespace) -> int:
    # Keep original tess2pass logic (not included here for brevity).
    print('[tess2pass] unchanged in this drop-in; use existing implementation.')
    return 0

def cmd_retry_missing(args: argparse.Namespace) -> int:
    print('[retry-missing] unchanged in this drop-in; use existing implementation.')
    return 0

def main(argv: List[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog='vasco.cli_pipeline', description='VASCO pipeline orchestrator (download + two-pass + export + QA + xmatch: local/CDS)')
    sub = p.add_subparsers(dest='cmd')

    one = sub.add_parser('one2pass', help='One RA/Dec -> download -> two-pass pipeline (post-xmatch backend selectable)')
    one.add_argument('--ra', type=str, required=True)
    one.add_argument('--dec', type=str, required=True)
    one.add_argument('--size-arcmin', type=float, default=60.0)
    one.add_argument('--survey', default='dss1-red')
    one.add_argument('--pixel-scale-arcsec', type=float, default=1.7)
    one.add_argument('--export', choices=['none','csv','parquet','both'], default='csv')
    one.add_argument('--hist-col', default='FWHM_IMAGE')
    one.add_argument('--workdir', default=None)
    one.add_argument('--xmatch-backend', choices=['local','cds'], default='local')
    one.add_argument('--xmatch-radius-arcsec', type=float, default=5.0)
    one.add_argument('--cds-gaia-table', default=os.getenv('VASCO_CDS_GAIA_TABLE'))
    one.add_argument('--cds-ps1-table', default=os.getenv('VASCO_CDS_PS1_TABLE'))
    one.set_defaults(func=cmd_one)

    s1 = sub.add_parser('step1-download', help='Download tile FITS to raw/')
    s1.add_argument('--ra', type=str, required=True)
    s1.add_argument('--dec', type=str, required=True)
    s1.add_argument('--size-arcmin', type=float, default=30.0)
    s1.add_argument('--survey', default='dss1-red')
    s1.add_argument('--pixel-scale-arcsec', type=float, default=1.7)
    s1.add_argument('--workdir', required=True)
    s1.set_defaults(func=cmd_step1_download)

    s2 = sub.add_parser('step2-pass1', help='Run SExtractor pass 1 (transitional)')
    s2.add_argument('--workdir', required=True)
    s2.set_defaults(func=cmd_step2_pass1)

    s3 = sub.add_parser('step3-psf-and-pass2', help='Run PSFEx and PSF-aware pass 2')
    s3.add_argument('--workdir', required=True)
    s3.set_defaults(func=cmd_step3_psf_and_pass2)

    s4 = sub.add_parser('step4-xmatch', help='Cross-match (local/CDS)')
    s4.add_argument('--workdir', required=True)
    s4.add_argument('--xmatch-backend', choices=['local','cds'], default='local')
    s4.add_argument('--xmatch-radius-arcsec', type=float, default=5.0)
    s4.add_argument('--size-arcmin', type=float, default=30.0)
    s4.add_argument('--cds-gaia-table', default=os.getenv('VASCO_CDS_GAIA_TABLE'))
    s4.add_argument('--cds-ps1-table', default=os.getenv('VASCO_CDS_PS1_TABLE'))
    s4.set_defaults(func=cmd_step4_xmatch)

    s5 = sub.add_parser('step5-filter-within5', help='Filter xmatch to <=5 arcsec')
    s5.add_argument('--workdir', required=True)
    s5.set_defaults(func=cmd_step5_within5)

    s6 = sub.add_parser('step6-summarize', help='Export final CSV/ECSV + QA + RUN_*')
    s6.add_argument('--workdir', required=True)
    s6.add_argument('--export', choices=['none','csv','parquet','both'], default='csv')
    s6.add_argument('--hist-col', default='FWHM_IMAGE')
    s6.set_defaults(func=cmd_step6_summarize)

    args = p.parse_args(argv)
    if hasattr(args, 'func'):
        return args.func(args)
    p.print_help()
    return 0

if __name__ == '__main__':
    raise SystemExit(main())

def _validate_within_5_arcsec(xmatch_csv):
    from pathlib import Path as _P
    return _validate_within_5_arcsec_unit_tolerant(_P(xmatch_csv))