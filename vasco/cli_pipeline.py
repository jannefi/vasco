
from __future__ import annotations
import argparse, json, time, subprocess, os, shutil
from pathlib import Path
from typing import List, Tuple
from . import downloader as dl
from .exporter3 import export_and_summarize
from .utils.coords import parse_ra as _parse_ra, parse_dec as _parse_dec
from .pipeline_split import run_pass1, run_psfex, run_pass2
from vasco.external_fetch_online import (fetch_gaia_neighbourhood, fetch_ps1_neighbourhood)
from vasco.external_fetch_usnob_vizier import fetch_usnob_neighbourhood
from vasco.mnras.xmatch_stilts import (xmatch_sextractor_with_gaia, xmatch_sextractor_with_ps1)
from vasco.utils.cdsskymatch import cdsskymatch

# --- helpers ---

def _ensure_tool_cli(tool: str) -> None:
    if shutil.which(tool) is None:
        raise RuntimeError(f"Required tool '{tool}' not found in PATH.")

def _validate_within5_arcsec_unit_tolerant(xmatch_csv: Path) -> Path:
    import csv
    _ensure_tool_cli('stilts')
    xmatch_csv = Path(xmatch_csv)
    out = xmatch_csv.with_name(xmatch_csv.stem + '_within5arcsec.csv')
    with open(xmatch_csv, newline='') as f:
        header = next(csv.reader(f), [])
    cols = set(header)
    def _write_empty():
        subprocess.run(['stilts', 'tpipe', f'in={str(xmatch_csv)}', 'cmd=select false', f'out={str(out)}', 'ofmt=csv'], check=True)
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
            cmd = ("cmd=addcol angDist_arcsec "
                   + f"3600*skyDistanceDegrees(ALPHA_J2000,DELTA_J2000,{a},{b}); "
                   + "select angDist_arcsec<=5")
            subprocess.run(['stilts','tpipe', f'in={str(xmatch_csv)}', cmd, f'out={str(out)}', 'ofmt=csv'], check=True)
            return out
    return _write_empty()

def _build_run_dir(base: str | Path | None = None) -> Path:
    base = Path(base) if base else Path('data') / 'runs'
    base.mkdir(parents=True, exist_ok=True)
    return base

def _write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding='utf-8')

def _write_json(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, indent=2), encoding='utf-8')

def _read_json(path: Path):
    return json.loads(Path(path).read_text(encoding='utf-8'))

# --- POSSI-E enforcement & header export ---

def _fits_survey(path: Path) -> str:
    from astropy.io import fits
    try:
        with fits.open(path, memmap=False) as hdul:
            hdr = hdul[0].header if hdul and hdul[0].header else {}
            return str(hdr.get('SURVEY','')).strip()
    except Exception:
        return ''

def _write_fits_header_json(fits_path: Path) -> Path:
    """Write a JSON sidecar with selected keys and full header dump next to FITS."""
    from astropy.io import fits
    import json as _json
    fits_path = Path(fits_path)
    sidecar = fits_path.with_suffix(fits_path.suffix + '.header.json')
    try:
        with fits.open(fits_path, memmap=False) as hdul:
            hdr = hdul[0].header if hdul and hdul[0].header else {}
            sel_keys = ['SURVEY','PLATEID','PLATE-ID','PLATE','DATE-OBS','RA','DEC','EQUINOX','MJD-OBS','NAXIS1','NAXIS2','CD1_1','CD1_2','CD2_1','CD2_2','CDELT1','CDELT2','CRPIX1','CRPIX2','CRVAL1','CRVAL2']
            selected = {k: (str(hdr.get(k)) if hdr.get(k) is not None else None) for k in sel_keys}
            full = {str(k): (str(hdr.get(k)) if hdr.get(k) is not None else None) for k in hdr.keys()}
            payload = {
                'fits_file': fits_path.name,
                'selected': selected,
                'header': full,
            }
            sidecar.write_text(_json.dumps(payload, indent=2), encoding='utf-8')
    except Exception:
        sidecar.write_text(json.dumps({'fits_file': fits_path.name, 'error': 'header_read_failed'}), encoding='utf-8')
    return sidecar

def _enforce_possi_e_or_skip(fits_path: Path, logger) -> None:
    """Check FITS SURVEY and skip (delete + raise) if not POSSI-E; else write header sidecar."""
    survey = _fits_survey(fits_path)
    if survey != 'POSSI-E':
        try:
            msg = f"[STEP1][FILTER] Non-POSS plate; SURVEY={survey!r} — file will be discarded"
            if logger:
                logger.info(msg)
            else:
                print(msg)
        finally:
            try:
                Path(fits_path).unlink(missing_ok=True)
            except Exception:
                pass
        raise RuntimeError(f"Non-POSS plate returned by STScI: SURVEY={survey!r}")
    else:
        sidecar = _write_fits_header_json(Path(fits_path))
        if logger:
            logger.info(f"[STEP1][HEADER] Wrote FITS header sidecar: {sidecar.name}")
        else:
            print(f"[STEP1][HEADER] Wrote FITS header sidecar: {sidecar.name}")

# --- commands ---

def _expected_stem(ra: float, dec: float, survey: str, size_arcmin: float) -> str:
    sv_name = dl.SURVEY_ALIASES.get(survey.lower(), survey)
    tag = sv_name.lower().replace(' ', '-')
    return f"{tag}_{ra:.6f}_{dec:.6f}_{int(round(size_arcmin))}arcmin"

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

def cmd_one(args: argparse.Namespace) -> int:
    run_dir = _build_run_dir(Path(args.workdir) if args.workdir else None)
    lg = dl.configure_logger(run_dir / 'logs')
    out_raw = run_dir / 'raw'; out_raw.mkdir(parents=True, exist_ok=True)
    ra = _to_float_ra(args.ra); dec = _to_float_dec(args.dec)
    try:
        fits = dl.fetch_skyview_dss(ra, dec, size_arcmin=args.size_arcmin, survey=args.survey, pixel_scale_arcsec=args.pixel_scale_arcsec, out_dir=out_raw, logger=lg)
        _enforce_possi_e_or_skip(Path(fits), lg)
    except RuntimeError as e:
        if 'Non-POSS plate returned by STScI' in str(e):
            print('[SKIP]', f'RA={ra:.6f}', f'Dec={dec:.6f}', '-> non-POSS; tile omitted.')
            counts = {'planned': 1, 'downloaded': 0, 'processed': 0, 'filtered_non_poss': 1}
            missing = [{'ra': float(ra), 'dec': float(dec), 'expected_stem': _expected_stem(ra, dec, args.survey, args.size_arcmin)}]
            _write_json(run_dir / 'RUN_COUNTS.json', counts)
            _write_json(run_dir / 'RUN_MISSING.json', missing)
            _write_json(run_dir / 'RUN_INDEX.json', [])
            _write_overview(run_dir, counts, [], missing)
            return 0
        else:
            raise
    p1, _ = run_pass1(fits, run_dir, config_root='configs')
    psf = run_psfex(p1, run_dir, config_root='configs')
    p2 = run_pass2(fits, run_dir, psf, config_root='configs')
    export_and_summarize(p2, run_dir, export=args.export, histogram_col=args.hist_col)
    radius_arcmin = args.size_arcmin * (2 ** 0.5) * 0.5
    backend = args.xmatch_backend
    if backend == 'local':
        try:
            fetch_gaia_neighbourhood(run_dir, ra, dec, radius_arcmin)
        except Exception as e:
            print('[POST][WARN]', run_dir.name, 'Gaia fetch failed:', e)
        try:
            if os.getenv('VASCO_DISABLE_PS1'):
                print('[POST][INFO]', run_dir.name, 'PS1 disabled by env — skipping fetch')
            else:
                fetch_ps1_neighbourhood(run_dir, ra, dec, radius_arcmin)
        except Exception as e:
            print('[POST][WARN]', run_dir.name, 'PS1 fetch failed:', e)
        try:
            if os.getenv('VASCO_DISABLE_USNOB'):
                print('[POST][INFO]', run_dir.name, 'USNO-B disabled by env — skipping fetch')
            else:
                fetch_usnob_neighbourhood(run_dir, ra, dec, radius_arcmin)
                print('[POST]', run_dir.name, 'USNO-B (VizieR) -> catalogs/usnob_neighbourhood.csv')
        except Exception as e:
            print('[POST][WARN]', run_dir.name, 'USNO-B fetch failed:', e)
        try:
            _post_xmatch_tile(run_dir, p2, radius_arcsec=float(args.xmatch_radius_arcsec))
        except Exception as e:
            print('[POST][WARN] xmatch failed for', run_dir.name, ':', e)
    elif backend == 'cds':
        try:
            _cds_xmatch_tile(run_dir, p2, radius_arcsec=float(args.xmatch_radius_arcsec),
                             cds_gaia_table=args.cds_gaia_table, cds_ps1_table=args.cds_ps1_table)
        except Exception as e:
            print('[POST][WARN] CDS xmatch failed for', run_dir.name, ':', e)
    else:
        print('[POST][WARN]', run_dir.name, 'Unknown xmatch backend:', backend)
    results = [{'tile': Path(fits).stem, 'pass1': str(p1), 'psf': str(psf), 'pass2': str(p2)}]
    counts = {'planned': 1, 'downloaded': 1, 'processed': 1, 'filtered_non_poss': 0}
    _write_json(run_dir / 'RUN_INDEX.json', results)
    _write_json(run_dir / 'RUN_COUNTS.json', counts)
    _write_json(run_dir / 'RUN_MISSING.json', [])
    _write_overview(run_dir, counts, results, [])
    print('Run directory:', run_dir)
    return 0

def cmd_step1_download(args: argparse.Namespace) -> int:
    run_dir = _build_run_dir(Path(args.workdir) if args.workdir else None)
    lg = dl.configure_logger(run_dir / 'logs')
    out_raw = run_dir / 'raw'; out_raw.mkdir(parents=True, exist_ok=True)
    ra = _to_float_ra(args.ra); dec = _to_float_dec(args.dec)
    try:
        fits = dl.fetch_skyview_dss(ra, dec, size_arcmin=args.size_arcmin, survey=args.survey, pixel_scale_arcsec=args.pixel_scale_arcsec, out_dir=out_raw, logger=lg)
        _enforce_possi_e_or_skip(Path(fits), lg)
    except RuntimeError as e:
        if 'Non-POSS plate returned by STScI' in str(e):
            print('[SKIP]', f'RA={ra:.6f}', f'Dec={dec:.6f}', '-> non-POSS; tile omitted.')
            counts = {'planned': 1, 'downloaded': 0, 'processed': 0, 'filtered_non_poss': 1}
            missing = [{'ra': float(ra), 'dec': float(dec), 'expected_stem': _expected_stem(ra, dec, args.survey, args.size_arcmin)}]
            _write_json(run_dir / 'RUN_COUNTS.json', counts)
            _write_json(run_dir / 'RUN_MISSING.json', missing)
            _write_json(run_dir / 'RUN_INDEX.json', [])
            _write_overview(run_dir, counts, [], missing)
            return 0
        else:
            raise
    counts = {'planned': 1, 'downloaded': 1, 'processed': 0, 'filtered_non_poss': 0}
    _write_json(run_dir / 'RUN_COUNTS.json', counts)
    _write_json(run_dir / 'RUN_INDEX.json', [{'tile': Path(fits).stem}])
    _write_json(run_dir / 'RUN_MISSING.json', [])
    _write_overview(run_dir, counts, [{'tile': Path(fits).stem}], [])
    print('[STEP1] Downloaded FITS ->', fits)
    return 0

# --- overview writer with extra summary count ---

def _write_overview(run_dir: Path, counts: dict, results: list, missing: list[dict] | None = None) -> None:
    lines = ['# Run Overview','',
             f"**Planned**: {counts.get('planned', 0)}",
             f"**Downloaded**: {counts.get('downloaded', 0)}",
             f"**Processed**: {counts.get('processed', 0)}",
             f"**Non-POSS filtered**: {counts.get('filtered_non_poss', 0)}",
             '']
    if results:
        lines.append('## Tiles (first 10)')
        for rec in results[:10]:
            t = rec.get('tile','?')
            p2 = Path(rec.get('pass2','pass2.ldac')).name
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
    _write_text(run_dir / 'RUN_OVERVIEW.md', ''.join(lines))

# --- step2, step3, post-xmatch, cds-xmatch, step4, step5, step6 ---

def cmd_step2_pass1(args: argparse.Namespace) -> int:
    run_dir = _build_run_dir(Path(args.workdir) if args.workdir else None)
    raw = run_dir / 'raw'
    fits = next((p for p in sorted(raw.glob('*.fits'))), None)
    if not fits:
        print('[STEP2][ERROR] No FITS in raw/. Run step1-download first.')
        return 2
    p1, _ = run_pass1(fits, run_dir, config_root='configs')
    _write_json(run_dir / 'RUN_INDEX.json', [{'tile': Path(fits).stem, 'pass1': str(p1)}])
    print('[STEP2] pass1 ->', p1)
    return 0

def cmd_step3_psf_and_pass2(args: argparse.Namespace) -> int:
    run_dir = _build_run_dir(Path(args.workdir) if args.workdir else None)
    p1 = run_dir / 'pass1.ldac'
    raw = run_dir / 'raw'
    fits = next((p for p in sorted(raw.glob('*.fits'))), None)
    if not p1.exists() or not fits:
        print('[STEP3][ERROR] pass1.ldac or FITS missing. Run step2-pass1 first.')
        return 2
    psf = run_psfex(p1, run_dir, config_root='configs')
    p2 = run_pass2(fits, run_dir, psf, config_root='configs')
    print('[STEP3] psf ->', psf, '; pass2 ->', p2)
    return 0

def _post_xmatch_tile(tile_dir, pass2_ldac, *, radius_arcsec: float = 5.0) -> None:
    tile_dir = Path(tile_dir)
    xdir = tile_dir / 'xmatch'; xdir.mkdir(parents=True, exist_ok=True)
    sex_csv = _ensure_sextractor_csv(tile_dir, pass2_ldac)
    sex_cols = _detect_radec_columns(sex_csv)
    gaia_csv = tile_dir / 'catalogs' / 'gaia_neighbourhood.csv'
    ps1_csv = tile_dir / 'catalogs' / 'ps1_neighbourhood.csv'
    usnob_csv = tile_dir / 'catalogs' / 'usnob_neighbourhood.csv'
    if gaia_csv.exists() and _csv_has_radec(gaia_csv):
        out_gaia = xdir / 'sex_gaia_xmatch.csv'
        xmatch_sextractor_with_gaia(sex_csv, gaia_csv, out_gaia, radius_arcsec=radius_arcsec)
        print('[POST]', tile_dir.name, 'Gaia xmatch ->', out_gaia)
    if ps1_csv.exists() and _csv_has_radec(ps1_csv):
        out_ps1 = xdir / 'sex_ps1_xmatch.csv'
        xmatch_sextractor_with_ps1(sex_csv, ps1_csv, out_ps1, radius_arcsec=radius_arcsec)
        print('[POST]', tile_dir.name, 'PS1 xmatch ->', out_ps1)
    try:
        if usnob_csv.exists() and _csv_has_radec(usnob_csv):
            usnob_cols = _detect_radec_columns(usnob_csv) or ('RAJ2000','DEJ2000')
            if sex_cols is None:
                sex_cols = ('ALPHA_J2000','DELTA_J2000')
            ra1, dec1 = sex_cols; ra2, dec2 = usnob_cols
            out_usnob = xdir / 'sex_usnob_xmatch.csv'
            subprocess.run(['stilts','tskymatch2', f'in1={str(sex_csv)}', f'in2={str(usnob_csv)}', f'ra1={ra1}', f'dec1={dec1}', f'ra2={ra2}', f'dec2={dec2}', f'error={radius_arcsec}', 'join=1and2', f'out={str(out_usnob)}', 'ofmt=csv'], check=True)
            print('[POST]', tile_dir.name, 'USNO-B xmatch ->', out_usnob)
    except FileNotFoundError:
        print('[POST][WARN]', tile_dir.name, 'STILTS not found; USNOB skipped')

def _csv_has_radec(csv_path: Path) -> bool:
    import csv
    try:
        with open(csv_path, newline='') as f:
            hdr = next(csv.reader(f))
            cols = {h.strip() for h in hdr}
        for a,b in [('ra','dec'), ('RA_ICRS','DE_ICRS'), ('RAJ2000','DEJ2000'), ('RA','DEC'), ('lon','lat'), ('raMean','decMean'), ('RAMean','DecMean'), ('ALPHA_J2000','DELTA_J2000')]:
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
    subprocess.run(['stilts','tcopy', f'in={str(pass2_ldac)}+2', f'out={str(sex_csv)}', 'ofmt=csv'], check=True)
    return sex_csv

def _cds_xmatch_tile(tile_dir, pass2_ldac, *, radius_arcsec: float = 5.0, cds_gaia_table: str | None = None, cds_ps1_table: str | None = None) -> None:
    tile_dir = Path(tile_dir)
    xdir = tile_dir / 'xmatch'; xdir.mkdir(parents=True, exist_ok=True)
    sex_csv = _ensure_sextractor_csv(tile_dir, pass2_ldac)
    sex_cols = _detect_radec_columns(sex_csv) or ('ALPHA_J2000','DELTA_J2000')
    ra_col, dec_col = sex_cols

    # Gaia
    if cds_gaia_table:
        out_gaia = xdir / 'sex_gaia_xmatch_cdss.csv'
        try:
            _cds_log(xdir, f"[STEP4][CDS] Start — radius={radius_arcsec} arcsec; GAIA={cds_gaia_table!r}; PS1={cds_ps1_table!r}")
            _cds_log(xdir, f"[STEP4][CDS] Using SExtractor CSV: {sex_csv.name} (RA={ra_col}, DEC={dec_col})")
            _cds_log(xdir, f"[STEP4][CDS] Query Gaia table {cds_gaia_table} -> {out_gaia.name}")
            cdsskymatch(sex_csv, out_gaia, ra=ra_col, dec=dec_col, cdstable=cds_gaia_table, radius_arcsec=radius_arcsec, find='best', ofmt='csv', omode='out')
            _validate_within5_arcsec_unit_tolerant(out_gaia)
            rows = _csv_row_count(out_gaia)
            _cds_log(xdir, f"[STEP4][CDS] Gaia OK — rows={rows}")
            time.sleep(10.0)
        except Exception as e:
            _cds_log(xdir, f"[STEP4][CDS][WARN] Gaia xmatch failed: {e}")
    else:
        _cds_log(xdir, "[STEP4][CDS] Gaia table not provided — skipping")

    # PS1 coverage guard (Fix A) — skip south of -30 deg
    if cds_ps1_table:
        if os.getenv('VASCO_DISABLE_PS1'):
            _cds_log(xdir, "[STEP4][CDS] PS1 disabled by env — skipping")
            return
        center = _tile_center_from_index_or_name(tile_dir)
        if center and center[1] < -30.0:
            _cds_log(xdir, f"[STEP4][CDS] PS1 skipped (Dec={center[1]:.3f} < -30°, outside survey coverage)")
            return
        out_ps1 = xdir / 'sex_ps1_xmatch_cdss.csv'
        try:
            _cds_log(xdir, f"[STEP4][CDS] Query PS1 table {cds_ps1_table} -> {out_ps1.name}")
            cdsskymatch(sex_csv, out_ps1, ra=ra_col, dec=dec_col, cdstable=cds_ps1_table, radius_arcsec=radius_arcsec, find='best', ofmt='csv', omode='out')
            _validate_within5_arcsec_unit_tolerant(out_ps1)
            rows = _csv_row_count(out_ps1)
            _cds_log(xdir, f"[STEP4][CDS] PS1 OK — rows={rows}")
        except Exception as e:
            _cds_log(xdir, f"[STEP4][CDS][WARN] PS1 xmatch failed: {e}")
    else:
        _cds_log(xdir, "[STEP4][CDS] PS1 table not provided — skipping")

# --- CDS logging & helpers ---

def _csv_row_count(path: Path) -> int:
    import csv
    try:
        with open(path, newline='') as f:
            r = csv.reader(f)
            next(r, None)
            return sum(1 for _ in r)
    except Exception:
        return -1

def _cds_log(xdir: Path, msg: str) -> None:
    xdir = Path(xdir)
    log = xdir / 'STEP4_CDS.log'
    with log.open('a', encoding='utf-8') as f:
        f.write(msg.rstrip('') + "")
    print(msg.rstrip(''))

def _coords_from_tile_dirname(name: str) -> tuple[float, float] | None:
    try:
        if not name.startswith('tile-RA') or '-DEC' not in name:
            return None
        ra_part = name[len('tile-RA'): name.index('-DEC')]
        dec_part = name[name.index('-DEC') + len('-DEC') :]
        return float(ra_part), float(dec_part)
    except Exception:
        return None

def _tile_center_from_index_or_name(run_dir: Path) -> tuple[float, float] | None:
    try:
        recs = _read_json(Path(run_dir) / 'RUN_INDEX.json')
        if recs:
            stem = Path(recs[0].get('tile','')).name
            parts = stem.split('_')
            return float(parts[1]), float(parts[2])
    except Exception:
        pass
    return _coords_from_tile_dirname(Path(run_dir).name)

# --- CLI ---

def _write_overview(run_dir: Path, counts: dict, results: list, missing: list[dict] | None = None) -> None:
    lines = ['# Run Overview','',
             f"**Planned**: {counts.get('planned', 0)}",
             f"**Downloaded**: {counts.get('downloaded', 0)}",
             f"**Processed**: {counts.get('processed', 0)}",
             f"**Non-POSS filtered**: {counts.get('filtered_non_poss', 0)}",
             '']
    if results:
        lines.append('## Tiles (first 10)')
        for rec in results[:10]:
            t = rec.get('tile','?')
            p2 = Path(rec.get('pass2','pass2.ldac')).name
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
    _write_text(run_dir / 'RUN_OVERVIEW.md', ''.join(lines))

# subcommands

def cmd_step4_xmatch(args: argparse.Namespace) -> int:
    run_dir = _build_run_dir(Path(args.workdir) if args.workdir else None)
    p2 = run_dir / 'pass2.ldac'
    if not p2.exists():
        print('[STEP4][ERROR] pass2.ldac missing. Run step3-psf-and-pass2 first.')
        return 2
    backend = args.xmatch_backend
    if backend == 'local':
        try:
            stem = Path(_read_json(run_dir / 'RUN_INDEX.json')[0]['tile']).name
            parts = stem.split('_'); ra_t = float(parts[1]); dec_t = float(parts[2])
        except Exception:
            ra_t, dec_t = 0.0, 0.0
        radius_arcmin = args.size_arcmin * (2 ** 0.5) * 0.5
        try:
            fetch_gaia_neighbourhood(run_dir, ra_t, dec_t, radius_arcmin)
        except Exception as e:
            print('[STEP4][WARN]', run_dir.name, 'Gaia fetch failed:', e)
        try:
            if os.getenv('VASCO_DISABLE_PS1'):
                print('[STEP4][INFO]', run_dir.name, 'PS1 disabled by env')
            else:
                fetch_ps1_neighbourhood(run_dir, ra_t, dec_t, radius_arcmin)
        except Exception as e:
            print('[STEP4][WARN]', run_dir.name, 'PS1 fetch failed:', e)
        try:
            if os.getenv('VASCO_DISABLE_USNOB'):
                print('[STEP4][INFO]', run_dir.name, 'USNO-B disabled by env')
            else:
                fetch_usnob_neighbourhood(run_dir, ra_t, dec_t, radius_arcmin)
                print('[STEP4]', run_dir.name, 'USNO-B -> catalogs/usnob_neighbourhood.csv')
        except Exception as e:
            print('[STEP4][WARN]', run_dir.name, 'USNO-B fetch failed:', e)
        _post_xmatch_tile(run_dir, p2, radius_arcsec=float(args.xmatch_radius_arcsec))
        return 0
    if backend == 'cds':
        _cds_xmatch_tile(run_dir, p2, radius_arcsec=float(args.xmatch_radius_arcsec),
                         cds_gaia_table=args.cds_gaia_table, cds_ps1_table=args.cds_ps1_table)
        return 0
    print('[STEP4][WARN]', run_dir.name, 'Unknown backend:', backend)
    return 0

def cmd_step5_filter_within5(args: argparse.Namespace) -> int:
    run_dir = _build_run_dir(Path(args.workdir) if args.workdir else None)
    xdir = run_dir / 'xmatch'
    if not xdir.exists():
        print('[STEP5][ERROR] xmatch/ missing. Run step4-xmatch first.')
        return 2
    wrote = 0
    for csv in xdir.glob('*.csv'):
        try:
            _validate_within5_arcsec_unit_tolerant(csv)
            wrote += 1
        except Exception as e:
            print('[STEP5][WARN] within5 failed for', csv.name, ':', e)
    print(f'[STEP5] Wrote within5 CSVs for {wrote} xmatch files.')
    return 0

def cmd_step6_summarize(args: argparse.Namespace) -> int:
    run_dir = _build_run_dir(Path(args.workdir) if args.workdir else None)
    p2 = run_dir / 'pass2.ldac'
    if not p2.exists():
        print('[STEP6][ERROR] pass2.ldac missing. Run step3-psf-and-pass2 first.')
        return 2
    export_and_summarize(p2, run_dir, export=args.export, histogram_col=args.hist_col)
    _write_text(run_dir / 'RUN_SUMMARY.md', '# Summary written')
    print('[STEP6] Summary + exports written.')
    return 0

# argparse + main

def main(argv: List[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog='vasco.cli_pipeline', description='VASCO pipeline orchestrator (split workflow + POSSI-E guard)')
    sub = p.add_subparsers(dest='cmd')

    one = sub.add_parser('one2pass', help='One RA/Dec -> 1+2+3 + xmatch + summarize')
    one.add_argument('--ra', type=str, required=True)
    one.add_argument('--dec', type=str, required=True)
    one.add_argument('--size-arcmin', type=float, default=30.0)
    one.add_argument('--survey', default='dss1-red')
    one.add_argument('--pixel-scale-arcsec', type=float, default=1.7)
    one.add_argument('--export', choices=['none','csv','parquet','both'], default='csv')
    one.add_argument('--hist-col', default='FWHM_IMAGE')
    one.add_argument('--workdir', required=True)
    one.add_argument('--xmatch-backend', choices=['local','cds'], default='local')
    one.add_argument('--xmatch-radius-arcsec', type=float, default=5.0)
    one.add_argument('--cds-gaia-table', default=os.getenv('VASCO_CDS_GAIA_TABLE'))
    one.add_argument('--cds-ps1-table', default=os.getenv('VASCO_CDS_PS1_TABLE'))
    one.set_defaults(func=cmd_one)

    s1 = sub.add_parser('step1-download', help='Download tile FITS to raw/ (POSSI-E enforced; header sidecar)')
    s1.add_argument('--ra', type=str, required=True)
    s1.add_argument('--dec', type=str, required=True)
    s1.add_argument('--size-arcmin', type=float, default=30.0)
    s1.add_argument('--survey', default='dss1-red')
    s1.add_argument('--pixel-scale-arcsec', type=float, default=1.7)
    s1.add_argument('--workdir', required=True)
    s1.set_defaults(func=cmd_step1_download)

    s2 = sub.add_parser('step2-pass1', help='Run SExtractor pass 1')
    s2.add_argument('--workdir', required=True)
    s2.set_defaults(func=cmd_step2_pass1)

    s3 = sub.add_parser('step3-psf-and-pass2', help='Run PSFEx and PSF-aware pass 2')
    s3.add_argument('--workdir', required=True)
    s3.set_defaults(func=cmd_step3_psf_and_pass2)

    s4 = sub.add_parser('step4-xmatch', help='Cross-match (local/CDS; PS1 coverage guard)')
    s4.add_argument('--workdir', required=True)
    s4.add_argument('--xmatch-backend', choices=['local','cds'], default='local')
    s4.add_argument('--xmatch-radius-arcsec', type=float, default=5.0)
    s4.add_argument('--size-arcmin', type=float, default=30.0)
    s4.add_argument('--cds-gaia-table', default=os.getenv('VASCO_CDS_GAIA_TABLE'))
    s4.add_argument('--cds-ps1-table', default=os.getenv('VASCO_CDS_PS1_TABLE'))
    s4.set_defaults(func=cmd_step4_xmatch)

    s5 = sub.add_parser('step5-filter-within5', help='Filter xmatch to <= 5 arcsec')
    s5.add_argument('--workdir', required=True)
    s5.set_defaults(func=cmd_step5_filter_within5)

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
