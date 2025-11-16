
from __future__ import annotations
import argparse, json, time, subprocess, os
from pathlib import Path
from typing import List, Dict, Any

from . import downloader as dl
from .pipeline import run_psf_two_pass, ToolMissingError
from .exporter3 import export_and_summarize

# Online external catalog fetchers
from vasco.external_fetch_online import (
    fetch_gaia_neighbourhood,
    fetch_ps1_neighbourhood,
)

# STILTS helpers
from vasco.mnras.xmatch_stilts import (
    xmatch_sextractor_with_gaia,
    xmatch_sextractor_with_ps1,
)

# -----------------------------------------------------------
# Run dirs / overview helpers
# -----------------------------------------------------------

def _build_run_dir(base: str | Path | None = None) -> Path:
    base = Path(base) if base else Path('data') / 'runs'
    base.mkdir(parents=True, exist_ok=True)
    ts = time.strftime('run-%Y%m%d_%H%M%S')
    rd = base / ts
    rd.mkdir(parents=True, exist_ok=True)
    return rd

def _write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding='utf-8')

def _write_json(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, indent=2), encoding='utf-8')

def _read_json(path: Path):
    return json.loads(Path(path).read_text(encoding='utf-8'))

def _tile_dir(run_dir: Path, stem: str) -> Path:
    td = run_dir / 'tiles' / stem
    td.mkdir(parents=True, exist_ok=True)
    return td

def _expected_stem(ra: float, dec: float, survey: str, size_arcmin: float) -> str:
    sv_name = dl.SURVEY_ALIASES.get(survey.lower(), survey)
    tag = sv_name.lower().replace(' ', '-')
    return f"{tag}_{ra:.6f}_{dec:.6f}_{int(round(size_arcmin))}arcmin"

def _write_overview(run_dir: Path, counts: dict, results: list, missing: list[dict] | None = None) -> None:
    nl = ''
    lines = [
        '# Run Overview',
        '',
        f"**Planned**: {counts.get('planned', 0)}",
        f"**Downloaded**: {counts.get('downloaded', 0)}",
        f"**Processed**: {counts.get('processed', 0)}",
        ''
    ]
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

# -----------------------------------------------------------
# LDAC → CSV export (robust)
# -----------------------------------------------------------

def _ensure_sextractor_csv(tile_dir: Path, pass2_ldac: str | Path) -> Path:
    tile_dir = Path(tile_dir)
    pass2_ldac = Path(pass2_ldac)
    cat_dir = tile_dir / 'catalogs'
    cat_dir.mkdir(parents=True, exist_ok=True)
    sex_csv = cat_dir / 'sextractor_pass2.csv'
    if sex_csv.exists():
        return sex_csv

    # Astropy (robust for LDAC)
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
        pass  # STILTS fallback

    try:
        cmd = ['stilts', 'tcopy', f'in={str(pass2_ldac)}+2', f'out={str(sex_csv)}', 'ofmt=csv']
        subprocess.run(cmd, check=True)
        return sex_csv
    except Exception as e:
        raise RuntimeError(f'Failed to export LDAC to CSV via Astropy or STILTS: {e}')

# -----------------------------------------------------------
# CSV header RA/Dec presence check (includes PS1 mean names)
# -----------------------------------------------------------

def _csv_has_radec(csv_path: Path) -> bool:
    import csv
    try:
        with open(csv_path, newline='') as f:
            hdr = next(csv.reader(f))
        cols = {h.strip() for h in hdr}
        for a, b in [
            ('ra','dec'), ('RA_ICRS','DE_ICRS'), ('RAJ2000','DEJ2000'),
            ('RA','DEC'), ('lon','lat'), ('raMean','decMean'), ('RAMean','DecMean')
        ]:
            if a in cols and b in cols:
                return True
        return False
    except Exception:
        return False

# -----------------------------------------------------------
# Post-xmatch per tile (Gaia/PS1 independent)
# -----------------------------------------------------------

def _post_xmatch_tile(tile_dir, pass2_ldac, *, radius_arcsec: float = 2.0) -> None:
    tile_dir = Path(tile_dir)
    xdir = tile_dir / 'xmatch'
    xdir.mkdir(parents=True, exist_ok=True)

    sex_csv = _ensure_sextractor_csv(tile_dir, pass2_ldac)
    gaia_csv = tile_dir / 'catalogs' / 'gaia_neighbourhood.csv'
    ps1_csv  = tile_dir / 'catalogs' / 'ps1_neighbourhood.csv'

    # Gaia independent
    try:
        if gaia_csv.exists() and _csv_has_radec(gaia_csv):
            out_gaia = xdir / 'sex_gaia_xmatch.csv'
            xmatch_sextractor_with_gaia(sex_csv, gaia_csv, out_gaia, radius_arcsec=radius_arcsec)
            print('[POST]', tile_dir.name, 'Gaia xmatch ->', out_gaia)
        else:
            print('[POST][WARN]', tile_dir.name, 'Gaia CSV missing or lacks RA/Dec → skipped')
    except Exception as e:
        print('[POST][WARN]', tile_dir.name, 'Gaia xmatch failed:', e)

    # PS1 independent
    try:
        if ps1_csv.exists() and _csv_has_radec(ps1_csv):
            out_ps1 = xdir / 'sex_ps1_xmatch.csv'
            xmatch_sextractor_with_ps1(sex_csv, ps1_csv, out_ps1, radius_arcsec=radius_arcsec)
            print('[POST]', tile_dir.name, 'PS1  xmatch ->', out_ps1)
        else:
            print('[POST][WARN]', tile_dir.name, 'PS1 CSV missing or lacks RA/Dec → skipped')
    except Exception as e:
        print('[POST][WARN]', tile_dir.name, 'PS1 xmatch failed:', e)

# -----------------------------------------------------------
# Commands
# -----------------------------------------------------------

def cmd_one(args: argparse.Namespace) -> int:
    run_dir = _build_run_dir(Path(args.workdir) if args.workdir else None)
    lg = dl.configure_logger(run_dir / 'logs')
    out_raw = run_dir / 'raw'; out_raw.mkdir(parents=True, exist_ok=True)

    fits = dl.fetch_skyview_dss(args.ra, args.dec, size_arcmin=args.size_arcmin,
                                survey=args.survey, pixel_scale_arcsec=args.pixel_scale_arcsec,
                                out_dir=out_raw, basename=None, logger=lg)
    td = _tile_dir(run_dir, Path(fits).stem)

    try:
        p1, psf, p2 = run_psf_two_pass(fits, td, config_root='configs')
    except ToolMissingError as e:
        print('[ERROR]', e)
        return 2

    export_and_summarize(p2, td, export=args.export, histogram_col=args.hist_col)

    # Online external fetch (Gaia via CDS/VizieR, PS1 via MAST)
    radius_arcmin = args.size_arcmin * (2 ** 0.5) * 0.5
    try:
        fetch_gaia_neighbourhood(td, args.ra, args.dec, radius_arcmin)
    except Exception as e:
        print('[POST][WARN]', td.name, 'Gaia fetch failed:', e)

    try:
        if os.getenv('VASCO_DISABLE_PS1'):
            print('[POST][INFO]', td.name, 'PS1 disabled by env — skipping fetch')
        else:
            fetch_ps1_neighbourhood(td, args.ra, args.dec, radius_arcmin)
    except Exception as e:
        print('[POST][WARN]', td.name, 'PS1 fetch failed:', e)

    # Post-xmatch (resilient)
    try:
        _post_xmatch_tile(td, p2, radius_arcsec=2.0)
    except Exception as e:
        print('[POST][WARN] xmatch failed for', td.name, ':', e)

    results = [{'tile': Path(fits).stem, 'pass1': p1, 'psf': psf, 'pass2': p2}]
    counts = {'planned': 1, 'downloaded': 1, 'processed': 1}
    missing: list[dict] = []

    _write_json(run_dir / 'RUN_INDEX.json', results)
    _write_json(run_dir / 'RUN_COUNTS.json', counts)
    _write_json(run_dir / 'RUN_MISSING.json', missing)
    _write_overview(run_dir, counts, results, missing)

    print('Run directory:', run_dir)
    print('Planned tiles:', counts['planned'], 'Downloaded:', counts['downloaded'], 'Processed:', counts['processed'])
    return 0


def cmd_tess(args: argparse.Namespace) -> int:
    run_dir = _build_run_dir(Path(args.workdir) if args.workdir else None)
    lg = dl.configure_logger(run_dir / 'logs')
    out_raw = run_dir / 'raw'; out_raw.mkdir(parents=True, exist_ok=True)

    centers = dl.tessellate_centers(args.center_ra, args.center_dec,
                                    width_arcmin=args.width_arcmin, height_arcmin=args.height_arcmin,
                                    tile_radius_arcmin=args.tile_radius_arcmin, overlap_arcmin=args.overlap_arcmin)
    planned = len(centers)

    fits_list = dl.fetch_many(centers, size_arcmin=args.size_arcmin, survey=args.survey,
                              pixel_scale_arcsec=args.pixel_scale_arcsec, out_dir=out_raw, logger=lg)
    downloaded = len(fits_list)

    results: list[Dict[str, Any]] = []

    for fp in fits_list:
        stem = Path(fp).stem
        td = _tile_dir(run_dir, stem)
        try:
            p1, psf, p2 = run_psf_two_pass(fp, td, config_root='configs')
        except ToolMissingError as e:
            print('[ERROR]', e)
            continue

        export_and_summarize(p2, td, export=args.export, histogram_col=args.hist_col)

        # Online external fetch per tile
        radius_arcmin = args.size_arcmin * (2 ** 0.5) * 0.5
        try:
            parts = stem.split('_')
            ra_t = float(parts[1]); dec_t = float(parts[2])
        except Exception:
            ra_t, dec_t = centers[0]

        try:
            fetch_gaia_neighbourhood(td, ra_t, dec_t, radius_arcmin)
        except Exception as e:
            print('[POST][WARN]', td.name, 'Gaia fetch failed:', e)

        try:
            if os.getenv('VASCO_DISABLE_PS1'):
                print('[POST][INFO]', td.name, 'PS1 disabled by env — skipping fetch')
            else:
                fetch_ps1_neighbourhood(td, ra_t, dec_t, radius_arcmin)
        except Exception as e:
            print('[POST][WARN]', td.name, 'PS1 fetch failed:', e)

        try:
            _post_xmatch_tile(td, p2, radius_arcsec=2.0)
        except Exception as e:
            print('[POST][WARN] xmatch failed for', td.name, ':', e)

        results.append({'tile': stem, 'pass1': p1, 'psf': psf, 'pass2': p2})

    processed = len(results)
    processed_stems = {rec['tile'] for rec in results}

    missing: list[dict] = []
    for ra, dec in centers:
        exp_stem = _expected_stem(ra, dec, args.survey, args.size_arcmin)
        if exp_stem not in processed_stems:
            missing.append({'ra': float(ra), 'dec': float(dec), 'expected_stem': exp_stem})

    counts = {'planned': planned, 'downloaded': downloaded, 'processed': processed}

    _write_json(run_dir / 'RUN_INDEX.json', results)
    _write_json(run_dir / 'RUN_COUNTS.json', counts)
    _write_json(run_dir / 'RUN_MISSING.json', missing)
    _write_overview(run_dir, counts, results, missing)

    print('Run directory:', run_dir)
    print('Planned tiles:', planned, 'Downloaded:', downloaded, 'Processed:', processed)
    if missing:
        print(f"Missing tiles: {len(missing)} (see RUN_MISSING.json / RUN_OVERVIEW.md)")
    return 0

# -----------------------------------------------------------
# retry-missing
# -----------------------------------------------------------

def _retry_sleep(attempt: int, base: float, cap: float) -> None:
    import random, time as _t
    delay = min(cap, base * (2 ** max(0, attempt - 1)))
    delay *= (0.8 + 0.4 * random.random())
    _t.sleep(delay)

def cmd_retry_missing(args: argparse.Namespace) -> int:
    run_dir = Path(args.run_dir).resolve()
    if not run_dir.exists():
        print('[ERROR] run dir not found:', run_dir)
        return 2

    counts_path  = run_dir / 'RUN_COUNTS.json'
    missing_path = run_dir / 'RUN_MISSING.json'
    index_path   = run_dir / 'RUN_INDEX.json'

    if not missing_path.exists():
        print('[INFO] No RUN_MISSING.json found. Nothing to retry.')
        print('Run directory:', run_dir)
        return 0

    try:
        counts  = _read_json(counts_path)  if counts_path.exists() else {'planned':0,'downloaded':0,'processed':0}
        missing = _read_json(missing_path)
        results = _read_json(index_path)   if index_path.exists()  else []
    except Exception as e:
        print('[ERROR] cannot read run artifacts:', e)
        return 2

    lg = dl.configure_logger(run_dir / 'logs')
    out_raw = run_dir / 'raw'; out_raw.mkdir(parents=True, exist_ok=True)

    recovered: list[dict] = []
    still_missing: list[dict] = []

    for rec in missing:
        ra = float(rec['ra']); dec = float(rec['dec'])
        ok = False; fp = None
        for attempt in range(1, args.attempts + 1):
            try:
                fp = dl.fetch_skyview_dss(ra, dec, size_arcmin=args.size_arcmin, survey=args.survey,
                                          pixel_scale_arcsec=args.pixel_scale_arcsec, out_dir=out_raw, logger=lg)
                ok = True; break
            except Exception as e:
                print(f"[WARN] Retry {attempt}/{args.attempts} failed for RA={ra:.6f} Dec={dec:.6f}: {e}")
            if attempt < args.attempts:
                _retry_sleep(attempt, args.backoff_base, args.backoff_cap)
        if not ok or fp is None:
            still_missing.append(rec); continue

        stem = Path(fp).stem
        td = _tile_dir(run_dir, stem)
        try:
            p1, psf, p2 = run_psf_two_pass(fp, td, config_root='configs')
            export_and_summarize(p2, td, export=args.export, histogram_col=args.hist_col)

            # Online fetch + post-xmatch
            radius_arcmin = args.size_arcmin * (2 ** 0.5) * 0.5
            try:
                parts = stem.split('_')
                ra_t = float(parts[1]); dec_t = float(parts[2])
            except Exception:
                ra_t, dec_t = ra, dec

            try:
                fetch_gaia_neighbourhood(td, ra_t, dec_t, radius_arcmin)
            except Exception as e:
                print('[POST][WARN]', td.name, 'Gaia fetch failed:', e)
            try:
                if os.getenv('VASCO_DISABLE_PS1'):
                    print('[POST][INFO]', td.name, 'PS1 disabled by env — skipping fetch')
                else:
                    fetch_ps1_neighbourhood(td, ra_t, dec_t, radius_arcmin)
            except Exception as e:
                print('[POST][WARN]', td.name, 'PS1 fetch failed:', e)
            try:
                _post_xmatch_tile(td, p2, radius_arcsec=2.0)
            except Exception as e:
                print('[POST][WARN] xmatch failed for', td.name, ':', e)

            results.append({'tile': stem, 'pass1': p1, 'psf': psf, 'pass2': p2})
            recovered.append({'ra': ra, 'dec': dec, 'expected_stem': stem})
        except ToolMissingError as e:
            print('[ERROR] Tools missing for', stem, ':', e); still_missing.append(rec)
        except Exception as e:
            print('[ERROR] Processing failed for', stem, ':', e); still_missing.append(rec)

    prev_processed  = int(counts.get('processed',  0) or 0)
    prev_downloaded = int(counts.get('downloaded', 0) or 0)
    counts['processed']  = prev_processed  + len(recovered)
    counts['downloaded'] = prev_downloaded + len(recovered)

    _write_json(index_path, results)
    _write_json(counts_path, counts)
    _write_json(missing_path, still_missing)
    _write_overview(run_dir, counts, results, still_missing)

    print('Run directory:', run_dir)
    print(f"Recovered tiles: {len(recovered)} Remaining missing: {len(still_missing)}")
    return 0

# -----------------------------------------------------------
# CLI
# -----------------------------------------------------------

def main(argv: List[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog='vasco.cli_pipeline', description='VASCO pipeline orchestrator (download + 2-pass + export + QA + post-xmatch)')
    sub = p.add_subparsers(dest='cmd')

    one = sub.add_parser('one2pass', help='One RA/Dec -> download -> two-pass pipeline (auto fetch + post-xmatch)')
    one.add_argument('--ra', type=float, required=True)
    one.add_argument('--dec', type=float, required=True)
    one.add_argument('--size-arcmin', type=float, default=60.0)
    one.add_argument('--survey', default='dss1-red')
    one.add_argument('--pixel-scale-arcsec', type=float, default=1.7)
    one.add_argument('--export', choices=['none','csv','parquet','both'], default='csv')
    one.add_argument('--hist-col', default='FWHM_IMAGE')
    one.add_argument('--workdir', default=None)
    one.set_defaults(func=cmd_one)

    tess = sub.add_parser('tess2pass', help='Tessellate region and run two-pass pipeline per tile (auto fetch + post-xmatch)')
    tess.add_argument('--center-ra', type=float, required=True)
    tess.add_argument('--center-dec', type=float, required=True)
    tess.add_argument('--width-arcmin', type=float, required=True)
    tess.add_argument('--height-arcmin', type=float, required=True)
    tess.add_argument('--tile-radius-arcmin', type=float, default=30.0)
    tess.add_argument('--overlap-arcmin', type=float, default=0.0)
    tess.add_argument('--size-arcmin', type=float, default=60.0)
    tess.add_argument('--survey', default='dss1-red')
    tess.add_argument('--pixel-scale-arcsec', type=float, default=1.7)
    tess.add_argument('--export', choices=['none','csv','parquet','both'], default='csv')
    tess.add_argument('--hist-col', default='FWHM_IMAGE')
    tess.add_argument('--workdir', default=None)
    tess.set_defaults(func=cmd_tess)

    rt = sub.add_parser('retry-missing', help='Retry tiles for an existing run (auto fetch + post-xmatch)')
    rt.add_argument('run_dir', help='Existing run directory (data/runs/run-YYYYMMDD_HHMMSS)')
    rt.add_argument('--survey', default='dss1-red')
    rt.add_argument('--size-arcmin', type=float, default=60.0)
    rt.add_argument('--pixel-scale-arcsec', type=float, default=1.7)
    rt.add_argument('--attempts', type=int, default=4)
    rt.add_argument('--backoff-base', type=float, default=1.0)
    rt.add_argument('--backoff-cap', type=float, default=8.0)
    rt.add_argument('--export', choices=['none','csv','parquet','both'], default='csv')
    rt.add_argument('--hist-col', default='FWHM_IMAGE')
    rt.set_defaults(func=cmd_retry_missing)

    args = p.parse_args(argv)
    if hasattr(args, 'func'):
        return args.func(args)
    p.print_help()
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
