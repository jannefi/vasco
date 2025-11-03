from __future__ import annotations
import argparse, json, time
from pathlib import Path
from typing import List, Dict, Any

from . import downloader as dl
from .pipeline import run_psf_two_pass, ToolMissingError
from .exporter3 import export_and_summarize


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
    nl = '\n'
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
            lines.append(f"- RA={ra:.6f}  Dec={dec:.6f}  → expected `{stem}`")
        if len(missing) > 15:
            lines.append(f"… and {len(missing)-15} more missing tiles.")
        lines.append('')
    _write_text(run_dir / 'RUN_OVERVIEW.md', nl.join(lines) + nl)


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

# retry missing

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
    counts_path = run_dir / 'RUN_COUNTS.json'
    missing_path = run_dir / 'RUN_MISSING.json'
    index_path = run_dir / 'RUN_INDEX.json'
    if not missing_path.exists():
        print('[INFO] No RUN_MISSING.json found. Nothing to retry.')
        print('Run directory:', run_dir)
        return 0
    try:
        counts = _read_json(counts_path) if counts_path.exists() else {'planned':0,'downloaded':0,'processed':0}
        missing = _read_json(missing_path)
        results = _read_json(index_path) if index_path.exists() else []
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
            results.append({'tile': stem, 'pass1': p1, 'psf': psf, 'pass2': p2})
            recovered.append({'ra': ra, 'dec': dec, 'expected_stem': stem})
        except ToolMissingError as e:
            print('[ERROR] Tools missing for', stem, ':', e); still_missing.append(rec)
        except Exception as e:
            print('[ERROR] Processing failed for', stem, ':', e); still_missing.append(rec)
    prev_processed = int(counts.get('processed', 0) or 0)
    prev_downloaded = int(counts.get('downloaded', 0) or 0)
    counts['processed'] = prev_processed + len(recovered)
    counts['downloaded'] = prev_downloaded + len(recovered)
    _write_json(index_path, results)
    _write_json(counts_path, counts)
    _write_json(missing_path, still_missing)
    _write_overview(run_dir, counts, results, still_missing)
    print('Run directory:', run_dir)
    print(f"Recovered tiles: {len(recovered)}  Remaining missing: {len(still_missing)}")
    return 0


def main(argv: List[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog='vasco.cli_pipeline', description='VASCO pipeline orchestrator (download + 2-pass + export + QA)')
    sub = p.add_subparsers(dest='cmd')

    one = sub.add_parser('one2pass', help='One RA/Dec -> download -> two-pass pipeline')
    one.add_argument('--ra', type=float, required=True)
    one.add_argument('--dec', type=float, required=True)
    one.add_argument('--size-arcmin', type=float, default=60.0)
    one.add_argument('--survey', default='dss1-red')
    one.add_argument('--pixel-scale-arcsec', type=float, default=1.7)
    one.add_argument('--export', choices=['none','csv','parquet','both'], default='csv')
    one.add_argument('--hist-col', default='FWHM_IMAGE')
    one.add_argument('--workdir', default=None)
    one.set_defaults(func=cmd_one)

    tess = sub.add_parser('tess2pass', help='Tessellate region and run two-pass pipeline per tile')
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

    rt = sub.add_parser('retry-missing', help='Retry tiles listed in RUN_MISSING.json for a prior run directory')
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
