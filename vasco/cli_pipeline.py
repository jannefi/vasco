from __future__ import annotations
import argparse, json, time, subprocess, os
from pathlib import Path
from typing import List, Dict, Any, Tuple
from . import downloader as dl
from .pipeline import run_psf_two_pass, ToolMissingError, _ensure_tool
from .exporter3 import export_and_summarize
# External catalog fetchers (existing)
from vasco.external_fetch_online import (
    fetch_gaia_neighbourhood,
    fetch_ps1_neighbourhood,
)
# USNO-B1.0 via VizieR (Astroquery)
from vasco.external_fetch_usnob_vizier import fetch_usnob_neighbourhood
# STILTS xmatch helpers (Gaia/PS1) - local backend
from vasco.mnras.xmatch_stilts import (
    xmatch_sextractor_with_gaia,
    xmatch_sextractor_with_ps1,
)
# STILTS CDS backend
from vasco.utils.cdsskymatch import cdsskymatch, StiltsNotFound
# Sexagesimal parsers
from .utils.coords import parse_ra as _parse_ra, parse_dec as _parse_dec

# ----------------------
# Helpers
# ----------------------

# --- begin: unit-tolerant within-5" validator for CDS x-match outputs ---

from pathlib import Path

def _ensure_tool_cli(tool: str) -> None:
    """Local ensure_tool to avoid imports; no-op if stilts is present."""
    import shutil as _sh
    if _sh.which(tool) is None:
        raise RuntimeError(f"Required tool '{tool}' not found in PATH.")

def _validate_within_5_arcsec_unit_tolerant(xmatch_csv: Path) -> Path:
    """
    Create <stem>_within5arcsec.csv keeping only rows within 5 arcsec.

    Logic:
    - If 'angDist' exists: try treating it as ARCSECONDS (angDist<=5).
      If that yields 0 rows, fallback to DEGREES (3600*angDist<=5).
    - Else: compute separation via skyDistanceDegrees(ALPHA_J2000,DELTA_J2000,<ext_ra>,<ext_dec>) and select <=5".
    """
    _ensure_tool_cli('stilts')
    import csv, subprocess

    xmatch_csv = Path(xmatch_csv)
    out = xmatch_csv.with_name(xmatch_csv.stem + '_within5arcsec.csv')

    # Inspect header
    with open(xmatch_csv, newline='') as f:
        header = next(csv.reader(f), [])
    cols = set(header)

    def _write_empty():
        subprocess.run(
            ['stilts', 'tpipe', f'in={str(xmatch_csv)}',
             'cmd=select false', f'out={str(out)}', 'ofmt=csv'],
            check=True
        )
        return out

    # Case A: angDist present
    if 'angDist' in cols:
        # Try arcseconds first
        p = subprocess.run(
            ['stilts','tpipe', f'in={str(xmatch_csv)}', 'cmd=select angDist<=5', 'omode=count'],
            capture_output=True, text=True
        )
        try:
            # 'omode=count' prints "columns: X   rows: Y" or just "Y"
            cnt_text = (p.stdout or '0').strip().split()  # robust parsing
            c = int(cnt_text[-1]) if cnt_text else 0
        except Exception:
            c = 0

        if c > 0:
            subprocess.run(
                ['stilts','tpipe', f'in={str(xmatch_csv)}',
                 'cmd=select angDist<=5', f'out={str(out)}', 'ofmt=csv'],
                check=True
            )
            return out

        # Fallback: treat as degrees
        subprocess.run(
            ['stilts','tpipe', f'in={str(xmatch_csv)}',
             'cmd=select 3600*angDist<=5', f'out={str(out)}', 'ofmt=csv'],
            check=True
        )
        return out

    # Case B: compute from RA/Dec columns if possible
    # Prefer common external RA/Dec column pairs; adapt if your CDS schema names differ.
    for a, b in [('ra','dec'), ('RAJ2000','DEJ2000'), ('RA_ICRS','DE_ICRS'), ('RA','DEC')]:
        if a in cols and b in cols:
            cmd = ("cmd=addcol angDist_arcsec "
                   f"3600*skyDistanceDegrees(ALPHA_J2000,DELTA_J2000,{a},{b}); "
                   "select angDist_arcsec<=5")
            subprocess.run(
                ['stilts','tpipe', f'in={str(xmatch_csv)}', cmd, f'out={str(out)}', 'ofmt=csv'],
                check=True
            )
            return out

    # No usable columns → empty
    return _write_empty()

# --- end: unit-tolerant within-5" validator for CDS x-match outputs ---


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

# ----------------------
# LDAC → CSV export (robust)
# ----------------------

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

# ----------------------
# CSV header RA/Dec detection
# ----------------------

def _csv_has_radec(csv_path: Path) -> bool:
    import csv
    try:
        with open(csv_path, newline='') as f:
            hdr = next(csv.reader(f))
            cols = {h.strip() for h in hdr}
            for a, b in [
                ('ra','dec'), ('RA_ICRS','DE_ICRS'), ('RAJ2000','DEJ2000'),
                ('RA','DEC'), ('lon','lat'), ('raMean','decMean'), ('RAMean','DecMean'),
                ('ALPHA_J2000','DELTA_J2000')
            ]:
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
            pairs = [
                ('ALPHA_J2000','DELTA_J2000'),
                ('RAJ2000','DEJ2000'),
                ('RA_ICRS','DE_ICRS'),
                ('ra','dec'), ('RA','DEC'), ('lon','lat'),
                ('raMean','decMean'), ('RAMean','DecMean')
            ]
            for a,b in pairs:
                if a in cols and b in cols:
                    return a,b
            return None
    except Exception:
        return None

# ----------------------
# Local post-xmatch (Gaia/PS1 + USNO-B)
# ----------------------

def _post_xmatch_tile(tile_dir, pass2_ldac, *, radius_arcsec: float = 5.0) -> None:
    tile_dir = Path(tile_dir)
    xdir = tile_dir / 'xmatch'
    xdir.mkdir(parents=True, exist_ok=True)
    sex_csv = _ensure_sextractor_csv(tile_dir, pass2_ldac)
    sex_cols = _detect_radec_columns(sex_csv)
    gaia_csv = tile_dir / 'catalogs' / 'gaia_neighbourhood.csv'
    ps1_csv = tile_dir / 'catalogs' / 'ps1_neighbourhood.csv'
    usnob_csv = tile_dir / 'catalogs' / 'usnob_neighbourhood.csv'
    # Gaia
    try:
        if gaia_csv.exists() and _csv_has_radec(gaia_csv):
            out_gaia = xdir / 'sex_gaia_xmatch.csv'
            xmatch_sextractor_with_gaia(sex_csv, gaia_csv, out_gaia, radius_arcsec=radius_arcsec)
            print('[POST]', tile_dir.name, 'Gaia xmatch ->', out_gaia)
        else:
            print('[POST][WARN]', tile_dir.name, 'Gaia CSV missing or lacks RA/Dec → skipped')
    except Exception as e:
        print('[POST][WARN]', tile_dir.name, 'Gaia xmatch failed:', e)
    # PS1
    try:
        if ps1_csv.exists() and _csv_has_radec(ps1_csv):
            out_ps1 = xdir / 'sex_ps1_xmatch.csv'
            xmatch_sextractor_with_ps1(sex_csv, ps1_csv, out_ps1, radius_arcsec=radius_arcsec)
            print('[POST]', tile_dir.name, 'PS1 xmatch ->', out_ps1)
        else:
            print('[POST][WARN]', tile_dir.name, 'PS1 CSV missing or lacks RA/Dec → skipped')
    except Exception as e:
        print('[POST][WARN]', tile_dir.name, 'PS1 xmatch failed:', e)
    # USNO-B (VizieR I/284): local STILTS-based xmatch using RAJ2000/DEJ2000
    try:
        if usnob_csv.exists() and _csv_has_radec(usnob_csv):
            usnob_cols = _detect_radec_columns(usnob_csv) or ('RAJ2000','DEJ2000')
            if sex_cols is None:
                sex_cols = ('ALPHA_J2000','DELTA_J2000')
            ra1, dec1 = sex_cols
            ra2, dec2 = usnob_cols
            out_usnob = xdir / 'sex_usnob_xmatch.csv'
            cmd = [
                'stilts', 'tskymatch2',
                f'in1={str(sex_csv)}', f'in2={str(usnob_csv)}',
                f'ra1={ra1}', f'dec1={dec1}', f'ra2={ra2}', f'dec2={dec2}',
                f'error={radius_arcsec}', 'join=1and2',
                f'out={str(out_usnob)}', 'ofmt=csv'
            ]
            subprocess.run(cmd, check=True)
            print('[POST]', tile_dir.name, 'USNO-B xmatch ->', out_usnob)
        else:
            print('[POST][WARN]', tile_dir.name, 'USNO-B CSV missing or lacks RA/Dec → skipped')
    except FileNotFoundError:
        print('[POST][WARN]', tile_dir.name, 'STILTS not found; USNO-B xmatch skipped')
    except Exception as e:
        print('[POST][WARN]', tile_dir.name, 'USNO-B xmatch failed:', e)

# ----------------------
# CDS (remote) xmatch backend using cdsskymatch
# ----------------------

def _cds_xmatch_tile(tile_dir, pass2_ldac, *, radius_arcsec: float = 5.0,
                     cds_gaia_table: str | None = None,
                     cds_ps1_table: str | None = None) -> None:
    tile_dir = Path(tile_dir)
    xdir = tile_dir / 'xmatch'
    xdir.mkdir(parents=True, exist_ok=True)
    sex_csv = _ensure_sextractor_csv(tile_dir, pass2_ldac)
    sex_cols = _detect_radec_columns(sex_csv) or ('ALPHA_J2000','DELTA_J2000')
    ra_col, dec_col = sex_cols
    # Gaia via CDS
    if cds_gaia_table:
        out_gaia = xdir / 'sex_gaia_xmatch_cdss.csv'
        try:
            cdsskymatch(sex_csv, out_gaia, ra=ra_col, dec=dec_col,
                cdstable=cds_gaia_table, radius_arcsec=radius_arcsec,
                find='best', ofmt='csv', omode='out')
            print('[POST][CDS]', tile_dir.name, 'Gaia xmatch ->', out_gaia)
            _validate_within_5_arcsec_unit_tolerant(out_gaia)
            time.sleep(45.0)  # courtesy delay to avoid immediate CDS overload
        except StiltsNotFound:
            print('[POST][WARN]', tile_dir.name, 'STILTS not found; CDS Gaia xmatch skipped')
        except Exception as e:
            print('[POST][WARN]', tile_dir.name, 'CDS Gaia xmatch failed:', e)
    else:
        print('[POST][INFO]', tile_dir.name, 'No cds_gaia_table provided; skipping CDS Gaia match')
    # PS1 via CDS
    if cds_ps1_table:
        out_ps1 = xdir / 'sex_ps1_xmatch_cdss.csv'
        try:
            cdsskymatch(sex_csv, out_ps1, ra=ra_col, dec=dec_col,
                        cdstable=cds_ps1_table, radius_arcsec=radius_arcsec,
                        find='best', ofmt='csv', omode='out')
            print('[POST][CDS]', tile_dir.name, 'PS1 xmatch ->', out_ps1)
            _validate_within_5_arcsec_unit_tolerant(out_ps1)
        # Optional pause after PS1 as well
        try:
            import os, time
            pause_sec = float(os.getenv('VASCO_CDS_PAUSE_SECONDS', '8'))
            if pause_sec > 0:
                time.sleep(pause_sec)
        except Exception:
            pass
        except StiltsNotFound:
            print('[POST][WARN]', tile_dir.name, 'STILTS not found; CDS PS1 xmatch skipped')
        except Exception as e:
            print('[POST][WARN]', tile_dir.name, 'CDS PS1 xmatch failed:', e)
    else:
        print('[POST][INFO]', tile_dir.name, 'No cds_ps1_table provided; skipping CDS PS1 match')


# ----------------------
# Validator: robust ≤5" writer with angDist fallback (compute from RA/Dec)
# ----------------------

# ----------------------
# Robust CDS runner: cdsskymatch with retry/backoff and blocksize
# ----------------------

def _run_cdsskymatch_retry(in_table_csv: Path, out_csv: Path, *, ra_col: str, dec_col: str,
                           cdstable: str, radius_arcsec: float = 5.0,
                           attempts: int = 6, base_delay: float = 3.0,
                           blocksize: int = 10000,
                           serviceurl: str | None = None) -> None:
    """Run STILTS cdsskymatch with retry/backoff and blocksize to mitigate CDS rate-limits.
    - attempts: number of retries (default 6)
    - base_delay: initial backoff base seconds (default 3.0)
    - blocksize: rows per upload chunk (default 10000)
    - serviceurl: optional CDS xmatch service URL
    """
    _ensure_tool('stilts')
    import subprocess, time as _t, random
    cmd = [
        'stilts', 'cdsskymatch',
        f'in={str(in_table_csv)}', f'ra={ra_col}', f'dec={dec_col}',
        f'cdstable={cdstable}', f'radius={radius_arcsec}',
        'find=best', 'compress=true', 'presort=true', f'blocksize={blocksize}',
        'omode=out', f'out={str(out_csv)}', 'ofmt=csv',
    ]
    if serviceurl:
        cmd.append(f'serviceurl={serviceurl}')
    last_err = None
    for i in range(1, attempts + 1):
        try:
            subprocess.run(cmd, check=True)
            return
        except subprocess.CalledProcessError as e:
            last_err = e
            msg = str(e)
            # Exponential backoff with jitter; extra delay if CDS queue saturated
            delay = base_delay * (1.7 ** (i - 1))
            delay *= 0.7 + 0.6 * random.random()
            if 'Too many jobs' in msg or 'Service Error' in msg:
                delay += 5.0
            print(f"[POST][WARN] cdsskymatch attempt {i}/{attempts} failed: {e}. Retrying in {delay:.1f}s...")
            _t.sleep(delay)
    raise RuntimeError(f"cdsskymatch failed after {attempts} attempts: {last_err}")

def _validate_within_5_arcsec(xmatch_csv: Path) -> Path:
    """Create validated CSV keeping only rows <= 5 arcsec.
    - If 'angDist' exists: count via omode=count (stdout) and write accordingly.
    - Else: compute angDist_arcsec = 3600*skyDistanceDegrees(ALPHA_J2000,DELTA_J2000, <ext_ra>, <ext_dec>)
      and filter <= 5. Auto-detect <ext_ra>/<ext_dec> from header.
    Always writes CSV (empty with header if zero rows). Appends summary.
    """
    _ensure_tool('stilts')
    import csv, subprocess
    out = xmatch_csv.with_name(xmatch_csv.stem + '_within5arcsec.csv')

    # Read header to detect columns
    with open(xmatch_csv, newline='') as f:
        rdr = csv.reader(f)
        header = next(rdr, [])
    cols = set(header)

    tile_dir = xmatch_csv.parent.parent

    def _write_empty():
        p = subprocess.run(['stilts','tpipe', f'in={str(xmatch_csv)}', 'cmd=select false', f'out={str(out)}', 'ofmt=csv'])
        if p.returncode != 0:
            raise RuntimeError(f"tpipe write empty failed: {p.stderr}")
        print(f"[POST][CDS] <=5\" filter wrote EMPTY {out} (rows=0)")
        # (summary disabled) tile_dir, xmatch_csv.name, out.name, 0, 0)

    # Case A: angDist present -> use count/write path
    if 'angDist' in cols:
        p1 = subprocess.run(['stilts','tpipe', f'in={str(xmatch_csv)}', 'cmd=select angDist<=5', 'omode=count'], capture_output=True, text=True)
        if p1.returncode != 0:
            print('[POST][WARN]', tile_dir.name, 'tpipe count arcsec failed:', (p1.stderr or '').strip())
            _write_empty(); return out
        try:
            c1 = int((p1.stdout or '0').strip())
        except Exception:
            c1 = 0
        if c1 > 0:
            p3 = subprocess.run(['stilts','tpipe', f'in={str(xmatch_csv)}', 'cmd=select angDist<=5', f'out={str(out)}', 'ofmt=csv'])
            if p3.returncode != 0:
                print('[POST][WARN]', tile_dir.name, 'tpipe write arcsec failed:', (p3.stderr or '').strip())
                _write_empty(); return out
            print(f"[POST][CDS] <=5\" arcsec filter wrote {out} (rows={c1})")
            # (summary disabled) tile_dir, xmatch_csv.name, out.name, c1, 0)
            return out
        else:
            _write_empty(); return out

    # Case B: angDist missing -> compute using RA/Dec pairs
    # Detect external RA/Dec columns
    ext_pairs = [
        ('ra','dec'), ('RAJ2000','DEJ2000'), ('RA_ICRS','DE_ICRS'), ('RA','DEC')
    ]
    ext_ra, ext_dec = None, None
    for a,b in ext_pairs:
        if a in cols and b in cols:
            ext_ra, ext_dec = a, b
            break
    if ext_ra is None:
        print('[POST][WARN]', tile_dir.name, 'No external RA/Dec columns found in', xmatch_csv.name, '— writing empty within5')
        _write_empty(); return out

    # Compute angDist_arcsec and count/write
    cmd_count = f"cmd=addcol angDist_arcsec 3600*skyDistanceDegrees(ALPHA_J2000,DELTA_J2000,{ext_ra},{ext_dec}); select angDist_arcsec<=5"
    p1 = subprocess.run(['stilts','tpipe', f'in={str(xmatch_csv)}', cmd_count, 'omode=count'], capture_output=True, text=True)
    if p1.returncode != 0:
        print('[POST][WARN]', tile_dir.name, 'tpipe count (computed) failed:', (p1.stderr or '').strip())
        _write_empty(); return out
    try:
        c2 = int((p1.stdout or '0').strip())
    except Exception:
        c2 = 0

    if c2 > 0:
        p3 = subprocess.run(['stilts','tpipe', f'in={str(xmatch_csv)}', cmd_count, f'out={str(out)}', 'ofmt=csv'])
        if p3.returncode != 0:
            print('[POST][WARN]', tile_dir.name, 'tpipe write (computed) failed:', (p3.stderr or '').strip())
            _write_empty(); return out
        print(f"[POST][CDS] <=5\" (computed) filter wrote {out} (rows={c2})")
        # (summary disabled) tile_dir, xmatch_csv.name, out.name, 0, c2)
        return out
    else:
        _write_empty(); return out

# ----------------------
# Sexagesimal helpers
# ----------------------

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

# ----------------------
# Commands
# ----------------------

def cmd_one(args: argparse.Namespace) -> int:
    run_dir = _build_run_dir(Path(args.workdir) if args.workdir else None)
    lg = dl.configure_logger(run_dir / 'logs')
    out_raw = run_dir / 'raw'; out_raw.mkdir(parents=True, exist_ok=True)
    ra = _to_float_ra(args.ra)
    dec = _to_float_dec(args.dec)
    # STScI-only download; strict POSS-I enforcement (skip non-POSS tiles)
    try:
        fits = dl.fetch_skyview_dss(ra, dec, size_arcmin=args.size_arcmin,
            survey=args.survey, pixel_scale_arcsec=args.pixel_scale_arcsec,
            out_dir=out_raw, basename=None, logger=lg)
    except RuntimeError as e:
        if 'Non-POSS plate returned by STScI' in str(e):
            print('[SKIP]', f'RA={ra:.6f}', f'Dec={dec:.6f}',
                  '-> non-POSS plate; tile omitted to preserve strict provenance.')
            results: list[dict] = []
            counts = {'planned': 1, 'downloaded': 0, 'processed': 0}
            missing = [{'ra': float(ra), 'dec': float(dec),
                       'expected_stem': _expected_stem(ra, dec, args.survey, args.size_arcmin)}]
            _write_json(run_dir / 'RUN_INDEX.json', results)
            _write_json(run_dir / 'RUN_COUNTS.json', counts)
            _write_json(run_dir / 'RUN_MISSING.json', missing)
            _write_overview(run_dir, counts, results, missing)
            print('Run directory:', run_dir)
            print('Planned tiles: 1 Downloaded: 0 Processed: 0 (non-POSS skipped)')
            return 0
        else:
            raise
    td = _tile_dir(run_dir, Path(fits).stem)
    try:
        p1, psf, p2 = run_psf_two_pass(fits, td, config_root='configs')
    except ToolMissingError as e:
        print('[ERROR]', e)
        return 2
    export_and_summarize(p2, td, export=args.export, histogram_col=args.hist_col)
    # Neighbourhood radius = inscribed circle of the square tile
    radius_arcmin = args.size_arcmin * (2 ** 0.5) * 0.5

    # Backend selection: local vs CDS
    backend = args.xmatch_backend
    if backend == 'local':
        # Fetch neighborhood catalogs
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
        # Local xmatch
        try:
            _post_xmatch_tile(td, p2, radius_arcsec=float(args.xmatch_radius_arcsec))
        except Exception as e:
            print('[POST][WARN] xmatch failed for', td.name, ':', e)
    elif backend == 'cds':
        # Remote CDS X-Match, no local neighbor fetch
        try:
            _cds_xmatch_tile(td, p2, radius_arcsec=float(args.xmatch_radius_arcsec),
                             cds_gaia_table=args.cds_gaia_table,
                             cds_ps1_table=args.cds_ps1_table)
        except Exception as e:
            print('[POST][WARN] CDS xmatch failed for', td.name, ':', e)
    else:
        print('[POST][WARN]', td.name, 'Unknown xmatch backend:', backend)

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
    center_ra = _to_float_ra(args.center_ra)
    center_dec = _to_float_dec(args.center_dec)
    centers = dl.tessellate_centers(center_ra, center_dec,
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
        # Neighbourhood radius
        radius_arcmin = args.size_arcmin * (2 ** 0.5) * 0.5
        # Derive RA/Dec from filename or fallback
        try:
            parts = stem.split('_')
            ra_t = float(parts[1]); dec_t = float(parts[2])
        except Exception:
            ra_t, dec_t = centers[0]
        backend = args.xmatch_backend
        if backend == 'local':
            # Fetch neighbor catalogs
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
                if os.getenv('VASCO_DISABLE_USNOB'):
                    print('[POST][INFO]', td.name, 'USNO-B disabled by env — skipping fetch')
                else:
                    fetch_usnob_neighbourhood(td, ra_t, dec_t, radius_arcmin)
                    print('[POST]', td.name, 'USNO-B (VizieR) -> catalogs/usnob_neighbourhood.csv')
            except Exception as e:
                print('[POST][WARN]', td.name, 'USNO-B fetch failed:', e)
            # Local xmatch
            try:
                _post_xmatch_tile(td, p2, radius_arcsec=float(args.xmatch_radius_arcsec))
            except Exception as e:
                print('[POST][WARN] xmatch failed for', td.name, ':', e)
        elif backend == 'cds':
            try:
                _cds_xmatch_tile(td, p2, radius_arcsec=float(args.xmatch_radius_arcsec),
                                 cds_gaia_table=args.cds_gaia_table,
                                 cds_ps1_table=args.cds_ps1_table)
            except Exception as e:
                print('[POST][WARN] CDS xmatch failed for', td.name, ':', e)
        else:
            print('[POST][WARN]', td.name, 'Unknown xmatch backend:', backend)
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

# ----------------------
# retry-missing command
# ----------------------

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
            radius_arcmin = args.size_arcmin * (2 ** 0.5) * 0.5
            try:
                parts = stem.split('_')
                ra_t = float(parts[1]); dec_t = float(parts[2])
            except Exception:
                ra_t, dec_t = ra, dec
            backend = args.xmatch_backend
            if backend == 'local':
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
                    if os.getenv('VASCO_DISABLE_USNOB'):
                        print('[POST][INFO]', td.name, 'USNO-B disabled by env — skipping fetch')
                    else:
                        fetch_usnob_neighbourhood(td, ra_t, dec_t, radius_arcmin)
                        print('[POST]', td.name, 'USNO-B (VizieR) -> catalogs/usnob_neighbourhood.csv')
                except Exception as e:
                    print('[POST][WARN]', td.name, 'USNO-B fetch failed:', e)
                try:
                    _post_xmatch_tile(td, p2, radius_arcsec=float(args.xmatch_radius_arcsec))
                except Exception as e:
                    print('[POST][WARN] xmatch failed for', td.name, ':', e)
            elif backend == 'cds':
                try:
                    _cds_xmatch_tile(td, p2, radius_arcsec=float(args.xmatch_radius_arcsec),
                                     cds_gaia_table=args.cds_gaia_table,
                                     cds_ps1_table=args.cds_ps1_table)
                except Exception as e:
                    print('[POST][WARN] CDS xmatch failed for', td.name, ':', e)
            else:
                print('[POST][WARN]', td.name, 'Unknown xmatch backend:', backend)
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
    print(f"Recovered tiles: {len(recovered)} Remaining missing: {len(still_missing)}")
    return 0

# ----------------------
# CLI
# ----------------------

def main(argv: List[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog='vasco.cli_pipeline', description='VASCO pipeline orchestrator (download + 2-pass + export + QA + xmatch: local/CDS)')
    sub = p.add_subparsers(dest='cmd')
    one = sub.add_parser('one2pass', help='One RA/Dec -> download -> two-pass pipeline (post-xmatch backend selectable)')
    one.add_argument('--ra', type=str, required=True, help='RA in decimal deg or sexagesimal hh:mm:ss.s')
    one.add_argument('--dec', type=str, required=True, help='Dec in decimal deg or sexagesimal ±dd:mm:ss.s')
    one.add_argument('--size-arcmin', type=float, default=60.0)
    one.add_argument('--survey', default='dss1-red')
    one.add_argument('--pixel-scale-arcsec', type=float, default=1.7)
    one.add_argument('--export', choices=['none','csv','parquet','both'], default='csv')
    one.add_argument('--hist-col', default='FWHM_IMAGE')
    one.add_argument('--workdir', default=None)
    # New: xmatch backend & options
    one.add_argument('--xmatch-backend', choices=['local','cds'], default='cds', help='Choose local (tskymatch2) or CDS (cdsskymatch) backend')
    one.add_argument('--xmatch-radius-arcsec', type=float, default=5.0, help='Cross-match radius in arcsec (default: 5.0)')
    one.add_argument('--cds-gaia-table', default=os.getenv('VASCO_CDS_GAIA_TABLE'), help='VizieR table ID for Gaia (CDS backend). Env VASCO_CDS_GAIA_TABLE respected.')
    one.add_argument('--cds-ps1-table', default=os.getenv('VASCO_CDS_PS1_TABLE'), help='VizieR table ID for PS1 (CDS backend). Env VASCO_CDS_PS1_TABLE respected.')
    one.set_defaults(func=cmd_one)

    tess = sub.add_parser('tess2pass', help='Tessellate region and run two-pass pipeline per tile (post-xmatch backend selectable)')
    tess.add_argument('--center-ra', type=str, required=True, help='Center RA in decimal deg or sexagesimal hh:mm:ss.s')
    tess.add_argument('--center-dec', type=str, required=True, help='Center Dec in decimal deg or sexagesimal ±dd:mm:ss.s')
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
    # New: xmatch backend & options
    tess.add_argument('--xmatch-backend', choices=['local','cds'], default='cds')
    tess.add_argument('--xmatch-radius-arcsec', type=float, default=5.0)
    tess.add_argument('--cds-gaia-table', default=os.getenv('VASCO_CDS_GAIA_TABLE'))
    tess.add_argument('--cds-ps1-table', default=os.getenv('VASCO_CDS_PS1_TABLE'))
    tess.set_defaults(func=cmd_tess)

    rt = sub.add_parser('retry-missing', help='Retry tiles for an existing run (post-xmatch backend selectable)')
    rt.add_argument('run_dir', help='Existing run directory (data/runs/run-YYYYMMDD_HHMMSS)')
    rt.add_argument('--survey', default='dss1-red')
    rt.add_argument('--size-arcmin', type=float, default=60.0)
    rt.add_argument('--pixel-scale-arcsec', type=float, default=1.7)
    rt.add_argument('--attempts', type=int, default=4)
    rt.add_argument('--backoff-base', type=float, default=1.0)
    rt.add_argument('--backoff-cap', type=float, default=8.0)
    rt.add_argument('--export', choices=['none','csv','parquet','both'], default='csv')
    rt.add_argument('--hist-col', default='FWHM_IMAGE')
    # New: xmatch backend & options
    rt.add_argument('--xmatch-backend', choices=['local','cds'], default='local')
    rt.add_argument('--xmatch-radius-arcsec', type=float, default=5.0)
    rt.add_argument('--cds-gaia-table', default=os.getenv('VASCO_CDS_GAIA_TABLE'))
    rt.add_argument('--cds-ps1-table', default=os.getenv('VASCO_CDS_PS1_TABLE'))
    rt.set_defaults(func=cmd_retry_missing)

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

