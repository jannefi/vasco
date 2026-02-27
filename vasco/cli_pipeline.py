# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, json, time, subprocess, os, shutil, math
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
# --- NEW imports for MNRAS modules (filters, spikes, HPM, buckets/report) ---
from astropy.table import Table
from vasco.mnras.filters_mnras import apply_extract_filters, apply_morphology_filters
from vasco.mnras.spikes import (
    fetch_bright_ps1, apply_spike_cuts,
    SpikeConfig, SpikeRuleConst, SpikeRuleLine
)
from vasco.mnras.hpm import backprop_gaia_row
from vasco.mnras.buckets import init_buckets, finalize
from vasco.mnras.report import write_summary
import csv as _csv
from vasco.wcsfix_early import ensure_wcsfix_catalog, WcsFixConfig

# --- helpers ---
def _ensure_tool_cli(tool: str) -> None:
    if shutil.which(tool) is None:
        raise RuntimeError(f"Required tool '{tool}' not found in PATH.")

def _validate_within5_arcsec_unit_tolerant(xmatch_csv: Path) -> Path:
    """
    Create a side-by-side CSV filtered to <=5 arcsec.
    Handles angDist in arcsec or degrees, and falls back to RA/Dec-based computation.
    Robust to empty/malformed input: writes an empty CSV with the same basename if needed.
    """
    import csv
    _ensure_tool_cli('stilts')
    xmatch_csv = Path(xmatch_csv)
    out = xmatch_csv.with_name(xmatch_csv.stem + '_within5arcsec.csv')
    # If input is empty/zero bytes, write an empty CSV and return
    try:
        if xmatch_csv.stat().st_size == 0:
            out.write_text('', encoding='utf-8')
            return out
    except FileNotFoundError:
        # Input doesn't exist -> produce empty output
        out.write_text('', encoding='utf-8')
        return out
    # Try to read header safely
    try:
        with xmatch_csv.open(newline='') as f:
            header = next(csv.reader(f), [])
    except Exception:
        out.write_text('', encoding='utf-8')
        return out
    cols = set(header)
    # Helper: write an empty output via stilts if possible, else python
    def _write_empty():
        try:
            subprocess.run(
                ['stilts', 'tpipe', f'in={str(xmatch_csv)}', 'cmd=select false',
                 f'out={str(out)}', 'ofmt=csv'],
                check=True
            )
        except Exception:
            out.write_text('', encoding='utf-8')
        return out
    # Case A: an 'angDist' column present (arcsec or degrees)
    if 'angDist' in cols:
        # Try direct arcsec first
        try:
            subprocess.run(
                ['stilts', 'tpipe', f'in={str(xmatch_csv)}', 'cmd=select angDist<=5',
                 f'out={str(out)}', 'ofmt=csv'],
                check=True
            )
            return out
        except subprocess.CalledProcessError:
            # Fallback: degrees -> convert to arcsec
            try:
                subprocess.run(
                    ['stilts', 'tpipe', f'in={str(xmatch_csv)}',
                     'cmd=select 3600*angDist<=5', f'out={str(out)}', 'ofmt=csv'],
                    check=True
                )
                return out
            except Exception:
                return _write_empty()
    # Case B: derive separation from RA/Dec columns
    for a,b in [('ra','dec'), ('RAJ2000','DEJ2000'), ('RA_ICRS','DE_ICRS'), ('RA','DEC')]:
        if a in cols and b in cols:
            cmd = ("cmd=addcol angDist_arcsec "
                   f"3600*skyDistanceDegrees(ALPHA_J2000,DELTA_J2000,{a},{b}); "
                   "select angDist_arcsec<=5")
            try:
                subprocess.run(
                    ['stilts','tpipe', f'in={str(xmatch_csv)}', cmd,
                     f'out={str(out)}', 'ofmt=csv'],
                    check=True
                )
                return out
            except Exception:
                return _write_empty()
    # No usable columns -> produce empty output
    return _write_empty()

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
            sel_keys = ['SURVEY','PLATEID','PLATE-ID','PLATE','DATE-OBS','RA','DEC','EQUINOX','MJD-OBS',
                        'NAXIS1','NAXIS2','CD1_1','CD1_2','CD2_1','CD2_2','CDELT1','CDELT2',
                        'CRPIX1','CRPIX2','CRVAL1','CRVAL2']
            selected = {k: (str(hdr.get(k)) if hdr.get(k) is not None else None) for k in sel_keys}
            full = {str(k): (str(hdr.get(k)) if hdr.get(k) is not None else None) for k in hdr.keys()}
            payload = {'fits_file': fits_path.name, 'selected': selected, 'header': full}
            sidecar.write_text(_json.dumps(payload, indent=2), encoding='utf-8')
    except Exception:
        sidecar.write_text(json.dumps({'fits_file': fits_path.name, 'error': 'header_read_failed'}),
                           encoding='utf-8')
    return sidecar

def _enforce_possi_e_or_skip(fits_path: Path, logger) -> None:
    """Check FITS SURVEY and skip (delete + raise) if not POSSI-E; else write header sidecar."""
    survey = _fits_survey(fits_path)
    if survey != 'POSSI-E':
        try:
            msg = f"[STEP1][FILTER] Non-POSS plate; SURVEY={survey!r} — file will be discarded"
            if logger: logger.info(msg)
            else: print(msg)
        finally:
            try:
                Path(fits_path).unlink(missing_ok=True)
            except Exception:
                pass
        raise RuntimeError(f"Non-POSS plate returned by STScI: SURVEY={survey!r}")
    else:
        sidecar = _write_fits_header_json(Path(fits_path))
        if logger: logger.info(f"[STEP1][HEADER] Wrote FITS header sidecar: {sidecar.name}")
        else: print(f"[STEP1][HEADER] Wrote FITS header sidecar: {sidecar.name}")

# --- small helpers ---
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

def _read_bright_cache(path: Path):
    import csv as _csv
    from vasco.mnras.spikes import BrightStar
    out = []
    if not path.exists() or path.stat().st_size == 0:
        return out
    with path.open(newline='', encoding='utf-8') as f:
        r = _csv.DictReader(f)
        for row in r:
            try:
                out.append(BrightStar(
                    ra=float(row['ra']),
                    dec=float(row['dec']),
                    rmag=float(row['rmag']),
                ))
            except Exception:
                continue
    return out

def _write_bright_cache(path: Path, bright):
    import csv as _csv
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='', encoding='utf-8') as f:
        w = _csv.DictWriter(f, fieldnames=['ra','dec','rmag'])
        w.writeheader()
        for b in bright:
            w.writerow({'ra': b.ra, 'dec': b.dec, 'rmag': b.rmag})

# --- NEW: MNRAS integration helpers ---

def _apply_mnras_filters_and_spikes(tile_dir: Path, sex_csv: Path, buckets: dict) -> Path:
    """
    Apply SNR/FLAGS + morphology filters and bright-star/diffraction spike cuts
    to a COPY of the SExtractor CSV before any cross-matching.
    Returns: catalogs/sextractor_pass2.filtered.csv
    """
    from astropy.table import Table
    import csv as _csv

    tile_dir = Path(tile_dir)
    sex_csv = Path(sex_csv)
    out_csv = sex_csv.with_name('sextractor_pass2.filtered.csv')

    # Load original (unfiltered)
    try:
        tab = Table.read(str(sex_csv), format='ascii.csv')
    except Exception:
        out_csv.write_text('', encoding='utf-8')
        return out_csv

    n0 = len(tab)

    # 1) FLAGS==0 & SNR_WIN>=30
    tab = apply_extract_filters(tab, cfg={'flags_equal': 0, 'snr_win_min': 30.0})

    # 2) Morphology gates (PSF-aware)
    tab = apply_morphology_filters(
        tab,
        cfg={
            'fwhm_lower': 2.0,
            'fwhm_upper': 7.0,
            'elongation_lt': 1.3,
            'spread_model_min': -0.002,
            # keep the paper-style robust clipping enabled (it defaults True in filters_mnras.py)
            'sigma_clip': True,
            'sigma_k': 2.0,
            # enable the paper’s pixel-extent guards when columns exist (your configs do output them)
            'extent_delta_lt': 2.0,
            'extent_min': 1.0,
        }
    )

    n1 = len(tab)
    buckets['morphology_rejected'] += max(0, n0 - n1)
    # Early exit: if nothing survives morphology, don't fetch bright-star catalogs
    if n1 == 0:
        # Preserve a valid CSV artifact (empty) and a valid rejected artifact (empty)
        out_csv.write_text('', encoding='utf-8')
        rej_path = tile_dir / 'catalogs' / 'sextractor_spike_rejected.csv'
        rej_path.write_text('', encoding='utf-8')
        return out_csv

    # Write intermediate
    tab.write(str(out_csv), format='ascii.csv', overwrite=True)

    # 3) Bright-star spike removal via PS1 (within ~35′, r<=16)
    center = _tile_center_from_index_or_name(tile_dir)
    bright = []
    if center:
        cache_path = (tile_dir / 'catalogs' / 'ps1_bright_stars_r16_rad35.csv')
        try:
            # Use cache if present
            if cache_path.exists() and cache_path.stat().st_size > 0:
                bright = _read_bright_cache(cache_path)
            else:
                bright = fetch_bright_ps1(
                    center[0], center[1],
                    radius_arcmin=35.0, rmag_max=16.0, mindetections=2
                )
                # Save cache for fast reruns
                _write_bright_cache(cache_path, bright)
        except Exception:
            bright = []

        with open(out_csv, newline='') as f:
            rdr = _csv.DictReader(f)
            rows = list(rdr)

        kept, rejected = apply_spike_cuts(
            rows, bright,
            SpikeConfig(rules=[
                SpikeRuleConst(const_max_mag=12.4),
                SpikeRuleLine(a=-0.09, b=15.3),  # slope per arsec
            ])
        )
        buckets['spikes_rejected'] += len(rejected)

        # Write final filtered rows (may be empty, header preserved)
        fieldnames = (kept[0].keys() if kept else (rows[0].keys() if rows else []))
        with open(out_csv, 'w', newline='') as fo:
            if fieldnames:
                w = _csv.DictWriter(fo, fieldnames=fieldnames)
                w.writeheader(); w.writerows(kept)
            else:
                fo.write('')

        # Diagnostics
        rej_path = tile_dir / 'catalogs' / 'sextractor_spike_rejected.csv'
        with rej_path.open('w', newline='') as fo:
            if rejected:
                w = _csv.DictWriter(fo, fieldnames=rejected[0].keys())
                w.writeheader(); w.writerows(rejected)
            else:
                fo.write('')

    return out_csv

    # HPM filtering helpers unchanged

def _sep_arcsec(ra1_deg: float, dec1_deg: float, ra2_deg: float, dec2_deg: float) -> float:
    ra1 = math.radians(ra1_deg); dec1 = math.radians(dec1_deg)
    ra2 = math.radians(ra2_deg); dec2 = math.radians(dec2_deg)
    s = 2*math.asin(math.sqrt(
        math.sin((dec2-dec1)/2)**2 +
        math.cos(dec1)*math.cos(dec2)*math.sin((ra2-ra1)/2)**2
    ))
    return math.degrees(s) * 3600.0

def _filter_hpm_gaia(xdir: Path, buckets: dict, poss_sep_arcsec: float = 5.0) -> None:
    """After Gaia xmatch, back-propagate Gaia positions to POSS epoch; flag HPM mismatches."""
    gx = xdir / 'sex_gaia_xmatch.csv'
    if not gx.exists():
        return
    with gx.open(newline='') as f:
        rdr = _csv.DictReader(f)
        rows = list(rdr)
    kept, flagged = [], []
    for row in rows:
        try:
            ra_bp, dec_bp = backprop_gaia_row(row, target_epoch=1950.0)
            poss_ra = float(row.get('ALPHA_J2000', row.get('ra', 'nan')))
            poss_dec = float(row.get('DELTA_J2000', row.get('dec', 'nan')))
            if any(map(lambda x: isinstance(x, float) and math.isnan(x),
                       [ra_bp, dec_bp, poss_ra, poss_dec])):
                kept.append(row); continue
            sep = _sep_arcsec(poss_ra, poss_dec, ra_bp, dec_bp)
            if sep <= poss_sep_arcsec:
                kept.append(row)
            else:
                r2 = dict(row); r2['hpm_sep_arcsec'] = f"{sep:.3f}"
                flagged.append(r2)
        except Exception:
            kept.append(row)
    buckets['hpm_objects'] += len(flagged)
    out_clean = xdir / 'sex_gaia_xmatch_hpmclean.csv'
    out_flag = xdir / 'sex_gaia_hpm_flagged.csv'
    if kept:
        with out_clean.open('w', newline='') as fo:
            w = _csv.DictWriter(fo, fieldnames=kept[0].keys())
            w.writeheader(); w.writerows(kept)
    else:
        out_clean.write_text('', encoding='utf-8')
    if flagged:
        with out_flag.open('w', newline='') as fo:
            w = _csv.DictWriter(fo, fieldnames=flagged[0].keys())
            w.writeheader(); w.writerows(flagged)
    else:
        out_flag.write_text('', encoding='utf-8')

# --- commands ---

def cmd_one(args: argparse.Namespace) -> int:
    """
    One-shot pipeline: 1+2+3 + export + xmatch, with deferred tile creation in step 1.
    Avoids materializing data/tiles/<tileid> on download errors (non-FITS/non-WCS/non-POSS).
    """
    # DO NOT pre-create tile directory; keep logger central (optional)
    run_dir = Path(args.workdir)  # do not call _build_run_dir here
    lg = dl.configure_logger(Path('./data/logs'))

    ra = _to_float_ra(args.ra)
    dec = _to_float_dec(args.dec)

    # --- STEP 1: download with deferral (downloader will stage & promote only on success)
    try:
        fits = dl.fetch_skyview_dss(
            ra, dec,
            size_arcmin=args.size_arcmin,
            survey=args.survey,
            pixel_scale_arcsec=args.pixel_scale_arcsec,
            out_dir=run_dir / 'raw',  # downloader creates this only on success
            logger=lg
        )
        # Enforce POSSI-E post-promotion; may unlink & raise if not POSSI-E
        _enforce_possi_e_or_skip(Path(fits), lg)

    except RuntimeError as e:
        # Non-POSS enforcement path keeps your original bookkeeping — but only
        # write RUN_* artifacts if the tile folder already exists.
        if 'Non-POSS plate returned by STScI' in str(e):
            print('[SKIP]', f'RA={ra:.6f}', f'Dec={dec:.6f}', '-> non-POSS; tile omitted.')
            counts = {'planned': 1, 'downloaded': 0, 'processed': 0, 'filtered_non_poss': 1}
            missing = [{
                'ra': float(ra),
                'dec': float(dec),
                'expected_stem': _expected_stem(ra, dec, args.survey, args.size_arcmin)
            }]
            if run_dir.exists():
                _write_json(run_dir / 'RUN_COUNTS.json', counts)
                _write_json(run_dir / 'RUN_MISSING.json', missing)
                _write_json(run_dir / 'RUN_INDEX.json', [])
                _write_overview(run_dir, counts, [], missing)
            return 0

        # For non-FITS / non-WCS / other failures: downloader already wrote error artifacts
        print('[STEP1][ERROR]', str(e))
        return 1

    # --- SUCCESS PATH ---
    # STEP 2 + STEP 3
    p1, _ = run_pass1(fits, run_dir, config_root='configs')
    psf = run_psfex(p1, run_dir, config_root='configs')
    p2 = run_pass2(fits, run_dir, psf, config_root='configs')

    # Exports & QA
    export_and_summarize(p2, run_dir, export=args.export, histogram_col=args.hist_col)

    # STEP 4: xmatch (local or CDS)
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
            _cds_xmatch_tile(
                run_dir, p2,
                radius_arcsec=float(args.xmatch_radius_arcsec),
                cds_gaia_table=args.cds_gaia_table,
                cds_ps1_table=args.cds_ps1_table,
                fallback_empty_use_raw=False  # one-shot path uses strict by default
            )
        except Exception as e:
            print('[POST][WARN] CDS xmatch failed for', run_dir.name, ':', e)
    else:
        print('[POST][WARN]', run_dir.name, 'Unknown xmatch backend:', backend)

    # Final run bookkeeping & overview (tile dir exists on success)
    results = [{'tile': Path(fits).stem, 'pass1': str(p1), 'psf': str(psf), 'pass2': str(p2)}]
    counts = {'planned': 1, 'downloaded': 1, 'processed': 1, 'filtered_non_poss': 0}
    _write_json(run_dir / 'RUN_INDEX.json', results)
    _write_json(run_dir / 'RUN_COUNTS.json', counts)
    _write_json(run_dir / 'RUN_MISSING.json', [])
    _write_overview(run_dir, counts, results, [])
    print('Run directory:', run_dir)
    return 0


# --- helpers for run bookkeeping ---
def _build_run_dir(base: str | Path | None = None) -> Path:
    base = Path(base) if base else Path('data') / 'runs'
    base.mkdir(parents=True, exist_ok=True)
    return base

def _write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding='utf-8')

def _write_json(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, indent=2), encoding='utf-8')

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

def _csv_has_radec(csv_path: Path) -> bool:
    import csv
    try:
        with open(csv_path, newline='') as f:
            hdr = next(csv.reader(f))
        cols = {h.strip() for h in hdr}
        for a,b in [('ra','dec'), ('RA_ICRS','DE_ICRS'), ('RAJ2000','DEJ2000'),
                    ('RA','DEC'), ('lon','lat'), ('raMean','decMean'), ('RAMean','DecMean'),
                    ('ALPHA_J2000','DELTA_J2000'), ('ALPHAWIN_J2000','DELTAWIN_J2000'),
                    ('X_WORLD','Y_WORLD')]:
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
        pairs = [('ALPHA_J2000','DELTA_J2000'), ('ALPHAWIN_J2000','DELTAWIN_J2000'),
                 ('X_WORLD','Y_WORLD'), ('RAJ2000','DEJ2000'), ('RA_ICRS','DE_ICRS'),
                 ('ra','dec'), ('RA','DEC')]
        for a,b in pairs:
            if a in cols and b in cols:
                return a,b
        return None
    except Exception:
        return None

# UPDATED ensure: schema + rows aware

def _ensure_sextractor_csv(tile_dir: Path, pass2_ldac: str | Path) -> Path:
    """
    Ensure catalogs/sextractor_pass2.csv exists and is usable:
      - non-empty
      - contains RA/Dec
      - has at least one data row
      - includes required columns used by filters
    If invalid, re-extract from LDAC using STILTS (multiple HDUs).
    """
    _ensure_tool_cli('stilts')
    import csv

    tile_dir = Path(tile_dir)
    pass2_ldac = Path(pass2_ldac)

    cat_dir = tile_dir / 'catalogs'
    cat_dir.mkdir(parents=True, exist_ok=True)
    sex_csv = cat_dir / 'sextractor_pass2.csv'
    probe = cat_dir / '_probe.csv'

    REQUIRED_COLS = {
        'ALPHA_J2000', 'DELTA_J2000',   # coordinates
        'FLAGS', 'SNR_WIN',             # extract-time
        'FWHM_IMAGE', 'ELONGATION',     # morphology
        'SPREAD_MODEL',                 # PSF-aware morphology
    }

    def _header(path: Path) -> List[str]:
        try:
            with path.open(newline='') as f:
                return [c.strip() for c in next(csv.reader(f), [])]
        except Exception:
            return []

    def _valid(path: Path) -> bool:
        try:
            if not path.exists() or path.stat().st_size == 0:
                return False
        except Exception:
            return False

        hdr = _header(path)
        if len(hdr) <= 2:
            return False
        if not _csv_has_radec(path):
            return False
        # at least one data row?
        try:
            with path.open(newline='') as f:
                rdr = csv.reader(f)
                next(rdr, None)
                if next(rdr, None) is None:
                    return False
        except Exception:
            return False
        # required schema
        cols = set(hdr)
        if not REQUIRED_COLS.issubset(cols):
            return False
        return True

    # Fast path
    if _valid(sex_csv):
        return sex_csv

    # Re-extract with multi-HDU probing
    hdu_tries = ['#LDAC_OBJECTS', '#2', '#1', '#0', '#3', '#4', '#5', '#6', '#7', '#8', '']
    for ext in hdu_tries:
        in_arg = f"in={str(pass2_ldac)}{ext}" if ext else f"in={str(pass2_ldac)}"
        try:
            subprocess.run(
                ['stilts', 'tcopy', in_arg, f'out={str(probe)}', 'ofmt=csv'],
                check=True, capture_output=True
            )
        except Exception:
            continue
        if _valid(probe):
            try:
                probe.replace(sex_csv)
            except Exception:
                shutil.copyfile(probe, sex_csv)
            return sex_csv

    # Last resort: ensure a placeholder exists
    try:
        sex_csv.write_text('', encoding='utf-8')
    except Exception:
        pass
    return sex_csv


def _post_xmatch_tile(tile_dir, pass2_ldac, *, radius_arcsec: float = 5.0) -> None:
    """
    Step4 (local backend): prepare catalogs, apply (current) MNRAS-ish filters/spike logic,
    then xmatch against Gaia/PS1/USNO.

    Updated behavior:
      - Ensures early WCSFIX canonical coordinates (RA_corr/Dec_corr) using local Gaia cache
        by generating catalogs/sextractor_pass2.wcsfix.csv and using it downstream.
      - Xmatch will automatically prefer RA_corr/Dec_corr if xmatch_stilts.py is updated to include them.
      - If WCSFIX cannot be fit, pipeline proceeds with raw coords and writes wcsfix_status.json.
    """
    tile_dir = Path(tile_dir)
    xdir = tile_dir / 'xmatch'
    xdir.mkdir(parents=True, exist_ok=True)

    # 1) Ensure base SExtractor CSV exists (big)
    sex_csv = _ensure_sextractor_csv(tile_dir, pass2_ldac)

    # 2) Apply MNRAS filters & spikes before any xmatch (current behavior kept)
    buckets = init_buckets()

    # 3) Ensure Gaia/PS1/USNO neighbourhood caches are present (fetch stage happens before this in cmd_step4_xmatch)
    gaia_csv = tile_dir / 'catalogs' / 'gaia_neighbourhood.csv'
    ps1_csv = tile_dir / 'catalogs' / 'ps1_neighbourhood.csv'
    usnob_csv = tile_dir / 'catalogs' / 'usnob_neighbourhood.csv'

    # 4) Early canonical coordinates (WCSFIX) using local Gaia cache
    #    - Write catalogs/sextractor_pass2.wcsfix.csv (adds RA_corr/Dec_corr)
    #    - If fails, continue with raw sex_csv; status is written to catalogs/wcsfix_status.json
    sex_for_downstream = sex_csv
    try:
        # Try to provide the tile center if we can (improves RA wrap behavior and stability)
        center = _tile_center_from_index_or_name(tile_dir)
        cfg = WcsFixConfig(
            bootstrap_radius_arcsec=float(os.getenv("VASCO_WCSFIX_BOOTSTRAP_ARCSEC", "5.0")),
            degree=int(os.getenv("VASCO_WCSFIX_DEGREE", "2")),
            min_matches=int(os.getenv("VASCO_WCSFIX_MIN_MATCHES", "20")),
        )
        if gaia_csv.exists() and gaia_csv.stat().st_size > 0:
            out_wcs, status = ensure_wcsfix_catalog(
                tile_dir,
                sex_csv,
                gaia_csv,
                center=center,
                cfg=cfg,
                force=bool(os.getenv("VASCO_WCSFIX_FORCE", "").strip()),
            )
            if status.get("ok"):
                sex_for_downstream = out_wcs
                print('[POST]', tile_dir.name, 'WCSFIX OK ->', out_wcs.name)
            else:
                print('[POST][INFO]', tile_dir.name, 'WCSFIX skipped/failed -> using raw coords:', status.get("reason"))
        else:
            print('[POST][INFO]', tile_dir.name, 'WCSFIX skipped: gaia_neighbourhood.csv missing/empty')
    except Exception as e:
        print('[POST][WARN]', tile_dir.name, 'WCSFIX error -> using raw coords:', e)

    # 5) Apply filters/spikes on the chosen catalog (raw or wcsfix-augmented)
    #    This writes catalogs/sextractor_pass2.filtered.csv (Astropy preserves extra columns if present).
    filtered_csv = _apply_mnras_filters_and_spikes(tile_dir, sex_for_downstream, buckets)
    sex_cols = _detect_radec_columns(filtered_csv) or ('ALPHA_J2000', 'DELTA_J2000')

    # Early exit: nothing survived filtering
    try:
        if not filtered_csv.exists() or filtered_csv.stat().st_size == 0:
            write_summary(tile_dir, finalize(buckets), md_path='MNRAS_SUMMARY.md', json_path='MNRAS_SUMMARY.json')
            return
    except Exception:
        pass

    # 6) Gaia xmatch (local)
    if gaia_csv.exists() and _csv_has_radec(gaia_csv):
        out_gaia = xdir / 'sex_gaia_xmatch.csv'
        xmatch_sextractor_with_gaia(filtered_csv, gaia_csv, out_gaia, radius_arcsec=radius_arcsec)
        print('[POST]', tile_dir.name, 'Gaia xmatch ->', out_gaia)

        # Existing lightweight HPM cleaning kept (not the paper’s 180′ sweep)
        _filter_hpm_gaia(xdir, buckets)

    # 7) PS1 xmatch (local)
    if ps1_csv.exists() and _csv_has_radec(ps1_csv):
        out_ps1 = xdir / 'sex_ps1_xmatch.csv'
        xmatch_sextractor_with_ps1(filtered_csv, ps1_csv, out_ps1, radius_arcsec=radius_arcsec)
        print('[POST]', tile_dir.name, 'PS1 xmatch ->', out_ps1)

    # 8) USNO-B xmatch (local; now intended to be mandatory in intent-mode)
    #    We keep the existing logic, but if missing, we record it in the summary buckets as an "incomplete tile".
    usnob_missing = False
    try:
        if usnob_csv.exists() and _csv_has_radec(usnob_csv):
            usnob_cols = _detect_radec_columns(usnob_csv) or ('RAJ2000', 'DEJ2000')
            ra1, dec1 = sex_cols
            ra2, dec2 = usnob_cols
            out_usnob = xdir / 'sex_usnob_xmatch.csv'
            subprocess.run(
                ['stilts', 'tskymatch2',
                 f'in1={str(filtered_csv)}', f'in2={str(usnob_csv)}',
                 f'ra1={ra1}', f'dec1={dec1}', f'ra2={ra2}', f'dec2={dec2}',
                 f'error={radius_arcsec}', 'join=1and2',
                 f'out={str(out_usnob)}', 'ofmt=csv'],
                check=True
            )
            print('[POST]', tile_dir.name, 'USNO-B xmatch ->', out_usnob)
        else:
            usnob_missing = True
            print('[POST][WARN]', tile_dir.name, 'USNO-B missing/invalid; tile marked incomplete for optical veto')
    except FileNotFoundError:
        usnob_missing = True
        print('[POST][WARN]', tile_dir.name, 'STILTS not found; USNO-B skipped (tile incomplete)')
    except Exception as e:
        usnob_missing = True
        print('[POST][WARN]', tile_dir.name, 'USNO-B xmatch failed (tile incomplete):', e)

    # 9) Write per-tile summary
    #    If USNO-B is missing, stash a note in buckets so the summary makes it visible.
    if usnob_missing:
        try:
            buckets.setdefault('missing_inputs', 0)
            buckets['missing_inputs'] += 1
        except Exception:
            pass

    write_summary(tile_dir, finalize(buckets), md_path='MNRAS_SUMMARY.md', json_path='MNRAS_SUMMARY.json')

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
    # write newline per entry for readability
    with log.open('a', encoding='utf-8') as f:
        f.write(msg.rstrip('\n') + '\n')
    print(msg.rstrip('\n'))

def _write_empty(path: Path) -> None:
    try:
        Path(path).write_text('', encoding='utf-8')
    except Exception:
        pass

def _write_status_json(xdir: Path, status: dict) -> None:
    try:
        (xdir / 'STEP4_XMATCH_STATUS.json').write_text(json.dumps(status, indent=2), encoding='utf-8')
    except Exception:
        pass

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
        recs = json.loads((Path(run_dir) / 'RUN_INDEX.json').read_text(encoding='utf-8'))
        if recs:
            stem = Path(recs[0].get('tile','')).name
            parts = stem.split('_')
            return float(parts[1]), float(parts[2])
    except Exception:
        pass
    return _coords_from_tile_dirname(Path(run_dir).name)

# --- UPDATED: CDS xmatch with fallback toggle ---

def _cds_xmatch_tile(
    tile_dir, pass2_ldac, *, radius_arcsec: float = 5.0,
    cds_gaia_table: str | None = None, cds_ps1_table: str | None = None,
    fallback_empty_use_raw: bool = False,
) -> None:
    tile_dir = Path(tile_dir)
    xdir = tile_dir / 'xmatch'; xdir.mkdir(parents=True, exist_ok=True)

    # Raw extraction
    sex_csv_raw = _ensure_sextractor_csv(tile_dir, pass2_ldac)

    # Apply filters to a COPY
    buckets = init_buckets()
    sex_csv_flt = _apply_mnras_filters_and_spikes(tile_dir, sex_csv_raw, buckets)

    # Decide which CSV to use
    chosen_csv = sex_csv_flt
    status = {'gaia': 'skipped', 'ps1': 'skipped', 'gaia_rows': 0, 'ps1_rows': 0}

    def _rows(path: Path) -> int:
        return _csv_row_count(path)

    if _rows(sex_csv_flt) == 0:
        if fallback_empty_use_raw and _rows(sex_csv_raw) > 0:
            _cds_log(xdir, "[STEP4][CDS] Filtered catalog empty — FALLBACK to raw SExtractor for CDS xmatch.")
            chosen_csv = sex_csv_raw
            status['fallback_used'] = True
            status['fallback_source'] = 'raw'
        else:
            _cds_log(xdir, "[STEP4][CDS] SExtractor filtered catalog is empty — skipping Gaia/PS1; writing placeholders.")
            out_gaia = xdir / 'sex_gaia_xmatch_cdss.csv'
            out_ps1  = xdir / 'sex_ps1_xmatch_cdss.csv'
            _write_empty(out_gaia); _validate_within5_arcsec_unit_tolerant(out_gaia)
            _write_empty(out_ps1);  _validate_within5_arcsec_unit_tolerant(out_ps1)
            status['gaia'] = 'skipped-empty-sextractor'
            status['ps1']  = 'skipped-empty-sextractor'
            _write_status_json(xdir, status)
            write_summary(tile_dir, finalize(buckets), md_path='MNRAS_SUMMARY.md', json_path='MNRAS_SUMMARY.json')
            return

    # Detect RA/Dec columns on the chosen CSV
    sex_cols = _detect_radec_columns(chosen_csv) or ('ALPHA_J2000', 'DELTA_J2000')
    ra_col, dec_col = sex_cols

    # --- Gaia (CDS) ---
    out_gaia = xdir / 'sex_gaia_xmatch_cdss.csv'
    if cds_gaia_table:
        try:
            if os.getenv("VASCO_CDS_PRECALL_SLEEP", "0") in ("1", "true", "True"):
                time.sleep(10.0)
            _cds_log(xdir, f"[STEP4][CDS] Start — radius={radius_arcsec} arcsec; GAIA={cds_gaia_table!r}; PS1={cds_ps1_table!r}")
            _cds_log(xdir, f"[STEP4][CDS] Using SExtractor CSV: {Path(chosen_csv).name} (RA={ra_col}, DEC={dec_col})")
            _cds_log(xdir, f"[STEP4][CDS] Query Gaia table {cds_gaia_table} -> {out_gaia.name}")
            cdsskymatch(
                chosen_csv, out_gaia,
                ra=ra_col, dec=dec_col,
                cdstable=cds_gaia_table,
                radius_arcsec=radius_arcsec,
                find='best', ofmt='csv', omode='out',
                blocksize=1000
            )
            _validate_within5_arcsec_unit_tolerant(out_gaia)
            rows = _csv_row_count(out_gaia)
            status['gaia'] = 'ok'; status['gaia_rows'] = rows
            _cds_log(xdir, f"[STEP4][CDS] Gaia OK — rows={rows}")
        except Exception as e:
            status['gaia'] = 'failed'; status['gaia_error'] = str(e)
            _cds_log(xdir, f"[STEP4][CDS][WARN] Gaia xmatch failed: {e}")
            _write_empty(out_gaia)
            _validate_within5_arcsec_unit_tolerant(out_gaia)
    else:
        _cds_log(xdir, "[STEP4][CDS] Gaia table not provided — skipping")

    # --- PS1 (coverage guard retained) ---
    if cds_ps1_table:
        if os.getenv('VASCO_DISABLE_PS1'):
            _cds_log(xdir, "[STEP4][CDS] PS1 disabled by env — skipping")
            _write_status_json(xdir, status)
            write_summary(tile_dir, finalize(buckets), md_path='MNRAS_SUMMARY.md', json_path='MNRAS_SUMMARY.json')
            return

        center = _tile_center_from_index_or_name(tile_dir)
        if center and center[1] < -30.0:
            _cds_log(xdir, f"[STEP4][CDS] PS1 skipped (Dec={center[1]:.3f} < -30°, outside coverage)")
            status['ps1'] = 'skipped'
            _write_status_json(xdir, status)
            write_summary(tile_dir, finalize(buckets), md_path='MNRAS_SUMMARY.md', json_path='MNRAS_SUMMARY.json')
            return

        out_ps1 = xdir / 'sex_ps1_xmatch_cdss.csv'
        try:
            if os.getenv("VASCO_CDS_PRECALL_SLEEP", "0") in ("1", "true", "True"):
                time.sleep(10.0)
            _cds_log(xdir, f"[STEP4][CDS] Query PS1 table {cds_ps1_table} -> {out_ps1.name}")
            cdsskymatch(
                chosen_csv, out_ps1,
                ra=ra_col, dec=dec_col,
                cdstable=cds_ps1_table,
                radius_arcsec=radius_arcsec,
                find='best', ofmt='csv', omode='out', blocksize=1000,
            )
            _validate_within5_arcsec_unit_tolerant(out_ps1)
            rows = _csv_row_count(out_ps1)
            status['ps1'] = 'ok'; status['ps1_rows'] = rows
            _cds_log(xdir, f"[STEP4][CDS] PS1 OK — rows={rows}")
        except Exception as e:
            status['ps1'] = 'failed'; status['ps1_error'] = str(e)
            _cds_log(xdir, f"[STEP4][CDS][WARN] PS1 xmatch failed: {e}")
            _write_empty(out_ps1)
            _validate_within5_arcsec_unit_tolerant(out_ps1)
    else:
        _cds_log(xdir, "[STEP4][CDS] PS1 table not provided — skipping")

    _write_status_json(xdir, status)
    write_summary(tile_dir, finalize(buckets), md_path='MNRAS_SUMMARY.md', json_path='MNRAS_SUMMARY.json')

# --- CLI ---

def cmd_step4_xmatch(args: argparse.Namespace) -> int:
    run_dir = _build_run_dir(Path(args.workdir) if args.workdir else None)
    p2 = run_dir / 'pass2.ldac'
    if not p2.exists():
        print('[STEP4][ERROR] pass2.ldac missing. Run step3-psf-and-pass2 first.')
        return 2
    backend = args.xmatch_backend
    if backend == 'local':
        try:
            stem = Path(json.loads((run_dir / 'RUN_INDEX.json').read_text(encoding='utf-8'))[0]['tile']).name
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
        _cds_xmatch_tile(
            run_dir, p2,
            radius_arcsec=float(args.xmatch_radius_arcsec),
            cds_gaia_table=args.cds_gaia_table, cds_ps1_table=args.cds_ps1_table,
            fallback_empty_use_raw=bool(getattr(args, 'fallback_empty_use_raw', False)),
        )
        return 0
    print('[STEP4][WARN]', run_dir.name, 'Unknown backend:', backend)
    return 0

def cmd_step5_filter_within5(args: argparse.Namespace) -> int:
    run_dir = _build_run_dir(Path(args.workdir) if args.workdir else None)
    xdir = run_dir / 'xmatch'
    if not xdir.exists():
        print('[STEP5][ERROR] xmatch/ missing. Run step4-xmatch first.')
        return 2
    # Only process canonical xmatch inputs; skip files already filtered
    patterns = [
        'sex_*_xmatch.csv',      # local GAIA/PS1/USNOB xmatches
        'sex_*_xmatch_cdss.csv', # CDS GAIA/PS1 xmatches
    ]
    targets: List[Path] = []
    for pat in patterns:
        targets.extend(sorted(xdir.glob(pat)))
    wrote = 0
    for src in targets:
        # Skip if the within5 output already exists
        out = src.with_name(src.stem + '_within5arcsec.csv')
        if out.exists():
            print(f'[STEP5][SKIP] {src.name} -> {out.name} (already exists)')
            continue
        try:
            _validate_within5_arcsec_unit_tolerant(src)
            wrote += 1
            print(f'[STEP5][OK] {src.name} -> {out.name}')
        except Exception as e:
            print('[STEP5][WARN] within5 failed for', src.name, ':', e)
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
    p = argparse.ArgumentParser(
        prog='vasco.cli_pipeline',
        description='VASCO pipeline orchestrator (split workflow + POSSI-E guard; LDAC#; CDS placeholders)'
    )
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
    one.add_argument('--cds-gaia-table', default=os.getenv('VASCO_CDS_GAIA_TABLE', 'I/355/gaiadr3'))
    one.add_argument('--cds-ps1-table', default=os.getenv('VASCO_CDS_PS1_TABLE', 'II/389/ps1_dr2'))
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

    s4 = sub.add_parser('step4-xmatch', help='Cross-match (local/CDS; PS1 coverage guard; CDS placeholders)')
    s4.add_argument('--workdir', required=True)
    s4.add_argument('--xmatch-backend', choices=['local','cds'], default='local')
    s4.add_argument('--xmatch-radius-arcsec', type=float, default=5.0)
    s4.add_argument('--size-arcmin', type=float, default=30.0)
    s4.add_argument('--cds-gaia-table', default=os.getenv('VASCO_CDS_GAIA_TABLE', 'I/355/gaiadr3'))
    s4.add_argument('--cds-ps1-table', default=os.getenv('VASCO_CDS_PS1_TABLE', 'II/389/ps1_dr2'))
    # NEW: fallback toggle
    s4.add_argument('--fallback-empty-use-raw', action='store_true')
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

# step1-download implementation (unchanged structure)

def cmd_step1_download(args: argparse.Namespace) -> int:
    run_dir = Path(args.workdir)
    lg = dl.configure_logger(Path('./data/logs'))
    ra = _to_float_ra(args.ra)
    dec = _to_float_dec(args.dec)

    try:
        # Downloader will mkdir raw/ only on success (late promotion)
        fits = dl.fetch_skyview_dss(
            ra, dec,
            size_arcmin=args.size_arcmin,
            survey=args.survey,
            pixel_scale_arcsec=args.pixel_scale_arcsec,
            out_dir=run_dir / 'raw',   # created only on success
            logger=lg,
        )
        # POSSI‑E enforcement (may delete and raise)
        _enforce_possi_e_or_skip(Path(fits), lg)

        # ---- SUCCESS PATH ----
        print('[STEP1] Downloaded FITS ->', fits)
        counts = {'planned': 1, 'downloaded': 1, 'processed': 0, 'filtered_non_poss': 0}
        _write_json(run_dir / 'RUN_COUNTS.json', counts)
        _write_json(run_dir / 'RUN_INDEX.json', [{'tile': Path(fits).stem}])
        _write_json(run_dir / 'RUN_MISSING.json', [])
        _write_overview(run_dir, counts, [{'tile': Path(fits).stem}], [])
        return 0

    except RuntimeError as e:
        # Known non‑POSS case: treated as a skip
        if 'Non-POSS plate returned by STScI' in str(e):
            print('[SKIP]', f'RA={ra:.6f}', f'Dec={dec:.6f}', '-> non-POSS; tile omitted.')
            counts = {'planned': 1, 'downloaded': 0, 'processed': 0, 'filtered_non_poss': 1}
            missing = [{
                'ra': float(ra),
                'dec': float(dec),
                'expected_stem': _expected_stem(ra, dec, args.survey, args.size_arcmin),
            }]
            # Write RUN_* only if the tile folder exists (avoid creating it on error)
            if run_dir.exists():
                _write_json(run_dir / 'RUN_COUNTS.json', counts)
                _write_json(run_dir / 'RUN_MISSING.json', missing)
                _write_json(run_dir / 'RUN_INDEX.json', [])
                _write_overview(run_dir, counts, [], missing)
            return 0

# overview writer unchanged

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

if __name__ == '__main__':
    raise SystemExit(main())
