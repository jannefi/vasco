
from __future__ import annotations
import logging, gzip
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import List, Tuple
from datetime import datetime, timezone
from astropy.io import fits

__all__ = [
    'configure_logger','fetch_skyview_dss','fetch_many',
    'tessellate_centers','fetch_tessellated','SURVEY_ALIASES'
]

_DEF_UA = 'VASCO/0.06.9 (+downloader stsci-only)'

# Aliases accepted by CLI. Note: STScI DSS selects plate series by declination for DSS1/2.
SURVEY_ALIASES = {
    'dss1'      : 'DSS1',
    'dss1-red'  : 'DSS1 Red',
    'dss1-blue' : 'DSS1 Blue',
    'dss'       : 'DSS',
    'dss2-red'  : 'DSS2 Red',
    'dss2-blue' : 'DSS2 Blue',
    'dss2-ir'   : 'DSS2 IR',
    # Intent: POSS-I E (red). We will enforce POSSI-E header prior to promotion (see below).
    'poss1-e'   : 'DSS1 Red',
}

# Staging & errors roots (repo-local)
STAGING_ROOT = Path('./data/.staging')
ERRORS_ROOT  = Path('./data/errors')

# -----------------------------
# Logging
# -----------------------------
def configure_logger(out_dir: Path) -> logging.Logger:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    lg = logging.getLogger('vasco.downloader')
    lg.setLevel(logging.INFO)
    if not lg.handlers:
        h = RotatingFileHandler(out_dir/'download.log', maxBytes=512000, backupCount=3)
        fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
        h.setFormatter(fmt); lg.addHandler(h)
        sh = logging.StreamHandler(); sh.setFormatter(fmt); lg.addHandler(sh)
    return lg

# -----------------------------
# STScI DSS helpers
# -----------------------------
def _stscidss_params(ra_deg: float, dec_deg: float, size_arcmin: float, survey_key: str,
                     user_agent: str) -> tuple[str, dict]:
    # See: https://stdatu.stsci.edu/dss/script_usage.html (v parameter)
    # Mapping summary: https://gsss.stsci.edu/SkySurveys/Surveys.htm
    base = 'https://archive.stsci.edu/cgi-bin/dss_search'
    params = {
        # Hidden but working knob: 'v=poss1_red' biases to POSS-I E plates
        'v': 'poss1_red',
        'r': '{:.6f}'.format(ra_deg),
        'd': '{:.6f}'.format(dec_deg),
        'e': 'J2000',
        'h': '{:.2f}'.format(size_arcmin),
        'w': '{:.2f}'.format(size_arcmin),
        'f': 'fits',
        'c': 'none',
        'fov': 'NONE',
        'v3': ''
    }
    headers = {'User-Agent': user_agent or _DEF_UA}
    p2 = dict(params); p2['__headers__'] = headers; return base, p2

# -----------------------------
# HTTP + FITS normalization
# -----------------------------
def _http_get(url: str, params: dict, timeout: float=60.0):
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    headers = params.pop('__headers__', {})
    s = requests.Session()
    rtry = Retry(total=5, backoff_factor=0.7, status_forcelist=[502,503,504,429])
    s.mount('https://', HTTPAdapter(max_retries=rtry)); s.mount('http://', HTTPAdapter(max_retries=rtry))
    r = s.get(url, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.content, r.headers.get('Content-Type','')

def _looks_like_gzip(buf: bytes) -> bool:
    return len(buf) >= 2 and buf[0] == 0x1F and buf[1] == 0x8B

def _normalize_fits_bytes(buf: bytes) -> tuple[bytes, bool]:
    if not buf or len(buf) < 2880:
        return buf, False
    if _looks_like_gzip(buf):
        try:
            data = gzip.decompress(buf)
        except Exception:
            return buf, False
        if data[:6] == b'SIMPLE' or data[:32].lstrip().startswith(b'SIMPLE'):
            return data, True
        return buf, False
    if buf[:6] == b'SIMPLE' or buf[:32].lstrip().startswith(b'SIMPLE'):
        return buf, True
    return buf, False

# -----------------------------
# Error artifact writer
# -----------------------------
def _write_error_artifacts(stem: str, *, content: bytes|None, content_type: str|None,
                           reason: str, meta: dict) -> None:
    ERRORS_ROOT.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).isoformat()
    j = {
        'when_utc': ts,
        'reason': reason,
        'content_type': content_type,
        **meta,
    }
    (ERRORS_ROOT / f'{stem}.json').write_text(json.dumps(j, indent=2), encoding='utf-8')
    if content:
        (ERRORS_ROOT / f'{stem}.html').write_bytes(content)

# -----------------------------
# Public API (STScI-only; staged writes + late promotion)
# -----------------------------

def fetch_skyview_dss(
    ra_deg: float,
    dec_deg: float,
    *,
    size_arcmin: float = 60.0,
    survey: str = 'dss1-red',
    pixel_scale_arcsec: float = 1.7,  # unused; kept for signature compatibility
    out_dir: Path | str = '.',
    basename: str | None = None,
    user_agent: str = _DEF_UA,
    logger: logging.Logger | None = None
) -> Path:
    """
    Fetch DSS imagery from **STScI only** using a repo-local staging directory to avoid
    creating tile folders on failures. Only promote to the final `out_dir` when:
      - Response is a valid FITS (optionally gzipped),
      - Minimal WCS keys are present,
      - **POSS-I E** is confirmed (for `dss1-red` and `poss1-e`).

    On failure, store artifacts under `./data/errors/` and raise RuntimeError.
    """
    lg = logger or logging.getLogger('vasco.downloader')

    def _norm_ra(x: float) -> float:
        r = float(x) % 360.0
        return r if r >= 0.0 else (r + 360.0)
    def _clamp_dec(x: float) -> float:
        d = float(x)
        return 90.0 if d > 90.0 else (-90.0 if d < -90.0 else d)

    nra = _norm_ra(ra_deg)
    ndec = _clamp_dec(dec_deg)

    tag = (survey.lower()).replace(' ', '-')
    qra = f"{nra:.3f}"
    qdec = f"{ndec:.3f}"
    name = basename or f"{tag}_{qra}_{qdec}_{int(round(size_arcmin))}arcmin.fits"
    stem = Path(name).with_suffix('').name  # used for error artifact names

    # Build STScI request
    url, params = _stscidss_params(nra, ndec, size_arcmin, survey, user_agent)
    lg.info('[GET] STScI DSS RA=%.6f Dec=%.6f size=%.2f arcmin v=%s', nra, ndec, size_arcmin, params.get('v'))

    # Repo-local staging
    STAGING_ROOT.mkdir(parents=True, exist_ok=True)
    stg = STAGING_ROOT / ('.tmp_' + name)

    # HTTP + FITS normalization
    content, ctype = _http_get(url, dict(params))
    data, ok = _normalize_fits_bytes(content)
    if not ok:
        _write_error_artifacts(stem, content=content, content_type=ctype,
                               reason='REJECT_NON_FITS',
                               meta={'survey': SURVEY_ALIASES.get(survey.lower(), survey),
                                     'ra_deg': nra, 'dec_deg': ndec, 'size_arcmin': float(size_arcmin)})
        lg.error('[REJECT_NON_FITS] Content-Type=%s bytes=%d -> %s.html', ctype, len(content), stem)
        raise RuntimeError(f'STScI returned non-FITS: {ctype}')

    # Stage the FITS bytes
    stg.write_bytes(data)

    # Minimal WCS sanity (primary HDU)
    try:
        with fits.open(stg, memmap=False) as hdul:
            hdr = hdul[0].header if hdul and hdul[0].header else {}
            for k in ('NAXIS1','NAXIS2','CRVAL1','CRVAL2'):
                if k not in hdr:
                    raise KeyError(k)
            survey_name = str(hdr.get('SURVEY','')).upper()
    except Exception as e:
        try:
            stg.unlink(missing_ok=True)
        finally:
            pass
        _write_error_artifacts(stem, content=None, content_type='application/fits',
                               reason='REJECT_NON_WCS',
                               meta={'error': str(e), 'survey': SURVEY_ALIASES.get(survey.lower(), survey),
                                     'ra_deg': nra, 'dec_deg': ndec, 'size_arcmin': float(size_arcmin)})
        lg.error('[REJECT_NON_WCS] Missing minimal WCS keys or unreadable FITS: %s', e)
        raise RuntimeError('FITS missing minimal WCS keys')

    # Strict POSS-I enforcement for dss1-red and poss1-e intents
    if survey.lower() in ('poss1-e','dss1-red'):
        if not (('POSS' in survey_name) or ('POSS-I' in survey_name) or ('POSS E' in survey_name) or ('POSS-E' in survey_name) or (survey_name == 'POSSI-E')):
            try:
                stg.unlink(missing_ok=True)
            finally:
                pass
            _write_error_artifacts(stem, content=None, content_type='application/fits',
                                   reason='REJECT_NON_POSS',
                                   meta={'header_SURVEY': survey_name or 'UNKNOWN',
                                         'survey': SURVEY_ALIASES.get(survey.lower(), survey),
                                         'ra_deg': nra, 'dec_deg': ndec, 'size_arcmin': float(size_arcmin)})
            lg.error('[ENFORCE][REJECT_NON_POSS] SURVEY=%r (declination likely outside POSS coverage)', survey_name or 'UNKNOWN')
            raise RuntimeError(f'Non-POSS plate returned by STScI: SURVEY={survey_name!r}')

    # Promote staging -> final
    out_dir = Path(out_dir)
    final_path = out_dir / name
    final_path.parent.mkdir(parents=True, exist_ok=True)
    stg.replace(final_path)
    lg.info('[OK] wrote %s (%d bytes)', str(final_path), final_path.stat().st_size)
    return final_path

# -----------------------------
# Batch fetch helpers (STScI-only)
# -----------------------------
def fetch_many(rows: List[Tuple[float,float]], *, size_arcmin: float=60.0,
               survey: str='dss1-red', pixel_scale_arcsec: float=1.7,
               out_dir: Path | str='.', user_agent: str=_DEF_UA,
               logger: logging.Logger | None=None) -> List[Path]:
    lg = logger or logging.getLogger('vasco.downloader')
    out: List[Path] = []
    for ra,dec in rows:
        try:
            path = fetch_skyview_dss(ra, dec, size_arcmin=size_arcmin, survey=survey,
                                     pixel_scale_arcsec=pixel_scale_arcsec, out_dir=out_dir,
                                     user_agent=user_agent, logger=lg)
            if path.suffix.lower() == '.fits':
                out.append(path)
        except Exception as e:
            lg.error('[FAIL] RA=%.6f Dec=%.6f -> %s', ra, dec, e)
    return out

def tessellate_centers(center_ra: float, center_dec: float, *,
                        width_arcmin: float, height_arcmin: float,
                        tile_radius_arcmin: float=30.0, overlap_arcmin: float=0.0) -> List[Tuple[float,float]]:
    hw = width_arcmin/2.0; hh = height_arcmin/2.0; r = tile_radius_arcmin
    from math import sqrt, cos, radians
    sy = max(1e-6, sqrt(3.0)*r - overlap_arcmin); sx = max(1e-6, 2.0*r - overlap_arcmin)
    res: List[Tuple[float,float]] = []; j=0; off=0.0
    while off <= hh + 1e-6:
        for sgn in (1.0, -1.0):
            dec = center_dec + (sgn*off)/60.0
            cd = max(1e-8, cos(radians(dec)))
            sxdeg = (sx/60.0)/cd; base = center_ra; raoff = 0.0 if (j%2)==0 else 0.5*sxdeg
            k=0
            while True:
                ra1 = base + raoff + k*sxdeg; ra2 = base + raoff - k*sxdeg
                dx1 = abs((ra1-center_ra)*cd*60.0); dx2 = abs((ra2-center_ra)*cd*60.0)
                if dx1 <= hw + 1e-6: res.append((ra1,dec))
                if k>0 and dx2 <= hw + 1e-6: res.append((ra2,dec))
                if dx1>hw+1e-6 and dx2>hw+1e-6: break
                k+=1
        j+=1; off+=sy
    uniq=[]; seen=set()
    for ra,dc in res:
        key=(round(ra,6), round(dc,6))
        if key not in seen: seen.add(key); uniq.append((ra,dc))
    return uniq

def fetch_tessellated(center_ra: float, center_dec: float, *,
                       width_arcmin: float, height_arcmin: float,
                       tile_radius_arcmin: float=30.0, overlap_arcmin: float=0.0,
                       size_arcmin: float=60.0, survey: str='dss1-red',
                       pixel_scale_arcsec: float=1.7, out_dir: Path | str='.',
                       user_agent: str=_DEF_UA, logger: logging.Logger | None=None) -> List[Path]:
    centers = tessellate_centers(center_ra, center_dec,
                                 width_arcmin=width_arcmin, height_arcmin=height_arcmin,
                                 tile_radius_arcmin=tile_radius_arcmin, overlap_arcmin=overlap_arcmin)
    return fetch_many(centers, size_arcmin=size_arcmin, survey=survey,
                      pixel_scale_arcsec=pixel_scale_arcsec, out_dir=out_dir,
                      user_agent=user_agent, logger=logger)

# Backward-compat shim kept from earlier edits
def get_image_service(service):
    if service.lower() == 'stsci':
        print('[INFO] Using STScI DSS endpoint for original pixel grid.')
    else:
        print('[INFO] STScI-only build: SkyView disabled')
