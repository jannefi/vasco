
from __future__ import annotations
import logging, gzip
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import List, Tuple
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
  # Intent: POSS-I E (red). STScI will still choose plate by declination; we enforce via header.
  'poss1-e'   : 'DSS1 Red',
}

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# STScI DSS helpers
# -----------------------------------------------------------------------------
def _stscidss_params(ra_deg: float, dec_deg: float, size_arcmin: float, survey_key: str,
                     user_agent: str) -> tuple[str, dict]:
    # STScI supports ONLY generation selection via v=1 (DSS1) or v=2 (DSS2);
    # plate series (POSS vs SERC/AAO) are chosen by declination zone.
    # See: https://stdatu.stsci.edu/dss/script_usage.html (v parameter)
    # Mapping summary: https://gsss.stsci.edu/SkySurveys/Surveys.htm
    v = {
        'dss1':'1','dss1-red':'1','dss1-blue':'1',
        'dss2-red':'2','dss2-blue':'2','dss2-ir':'2',
        'poss1-e':'1',  # explicit intent for POSS-I E => DSS1 Red (v=1)
    }.get(survey_key.lower(), '1')
    base = 'https://archive.stsci.edu/cgi-bin/dss_search'
    params = {
        # force possi1_red to -v. It is unodumented but works.
        'v': 'poss1_red', 'r': '{:.6f}'.format(ra_deg), 'd': '{:.6f}'.format(dec_deg), 'e': 'J2000', 
        'h': '{:.2f}'.format(size_arcmin), 'w': '{:.2f}'.format(size_arcmin),
        'f': 'fits', 'c': 'none', 'fov': 'NONE', 'v3': ''
    }
    headers = {'User-Agent': user_agent or _DEF_UA}
    p2 = dict(params); p2['__headers__'] = headers; return base, p2

# -----------------------------------------------------------------------------
# HTTP + FITS normalization
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# Public API (STScI-only; SkyView disabled)
# -----------------------------------------------------------------------------
def fetch_skyview_dss(ra_deg: float, dec_deg: float, *,
  size_arcmin: float=60.0,
  survey: str='dss1-red',
  pixel_scale_arcsec: float=1.7,  # unused; kept for signature compatibility
  out_dir: Path | str='.',
  basename: str | None=None,
  user_agent: str=_DEF_UA,
  logger: logging.Logger | None=None) -> Path:
    """Fetch DSS imagery from **STScI only**. If `survey` is 'poss1-e', strictly enforce
    POSS-I E by inspecting FITS headers and failing on mismatch.

    Note: STScI `dss_search` supports survey generation via `v=1/2` only; underlying
    plate series (POSS vs SERC/AAO) are chosen by declination zone. We enforce POSS-I E
    post-download by inspecting FITS headers.
    References: DSS script usage (v=1/2), survey mapping by hemisphere.
    """
    out_dir = Path(out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    lg = logger or logging.getLogger('vasco.downloader')
    tag = (survey.lower()).replace(' ','-')
    name = basename or f"{tag}_{ra_deg:.6f}_{dec_deg:.6f}_{int(round(size_arcmin))}arcmin.fits"
    out_path = out_dir / name

    url, p = _stscidss_params(ra_deg, dec_deg, size_arcmin, survey, user_agent)
    lg.info('[GET] STScI DSS RA=%.6f Dec=%.6f size=%.2f arcmin v=%s',
            ra_deg, dec_deg, size_arcmin, p.get('v'))

    content, ctype = _http_get(url, dict(p))
    data, ok = _normalize_fits_bytes(content)
    if not ok:
        bad = out_path.with_suffix('.html')
        bad.write_bytes(content)
        lg.error('[FAIL] Non-FITS from STScI (Content-Type=%s, len=%d) -> %s', ctype, len(content), bad)
        raise RuntimeError(f'STScI returned non-FITS: {ctype}')

    out_path.write_bytes(data)
    lg.info('[OK] wrote %s (%d bytes)', str(out_path), len(data))

    # Strict enforcement: POSS-I E only (when requested)
    if survey.lower() == 'poss1-e':
        with fits.open(out_path, memmap=False) as hdul:
            hdr = hdul[0].header
            sname = str(hdr.get('SURVEY','')).upper()  # e.g. 'POSS-I E', 'SERC-EJ'
            origin = str(hdr.get('ORIGIN','')).upper()
            plate = str(hdr.get('PLATEID','')).upper()
        if not (('POSS' in sname) or ('POSS-I' in sname) or ('POSS E' in sname) or ('POSS-E' in sname)):
            lg.error('[ENFORCE] Requested POSS-I E but header shows SURVEY=%r ORIGIN=%r PLATEID=%r',
                     sname or 'UNKNOWN', origin or 'UNKNOWN', plate or 'UNKNOWN')
            # STScI-only policy: do not auto-replace via SkyView. Fail fast.
            raise RuntimeError(f'Non-POSS plate returned by STScI: SURVEY={sname!r} '                                '(declination likely outside POSS coverage)')

    return out_path

# -----------------------------------------------------------------------------
# Batch fetch helpers (unchanged signatures; STScI-only inside)
# -----------------------------------------------------------------------------
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
