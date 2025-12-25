from __future__ import annotations
from typing import Dict, Any
from astropy.table import Table
import numpy as np

def apply_extract_filters(tab: Table, cfg: Dict[str, Any]) -> Table:
    """Apply Section 2 detect-time style filters on a per-row basis.
    We assume SNR proxy SNR_WIN exists; DETECT_THRESH provenance is kept as config.
    """
    keep = [True] * len(tab)
    if 'FLAGS' in tab.colnames:
        keep = (tab['FLAGS'] == cfg.get('flags_equal', 0))
    if 'SNR_WIN' in tab.colnames:
        keep = keep & (tab['SNR_WIN'] > cfg.get('snr_win_min', 30.0))
    # Note: DETECT_THRESH=5 is a config of the extraction; we log it elsewhere.
    return tab[keep]


def apply_morphology_filters(tab, cfg):
    keep = np.ones(len(tab), dtype=bool)
    # SPREAD_MODEL filter
    if 'SPREAD_MODEL' in tab.colnames:
        spread = np.array(tab['SPREAD_MODEL'])
        spread = np.where(np.isfinite(spread), spread, np.nan)
        keep = keep & (spread > cfg.get('spread_model_min', -0.002))
    # FWHM_IMAGE filter
    if 'FWHM_IMAGE' in tab.colnames:
        fwhm = np.array(tab['FWHM_IMAGE'])
        # Try to coerce to float, set non-convertible to NaN
        try:
            fwhm = fwhm.astype(float)
        except Exception:
            fwhm = np.array([float(x) if str(x).replace('.', '', 1).isdigit() else np.nan for x in fwhm])
        keep = keep & ((2.0 * fwhm) < cfg.get('two_fwhm_lt', 7.0))
    # ELONGATION filter
    if 'ELONGATION' in tab.colnames:
        elong = np.array(tab['ELONGATION'])
        try:
            elong = elong.astype(float)
        except Exception:
            elong = np.array([float(x) if str(x).replace('.', '', 1).isdigit() else np.nan for x in elong])
        keep = keep & (elong < cfg.get('elongation_lt', 1.3))
    # Optional XY bounds
    xy = cfg.get('xy_bounds', {}) or {}
    for axis, cmin, cmax in [
        ('X_IMAGE', xy.get('xmin'), xy.get('xmax')),
        ('Y_IMAGE', xy.get('ymin'), xy.get('ymax'))
    ]:
        if axis in tab.colnames:
            arr = np.array(tab[axis])
            try:
                arr = arr.astype(float)
            except Exception:
                arr = np.array([float(x) if str(x).replace('.', '', 1).isdigit() else np.nan for x in arr])
            if cmin is not None:
                keep = keep & (arr >= float(cmin))
            if cmax is not None:
                keep = keep & (arr <= float(cmax))
    return tab[keep]
