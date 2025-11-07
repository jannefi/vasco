from __future__ import annotations
from typing import Dict, Any
from astropy.table import Table


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


def apply_morphology_filters(tab: Table, cfg: Dict[str, Any]) -> Table:
    """Apply morphology/shape/size-based artifact removal (from the flow figure)."""
    keep = [True] * len(tab)
    if 'SPREAD_MODEL' in tab.colnames:
        keep = (tab['SPREAD_MODEL'] > cfg.get('spread_model_min', -0.002))
    if 'FWHM_IMAGE' in tab.colnames:
        keep = keep & ((2.0 * tab['FWHM_IMAGE']) < cfg.get('two_fwhm_lt', 7.0))
    if 'ELONGATION' in tab.colnames:
        keep = keep & (tab['ELONGATION'] < cfg.get('elongation_lt', 1.3))
    # Optional XY bounds
    xy = cfg.get('xy_bounds', {}) or {}
    for axis, cmin, cmax in [('X_IMAGE', xy.get('xmin'), xy.get('xmax')),
                             ('Y_IMAGE', xy.get('ymin'), xy.get('ymax'))]:
        if axis in tab.colnames:
            if cmin is not None:
                keep = keep & (tab[axis] >= float(cmin))
            if cmax is not None:
                keep = keep & (tab[axis] <= float(cmax))
    return tab[keep]
