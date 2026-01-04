
# filters_mnras.py  — MNRAS-aligned filters
from __future__ import annotations
from typing import Dict, Any
from astropy.table import Table
import numpy as np

def _robust_sigma_clip(x: np.ndarray, k: float = 2.0) -> np.ndarray:
    """Return boolean mask keeping |x - median| <= k * sigma, with sigma ≈ 1.4826 * MAD."""
    x = np.asarray(x, dtype=float)
    med = np.nanmedian(x)
    mad = np.nanmedian(np.abs(x - med))
    sigma = 1.4826 * mad
    if not np.isfinite(sigma) or sigma <= 0:
        return np.isfinite(x)
    return np.isfinite(x) & (np.abs(x - med) <= k * sigma)

def apply_extract_filters(tab: Table, cfg: Dict[str, Any]) -> Table:
    """
    Section 2 (paper): keep FLAG==0 and SNR_WIN>30. DETECT_THRESH=5 is extraction-time.
    """
    keep = np.ones(len(tab), dtype=bool)
    if 'FLAGS' in tab.colnames:
        keep &= (np.array(tab['FLAGS']) == cfg.get('flags_equal', 0))
    if 'SNR_WIN' in tab.colnames:
        keep &= (np.array(tab['SNR_WIN'], dtype=float) > cfg.get('snr_win_min', 30.0))
    return tab[keep]

def apply_morphology_filters(tab: Table, cfg: Dict[str, Any]) -> Table:
    """
    Section 2 paper rules:
      - SPREAD_MODEL > -0.002
      - 2 < FWHM_IMAGE < 7
      - ELONGATION < 1.3
      - |(XMAX-XMIN) - (YMAX-YMIN)| < 2  and  (XMAX-XMIN) > 1  and  (YMAX-YMIN) > 1
      - (optional) 2σ clipping for FWHM_IMAGE and ELONGATION using median+MAD
    """
    keep = np.ones(len(tab), dtype=bool)

    # Optional 2σ clipping (default True)
    if cfg.get('sigma_clip', True):
        if 'FWHM_IMAGE' in tab.colnames:
            fwhm = np.array(tab['FWHM_IMAGE'], dtype=float)
            keep &= _robust_sigma_clip(fwhm, k=float(cfg.get('sigma_k', 2.0)))
        if 'ELONGATION' in tab.colnames:
            elong = np.array(tab['ELONGATION'], dtype=float)
            keep &= _robust_sigma_clip(elong, k=float(cfg.get('sigma_k', 2.0)))

    # SPREAD_MODEL > -0.002
    if 'SPREAD_MODEL' in tab.colnames:
        spread = np.array(tab['SPREAD_MODEL'], dtype=float)
        keep &= (spread > float(cfg.get('spread_model_min', -0.002)))

    # 2 < FWHM_IMAGE < 7
    if 'FWHM_IMAGE' in tab.colnames:
        fwhm = np.array(tab['FWHM_IMAGE'], dtype=float)
        fmin = float(cfg.get('fwhm_lower', 2.0))
        fmax = float(cfg.get('fwhm_upper', 7.0))
        keep &= np.isfinite(fwhm)
        keep &= (fwhm > fmin) & (fwhm < fmax)

    # ELONGATION < 1.3
    if 'ELONGATION' in tab.colnames:
        elong = np.array(tab['ELONGATION'], dtype=float)
        keep &= np.isfinite(elong) & (elong < float(cfg.get('elongation_lt', 1.3)))

    # Pixel-extent constraints (need these columns present)
    need = {'XMAX_IMAGE','XMIN_IMAGE','YMAX_IMAGE','YMIN_IMAGE'}
    if need.issubset(set(tab.colnames)):
        xmax = np.array(tab['XMAX_IMAGE'], dtype=float)
        xmin = np.array(tab['XMIN_IMAGE'], dtype=float)
        ymax = np.array(tab['YMAX_IMAGE'], dtype=float)
        ymin = np.array(tab['YMIN_IMAGE'], dtype=float)
        dx = xmax - xmin
        dy = ymax - ymin
        keep &= np.isfinite(dx) & np.isfinite(dy)
        keep &= (np.abs(dx - dy) < float(cfg.get('extent_delta_lt', 2.0)))
        keep &= (dx > float(cfg.get('extent_min', 1.0))) & (dy > float(cfg.get('extent_min', 1.0)))

    # (Optional) legacy xy_bounds hook still supported
    xy = cfg.get('xy_bounds', {}) or {}
    for axis, cmin, cmax in [
        ('X_IMAGE', xy.get('xmin'), xy.get('xmax')),
        ('Y_IMAGE', xy.get('ymin'), xy.get('ymax')),
    ]:
        if axis in tab.colnames and (cmin is not None or cmax is not None):
            arr = np.array(tab[axis], dtype=float)
            if cmin is not None: keep &= (arr >= float(cmin))
            if cmax is not None: keep &= (arr <= float(cmax))

    return tab[keep]
