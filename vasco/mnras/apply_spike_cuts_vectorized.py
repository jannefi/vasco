# vasco/mnras/apply_spike_cuts_vectorized.py
from __future__ import annotations
from typing import Any, Dict, Iterable, List, Tuple
import numpy as np
import astropy.units as u
from astropy.coordinates import SkyCoord
from vasco.mnras.spikes import BrightStar, SpikeConfig, SpikeRuleConst, SpikeRuleLine

def apply_spike_cuts_vectorized(
    tile_rows: Iterable[Dict[str, Any]],
    bright: List[BrightStar],
    cfg: SpikeConfig,
    src_ra_key: str = "ALPHA_J2000",
    src_dec_key: str = "DELTA_J2000",
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Vectorized spike rejection using nearest-neighbor matching.

    Semantics follow vasco.mnras.spikes.apply_spike_cuts:
    - nearest bright star within cfg.search_radius_arcmin determines (d_arcsec, m_near)
    - reject if any rule matches:
      * CONST: m_near <= const_max_mag
      * LINE: m_near < a*d_arcsec + b (strict inequality)
    - annotate rejected rows with:
      spike_d_arcmin, spike_m_near, spike_reason
    - annotate kept rows with spike_reason="" (and keep 'no_wcs' rows as kept)
    """
    rows = list(tile_rows)
    if not rows:
        return [], []

    # If no bright stars, no effect (but preserve spike_reason field)
    if not bright:
        kept = []
        for r in rows:
            r2 = dict(r)
            r2["spike_reason"] = ""
            kept.append(r2)
        return kept, []

    # --- parse detection coordinates (track which rows are valid) ---
    det_ra = []
    det_dec = []
    valid_pos = []  # positions in `rows` list that have valid coords
    for i, r in enumerate(rows):
        try:
            det_ra.append(float(r[src_ra_key]))
            det_dec.append(float(r[src_dec_key]))
            valid_pos.append(i)
        except Exception:
            continue

    # If none have WCS coords, nothing to do
    if not valid_pos:
        kept = []
        for r in rows:
            r2 = dict(r)
            r2["spike_reason"] = "no_wcs"
            kept.append(r2)
        return kept, []

    det_ra = np.asarray(det_ra, dtype=np.float64)
    det_dec = np.asarray(det_dec, dtype=np.float64)
    det_coords = SkyCoord(det_ra * u.deg, det_dec * u.deg, frame="icrs")

    # --- bright-star coords ---
    b_ra = np.asarray([b.ra for b in bright], dtype=np.float64)
    b_dec = np.asarray([b.dec for b in bright], dtype=np.float64)
    b_mag = np.asarray([b.rmag for b in bright], dtype=np.float64)
    bright_coords = SkyCoord(b_ra * u.deg, b_dec * u.deg, frame="icrs")

    # --- nearest neighbor for each detection ---
    idx, sep2d, _ = det_coords.match_to_catalog_sky(bright_coords)
    d_arcsec = sep2d.arcsec
    m_near = b_mag[idx]

    # If nearest bright star is outside cfg.search_radius_arcmin, treat as "no bright star"
    max_arcsec = float(cfg.search_radius_arcmin) * 60.0

    # Guard against invalid/sentinel magnitudes (e.g. -999 from some feeds).
    # Also enforce the catalog “bright-star” upper bound as a safety net.
    valid_mag = (
        np.isfinite(m_near)
        & (m_near > -900.0)  # kills -999 style sentinels
        & (m_near >= 0.0)
        & (m_near <= float(cfg.rmag_max_catalog))
    )

    has_bright = (d_arcsec <= max_arcsec) & valid_mag

    # --- apply rules (only where has_bright) ---
    reject = np.zeros(len(d_arcsec), dtype=bool)
    reasons: List[List[str]] = [[] for _ in range(len(d_arcsec))]

    for rule in (cfg.rules or []):
        if isinstance(rule, SpikeRuleConst):
            mask = has_bright & (m_near <= float(rule.const_max_mag))
            reject |= mask
            for j in np.where(mask)[0]:
                reasons[j].append(
                    f"CONST(m*={m_near[j]:.2f} <= {float(rule.const_max_mag):.2f})"
                )
        elif isinstance(rule, SpikeRuleLine):
            a = float(rule.a)
            b = float(rule.b)
            thresh = a * d_arcsec + b
            mask = has_bright & (m_near < thresh)  # strict inequality (keep equality)
            reject |= mask
            for j in np.where(mask)[0]:
                reasons[j].append(
                    f"LINE(m*={m_near[j]:.2f} < {a:.3f}*{d_arcsec[j]:.1f}+{b:.2f}={thresh[j]:.2f})"
                )

    # --- rebuild full rows list (kept + rejected) ---
    kept: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    det_i = 0  # index into det arrays
    valid_set = set(valid_pos)

    for i, r in enumerate(rows):
        r2 = dict(r)
        if i not in valid_set:
            r2["spike_reason"] = "no_wcs"
            kept.append(r2)
            continue

        # If no usable bright star (outside radius OR invalid magnitude): keep
        if not has_bright[det_i]:
            r2["spike_reason"] = ""
            kept.append(r2)
            det_i += 1
            continue

        if reject[det_i]:
            r2["spike_d_arcmin"] = round(float(d_arcsec[det_i]) / 60.0, 3)
            r2["spike_m_near"] = float(m_near[det_i])
            r2["spike_reason"] = ";".join(reasons[det_i])
            rejected.append(r2)
        else:
            r2["spike_reason"] = ""
            kept.append(r2)

        det_i += 1

    return kept, rejected