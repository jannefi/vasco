from __future__ import annotations
from typing import Tuple, Dict, Any
import astropy.units as u
from astropy.coordinates import SkyCoord
from astropy.time import Time


def backprop_gaia_row(
    row: Dict[str, Any], target_epoch: float = 1950.0
) -> Tuple[float, float]:
    """Back-propagate a Gaia row (ra, dec, pmra, pmdec) to target_epoch (Julian year)."""
    try:
        c = SkyCoord(
            ra=float(row["ra"]) * u.deg,
            dec=float(row["dec"]) * u.deg,
            pm_ra_cosdec=float(row.get("pmra", 0.0)) * u.mas / u.yr,
            pm_dec=float(row.get("pmdec", 0.0)) * u.mas / u.yr,
            obstime=Time(2016.0, format="jyear"),
        )
        c2 = c.apply_space_motion(Time(target_epoch, format="jyear"))
        return c2.ra.deg, c2.dec.deg
    except Exception:
        return float("nan"), float("nan")
