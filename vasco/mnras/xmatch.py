from __future__ import annotations
from typing import Tuple, Dict, Any
import astropy.units as u
from astropy.coordinates import SkyCoord

# PS1 via MAST catalogs (astroquery)
try:
    from astroquery.mast import Catalogs
except Exception:  # pragma: no cover
    Catalogs = None  # type: ignore

# Gaia via astroquery TAP convenience
try:
    from astroquery.gaia import Gaia
except Exception:  # pragma: no cover
    Gaia = None  # type: ignore


def ps1_match(ra_deg: float, dec_deg: float, r_arcsec: float = 5.0) -> bool:
    """Return True if a PS1 DR2 mean object exists within r_arcsec."""
    if Catalogs is None:
        return False
    try:
        tab = Catalogs.query_region(
            f"{ra_deg} {dec_deg}",
            radius=r_arcsec * u.arcsec,
            catalog="Panstarrs",
            data_release="dr2",
        )
        return len(tab) > 0
    except Exception:
        return False


def gaia_match(
    ra_deg: float, dec_deg: float, r_arcsec: float = 5.0
) -> Tuple[bool, Dict[str, Any]]:
    """Return (matched, best_row) using a Gaia EDR3 cone; best_row may be empty dict if no match."""
    if Gaia is None:
        return False, {}
    try:
        coord = SkyCoord(ra=ra_deg * u.deg, dec=dec_deg * u.deg, frame="icrs")
        j = Gaia.cone_search_async(coord, radius=r_arcsec * u.arcsec)
        res = j.get_results()
        if len(res) == 0:
            return False, {}
        # pick nearest
        res = res.to_pandas()
        # Gaia returns distance columns only in some endpoints; fallback to angular calc if missing
        res["sep"] = (res["ra"] - ra_deg).abs() + (res["dec"] - dec_deg).abs()
        row = res.sort_values("sep").iloc[0].to_dict()
        return True, row
    except Exception:
        return False, {}
