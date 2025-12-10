#!/usr/bin/env python3
"""
Bright star masking utilities – v3

Fixes & improvements
--------------------
1) **Astropy unit bug**: avoid RA*deg when RA/Dec columns already carry units
   (that produced 'deg2' and raised UnitTypeError). Now we detect and handle
   both unitless and unitful columns cleanly.
2) **Zero-result fallback**: if Gaia returns zero stars after magnitude
   filtering, optionally fallback to VizieR Tycho-2 query.
3) **Robust RA/Dec standardization**: supports `_RAJ2000`, `_DEJ2000`,
   `RAdeg`, `DEdeg`, `RA_ICRS`, `DE_ICRS`, etc.

"""
from __future__ import annotations
import logging
from typing import Optional, Tuple

import numpy as np
from astropy.table import Table
from astropy.coordinates import SkyCoord
import astropy.units as u
from astropy.io import fits
from astropy.wcs import WCS

# External queries
try:
    from astroquery.gaia import Gaia
except Exception:
    Gaia = None
try:
    from astroquery.vizier import Vizier
except Exception:
    Vizier = None

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

MAG_PREFERENCE = [
    "phot_rp_mean_mag",
    "phot_g_mean_mag",
    "phot_bp_mean_mag",
    "VTmag",
    "BTmag",  # Tycho-2
    "Rmag",
    "RPmag",
    "r_mag",
    "r",
    "Vmag",
]

RA_CANDIDATES = ["RA", "ra", "RA_ICRS", "ra_icrs", "RAJ2000", "_RAJ2000", "RAdeg"]
DEC_CANDIDATES = ["Dec", "dec", "DE_ICRS", "dec_icrs", "DEJ2000", "_DEJ2000", "DEdeg"]


def _pick_mag_column(tbl: Table, requested: Optional[str]) -> str:
    cols = set(tbl.colnames)
    if requested and requested in cols:
        return requested
    for name in MAG_PREFERENCE:
        if name in cols:
            return name
    for name in tbl.colnames:
        if "mag" in name.lower():
            return name
    raise KeyError(
        f"No usable magnitude column found in table. Columns: {tbl.colnames}"
    )


def _standardize_ra_dec(tbl: Table) -> None:
    ra_name = next((c for c in RA_CANDIDATES if c in tbl.colnames), None)
    dec_name = next((c for c in DEC_CANDIDATES if c in tbl.colnames), None)
    if ra_name and ra_name != "RA":
        try:
            tbl.rename_column(ra_name, "RA")
        except Exception:
            pass
    if dec_name and dec_name != "Dec":
        try:
            tbl.rename_column(dec_name, "Dec")
        except Exception:
            pass
    if "RA" not in tbl.colnames or "Dec" not in tbl.colnames:
        raise RuntimeError("Could not standardize RA/Dec columns in star table")


def query_bright_stars(
    ra: float,
    dec: float,
    size_arcmin: int,
    mag_limit: float = 12.0,
    mag_column: Optional[str] = None,
    fallback_vizier_if_empty: bool = True,
) -> Table:
    c = SkyCoord(ra * u.deg, dec * u.deg, frame="icrs")
    radius_deg = (np.sqrt(2) * size_arcmin) / 60.0 / 2.0

    tbl = None
    gaia_filtered = None

    # Gaia query (keyword args for version compatibility)
    if Gaia is not None:
        try:
            job = Gaia.cone_search_async(coordinate=c, radius=radius_deg * u.deg)
            gtbl = job.get_results()
            _standardize_ra_dec(gtbl)
            mcol = _pick_mag_column(gtbl, mag_column)
            mask = np.isfinite(gtbl[mcol]) & (gtbl[mcol] <= mag_limit)
            gaia_filtered = gtbl[mask]
            logging.info(
                f"Gaia returned {len(gtbl)} stars; {len(gaia_filtered)} <= {mag_limit} mag using '{mcol}'."
            )
            tbl = gaia_filtered
        except Exception as e:
            logging.warning(f"Gaia query failed: {e}")
            tbl = None

    # Fallback to VizieR if Gaia produced zero after filtering
    if (
        (tbl is None or len(tbl) == 0)
        and Vizier is not None
        and fallback_vizier_if_empty
    ):
        try:
            Vizier.ROW_LIMIT = -1
            res = Vizier.query_region(
                c, radius=radius_deg * u.deg, catalog=["I/259/tyc2"]
            )
            if res and len(res) > 0:
                vtbl = res[0]
                _standardize_ra_dec(vtbl)
                mcol = _pick_mag_column(vtbl, mag_column)
                mask = np.isfinite(vtbl[mcol]) & (vtbl[mcol] <= mag_limit)
                tbl = vtbl[mask]
                logging.info(
                    f"VizieR Tycho-2 returned {len(vtbl)}; {len(tbl)} <= {mag_limit} mag using '{mcol}'."
                )
        except Exception as e:
            logging.warning(f"VizieR query failed: {e}")

    if tbl is None:
        raise RuntimeError(
            "No star catalog available (Gaia/VizieR queries failed or not installed)"
        )

    # Record selected magnitude column
    sel_mag = _pick_mag_column(tbl, mag_column)
    tbl.meta["MAG_COL"] = sel_mag
    return tbl


def _ensure_quantity_deg(col) -> u.Quantity:
    """Return a quantity with units of degrees, handling unitless/Quantity columns."""
    try:
        unit = getattr(col, "unit", None)
    except Exception:
        unit = None
    if unit is None:
        return np.asarray(col) * u.deg
    # Ensure degree units (convert if necessary)
    q = col
    try:
        return q.to(u.deg)
    except Exception:
        # If conversion fails, assume it's already angular; fallback to value*deg
        return np.asarray(q) * u.deg


def generate_mask_fits(
    mask_path: str,
    header,
    stars: Table,
    mag_column: Optional[str] = None,
    base_radius_arcsec: float = 15.0,
    scale_bright: float = 8.0,
    scale_faint: float = 12.0,
) -> None:
    w = WCS(header)
    ny = int(header.get("NAXIS2"))
    nx = int(header.get("NAXIS1"))
    mask = np.ones((ny, nx), dtype=np.float32)

    if len(stars) == 0:
        fits.writeto(mask_path, mask, header=header, overwrite=True)
        logging.info(f"Mask written (no bright stars found): {mask_path}")
        return

    sel_mag = mag_column or stars.meta.get("MAG_COL") or _pick_mag_column(stars, None)

    ra_q = _ensure_quantity_deg(stars["RA"])
    dec_q = _ensure_quantity_deg(stars["Dec"])

    sc = SkyCoord(ra_q, dec_q, frame="icrs")
    xs, ys = w.world_to_pixel(sc)

    yy, xx = np.meshgrid(np.arange(ny), np.arange(nx), indexing="ij")

    cd = w.pixel_scale_matrix
    scale_deg = np.sqrt((cd[0, 0] ** 2 + cd[1, 1] ** 2))
    pix_per_arcsec = 1.0 / (scale_deg * 3600.0)

    for i in range(len(stars)):
        mag = float(stars[sel_mag][i]) if sel_mag in stars.colnames else np.nan
        if not np.isfinite(mag):
            continue
        r_arcsec = _mask_radius_for_mag(
            mag, base_radius_arcsec, scale_bright, scale_faint
        )
        r_pix = r_arcsec * pix_per_arcsec
        cx, cy = xs[i], ys[i]
        if not np.isfinite(cx) or not np.isfinite(cy):
            continue
        rr2 = (yy - cy) ** 2 + (xx - cx) ** 2
        mask[rr2 <= r_pix**2] = 0.0

    fits.writeto(mask_path, mask, header=header, overwrite=True)
    logging.info(
        f"Mask written: {mask_path} (MAG='{sel_mag}', base_radius_arcsec={base_radius_arcsec})"
    )
