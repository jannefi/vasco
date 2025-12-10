#!/usr/bin/env python3
"""
Create a small synthetic FITS image with minimal WCS so the pipeline can be tested
without network downloads.
"""
import os
import numpy as np
from astropy.io import fits
from astropy.wcs import WCS


def ensure_synthetic_fits(
    path: str, ra: float, dec: float, size_arcmin: int, pixel_scale_arcsec: float = 1.7
):
    # Compute image dimensions from field size and pixel scale
    pix_per_arcmin = 60.0 / pixel_scale_arcsec
    side_pix = int(max(64, round(size_arcmin * pix_per_arcmin)))
    ny, nx = side_pix, side_pix

    # Create background noise
    rng = np.random.default_rng(42)
    img = rng.normal(loc=1000.0, scale=5.0, size=(ny, nx)).astype(np.float32)

    # Plant a few synthetic point sources
    for y, x, amp, sigma in [
        (ny // 3, nx // 3, 500.0, 1.5),
        (ny // 2, nx // 2, 800.0, 2.0),
        (2 * ny // 3, 2 * nx // 3, 300.0, 1.2),
    ]:
        yy, xx = np.meshgrid(np.arange(ny), np.arange(nx), indexing="ij")
        r2 = (yy - y) ** 2 + (xx - x) ** 2
        img += amp * np.exp(-0.5 * r2 / (sigma**2))

    # Build minimal WCS
    w = WCS(naxis=2)
    # Pixel scale in deg/pix
    scale_deg = pixel_scale_arcsec / 3600.0
    w.wcs.crpix = [nx / 2.0, ny / 2.0]
    w.wcs.cd = [[-scale_deg, 0.0], [0.0, scale_deg]]
    w.wcs.crval = [ra, dec]
    w.wcs.ctype = ["RA---TAN", "DEC--TAN"]

    h = w.to_header()
    h["BUNIT"] = "adu"
    h["EXPTIME"] = 30.0

    os.makedirs(os.path.dirname(path), exist_ok=True)
    fits.writeto(path, img, header=h, overwrite=True)
