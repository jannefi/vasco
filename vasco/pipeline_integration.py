"""Thin integration layer to wire downloader preference, bright-star masking,
and STILTS cross-matching into callable functions for tests/CLIs.
"""

from __future__ import annotations
from pathlib import Path
from typing import Optional

# Existing modules from your repo
from vasco.downloader import get_image_service
from vasco.mnras.spikes import apply_usno_b1_mask
from vasco.utils.stilts_wrapper import stilts_xmatch


def prefer_image_service(service: str = "stsci") -> None:
    """Emit info/warnings for the selected image service.
    This does not perform a download; it only centralizes the policy/logging.
    """
    get_image_service(service)


def mask_bright_stars_usno_b1(
    catalog_path: str | Path, ra_deg: float, dec_deg: float, radius_deg: float = 0.5
) -> None:
    """Call the USNO-B1.0 bright-star mask placeholder.
    Replace with the real implementation when available.
    """
    apply_usno_b1_mask(str(catalog_path), ra_deg, dec_deg, radius_deg=radius_deg)


def xmatch_with_stilts(
    table1: str | Path,
    table2: str | Path,
    out_table: str | Path,
    join_type: str = "1and2",
    radius_arcsec: float = 1.0,
) -> None:
    """Invoke STILTS sky cross-match via wrapper.
    Parameters map 1:1 to the wrapper for clarity.
    """
    stilts_xmatch(
        str(table1),
        str(table2),
        str(out_table),
        join_type=join_type,
        radius_arcsec=radius_arcsec,
    )
