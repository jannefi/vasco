from __future__ import annotations
from pathlib import Path
from typing import Optional

# Astroquery Vizier
from astroquery.vizier import Vizier
from astropy.coordinates import SkyCoord
import astropy.units as u

USNOB_CAT = "I/284"  # VizieR catalog code for USNO-B1.0

# Default column set (keep payload modest, but useful for cross-match/QA)
_USNOB_COLUMNS = [
    "USNO-B1.0",  # id
    "RAJ2000",
    "DEJ2000",  # degrees J2000
    "B1mag",
    "R1mag",
    "B2mag",
    "R2mag",
    "Imag",
    "pmRA",
    "pmDE",
]


def fetch_usnob_neighbourhood(
    tile_dir: Path,
    ra_deg: float,
    dec_deg: float,
    radius_arcmin: float,
    *,
    row_limit: int = 20000,
    columns: Optional[list[str]] = None,
) -> Path:
    """
    Fetch USNO-B1.0 (VizieR I/284) sources in a circular neighbourhood
    around (ra_deg, dec_deg) with the given radius (arcmin), and write CSV
    under `tile_dir/catalogs/usnob_neighbourhood.csv`.

    Returns the path to the written CSV.
    """
    tile_dir = Path(tile_dir)
    cat_dir = tile_dir / "catalogs"
    cat_dir.mkdir(parents=True, exist_ok=True)
    out_csv = cat_dir / "usnob_neighbourhood.csv"

    # Configure Vizier
    Vizier.ROW_LIMIT = int(row_limit) if row_limit and row_limit > 0 else -1
    cols = columns or _USNOB_COLUMNS
    viz = Vizier(columns=cols)

    # SkyCoord and radius
    pos = SkyCoord(ra_deg * u.deg, dec_deg * u.deg, frame="icrs")
    radius = (radius_arcmin * u.arcmin).to(u.deg)

    # Query region
    tables = viz.query_region(pos, radius=radius, catalog=USNOB_CAT)

    # No rows -> write an empty CSV with header for consistency
    if len(tables) == 0 or len(tables[0]) == 0:
        # Construct header from requested columns
        header = ",".join(cols)
        out_csv.write_text(header + "", encoding="utf-8")
        return out_csv

    # First table is the main result
    t = tables[0]

    # Ensure we have the expected columns; if not, write whatever we got
    t.write(out_csv, format="csv", overwrite=True)
    return out_csv
