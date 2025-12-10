from __future__ import annotations
from pathlib import Path
import csv
import requests

__all__ = [
    "fetch_gaia_neighbourhood",
    "fetch_ps1_neighbourhood",
]

# ---------------------------------------------------------------
# Gaia DR3 via CDS/VizieR (normalize to ra/dec; skip unit rows)
# ---------------------------------------------------------------
_VIZIER_TSV = "https://vizier.u-strasbg.fr/viz-bin/asu-tsv"
_DEF_GAIA_COLS = [
    "RA_ICRS",
    "DE_ICRS",
    "Gmag",
    "BPmag",
    "RPmag",
    "pmRA",
    "pmDE",
    "Plx",
    "_r",
]


def fetch_gaia_neighbourhood(
    tile_dir: Path | str,
    ra_deg: float,
    dec_deg: float,
    radius_arcmin: float,
    *,
    max_rows: int = 200000,
    timeout: float = 60.0,
) -> Path:
    """Fetch Gaia DR3 neighborhood via VizieR TSV -> CSV; map RA/Dec and filter unit rows."""
    tile_dir = Path(tile_dir)
    out = tile_dir / "catalogs" / "gaia_neighbourhood.csv"
    out.parent.mkdir(parents=True, exist_ok=True)

    params = {
        "-source": "I/355/gaiadr3",
        "-c": f"{ra_deg} {dec_deg}",
        "-c.r": f"{radius_arcmin:.6f}",
        "-out.max": str(max_rows),
        "-out.add": "_r",
        "-out.form": "dec",
        "-sort": "_r",
        "-out": ",".join(_DEF_GAIA_COLS),
    }
    r = requests.get(_VIZIER_TSV, params=params, timeout=timeout)
    r.raise_for_status()

    # Keep non-empty, non-comment lines
    lines = [row for row in r.text.splitlines() if row and not row.startswith("#")]
    if not lines:
        out.write_text("ra,dec")
        return out

    header = lines[0].split("	")
    data_rows = [ln.split("	") for ln in lines[1:]]
    colmap = {name: idx for idx, name in enumerate(header)}
    keep = [c for c in _DEF_GAIA_COLS if c in colmap]

    with out.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            ["ra" if c == "RA_ICRS" else ("dec" if c == "DE_ICRS" else c) for c in keep]
        )
        for row in data_rows:
            try:
                # Ensure RA/DEC are numeric (skip unit rows like 'deg')
                float(row[colmap["RA_ICRS"]])
                float(row[colmap["DE_ICRS"]])
            except Exception:
                continue
            w.writerow([row[colmap[c]] for c in keep])
    return out


# ---------------------------------------------------------------
# PS1 DR2 via MAST Catalogs API (mean table, explicit columns)
# ---------------------------------------------------------------


def fetch_ps1_neighbourhood(
    tile_dir: Path | str,
    ra_deg: float,
    dec_deg: float,
    radius_arcmin: float,
    *,
    max_records: int = 50000,
    timeout: float = 60.0,
) -> Path:
    """Fetch PS1 DR2 mean-table neighborhood CSV with explicit columns and progress logs.

    Honors environment variables:
        VASCO_PS1_RADIUS_DEG  (override radius in degrees)
        VASCO_PS1_TIMEOUT     (seconds per attempt)
        VASCO_PS1_ATTEMPTS    (retry attempts)
        VASCO_PS1_COLUMNS     (comma-separated override of requested columns)
    """
    import os, time

    tile_dir = Path(tile_dir)
    out = tile_dir / "catalogs" / "ps1_neighbourhood.csv"
    out.parent.mkdir(parents=True, exist_ok=True)

    # Radius (degrees). Cap to 0.5 deg; allow override for dev/testing.
    radius_deg = None
    _r = os.getenv("VASCO_PS1_RADIUS_DEG")
    if _r:
        try:
            radius_deg = float(_r)
        except Exception:
            radius_deg = None
    if radius_deg is None:
        radius_deg = min(float(radius_arcmin) / 60.0, 0.5)

    base = "https://catalogs.mast.stsci.edu/api/v0.1/panstarrs"
    url = base + "/dr2/mean.csv"

    _timeout = float(os.getenv("VASCO_PS1_TIMEOUT", timeout))
    _attempts = int(os.getenv("VASCO_PS1_ATTEMPTS", "3"))

    default_cols = [
        "objID",
        "raMean",
        "decMean",
        "nDetections",
        "ng",
        "nr",
        "ni",
        "nz",
        "ny",
        "gMeanPSFMag",
        "rMeanPSFMag",
        "iMeanPSFMag",
        "zMeanPSFMag",
        "yMeanPSFMag",
    ]
    _cols_override = os.getenv("VASCO_PS1_COLUMNS")
    cols = (
        [c.strip() for c in _cols_override.split(",")]
        if _cols_override
        else default_cols
    )

    params = {
        "ra": "{:.8f}".format(float(ra_deg)),
        "dec": "{:.8f}".format(float(dec_deg)),
        "radius": "{:.8f}".format(float(radius_deg)),
        "nDetections.gte": "1",
        "pagesize": str(int(max_records)),
        "format": "csv",
        "columns": "[" + ",".join(cols) + "]",
    }

    def _try_once():
        t0 = time.time()
        print(
            "[POST][PS1] GET {} (timeout={}s, radius={})".format(
                url, _timeout, params["radius"]
            )
        )
        r = requests.get(url, params=params, timeout=_timeout)
        r.raise_for_status()
        dt = time.time() - t0
        print("[POST][PS1] OK in {:.2f}s ({} bytes)".format(dt, len(r.content)))
        return r

    last_exc = None
    for k in range(1, _attempts + 1):
        try:
            r = _try_once()
            out.write_bytes(r.content)
            return out
        except Exception as e:
            last_exc = e
            print("[POST][WARN] PS1 attempt {}/{} failed: {}".format(k, _attempts, e))
            if k < _attempts:
                time.sleep(min(10, 1.5**k))
    raise last_exc
