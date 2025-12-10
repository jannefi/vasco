from __future__ import annotations
import csv
import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple, Optional, Dict, Any

# --- helpers ---


def angsep_arcmin(
    ra1_deg: float, dec1_deg: float, ra2_deg: float, dec2_deg: float
) -> float:
    """Angular separation in arcmin (accurate for small angles)."""
    import math

    ra1 = math.radians(ra1_deg)
    dec1 = math.radians(dec1_deg)
    ra2 = math.radians(ra2_deg)
    dec2 = math.radians(dec2_deg)
    s = 2 * math.asin(
        math.sqrt(
            math.sin((dec2 - dec1) / 2) ** 2
            + math.cos(dec1) * math.cos(dec2) * math.sin((ra2 - ra1) / 2) ** 2
        )
    )
    return math.degrees(s) * 60.0


@dataclass
class BrightStar:
    ra: float
    dec: float
    rmag: float


# --- PS1 fetch ---


def fetch_bright_ps1(
    ra_deg: float,
    dec_deg: float,
    radius_arcmin: float = 35.0,
    rmag_max: float = 16.0,
    mindetections: int = 2,
) -> List[BrightStar]:
    """
    Fetch bright stars from Pan-STARRS DR2 (MAST catalogs API).
    Returns list of BrightStar(ra,dec,rmag) with rMeanPSFMag <= rmag_max within radius.
    """
    import urllib.parse, urllib.request, ssl, csv as _csv

    radius_deg = radius_arcmin / 60.0
    base = "https://catalogs.mast.stsci.edu/api/v0.1/panstarrs/dr2/mean.csv"
    columns = ["objName", "raMean", "decMean", "rMeanPSFMag", "nDetections"]
    params = {
        "ra": f"{ra_deg:.8f}",
        "dec": f"{dec_deg:.8f}",
        "radius": f"{radius_deg:.8f}",
        "nDetections.gte": str(mindetections),
        "columns": "[" + ",".join(columns) + "]",
        "pagesize": "100000",
        "format": "csv",
    }
    url = base + "?" + urllib.parse.urlencode(params)
    ctx = ssl.create_default_context()
    ctx.set_ciphers("DEFAULT")
    # Fetch
    with urllib.request.urlopen(url, context=ctx, timeout=120) as resp:
        text = resp.read().decode("utf-8", "replace")
    # Parse CSV
    out: List[BrightStar] = []
    rdr = _csv.DictReader(text.splitlines())
    for row in rdr:
        try:
            rmag = float(row.get("rMeanPSFMag", "nan"))
            if not (rmag <= rmag_max):
                continue
            ra = float(row["raMean"])
            dec = float(row["decMean"])
            out.append(BrightStar(ra=ra, dec=dec, rmag=rmag))
        except Exception:
            continue
    return out


# --- Spike rules ---
@dataclass
class SpikeRuleConst:
    # reject if bright-star rmag <= const_max_mag
    const_max_mag: float


@dataclass
class SpikeRuleLine:
    # reject if bright-star rmag <= a*dist_arcmin + b (a negative slope in the slide)
    a: float
    b: float


@dataclass
class SpikeConfig:
    rmag_key: str = "rMeanPSFMag"
    rules: List[Any] = None
    search_radius_arcmin: float = 35.0
    rmag_max_catalog: float = 16.0

    @staticmethod
    def from_yaml(path: Path) -> "SpikeConfig":
        import yaml

        cfg = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        s = cfg.get("spikes", {})
        rules = []
        for r in s.get("rules", []):
            if r.get("type") == "const":
                rules.append(SpikeRuleConst(const_max_mag=float(r["max_mag"])))
            elif r.get("type") == "line":
                rules.append(SpikeRuleLine(a=float(r["a"]), b=float(r["b"])))
        return SpikeConfig(
            rmag_key=s.get("mag_key", "rMeanPSFMag"),
            rules=rules,
            search_radius_arcmin=float(s.get("search_radius_arcmin", 35.0)),
            rmag_max_catalog=float(s.get("rmag_max_catalog", 16.0)),
        )


# --- Apply spike cuts ---


def apply_spike_cuts(
    tile_rows: Iterable[Dict[str, Any]],
    bright: List[BrightStar],
    cfg: SpikeConfig,
    src_ra_key="ALPHA_J2000",
    src_dec_key="DELTA_J2000",
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Given tile detections and a list of bright stars, return (kept_rows, rejected_rows) with reason.
    Each input row must have world coords in ALPHA_J2000/DELTA_J2000 (or override keys).
    """
    kept = []
    rejected = []
    for r in tile_rows:
        try:
            ra = float(r[src_ra_key])
            dec = float(r[src_dec_key])
        except Exception:
            # cannot test; keep by default but mark reason
            r2 = dict(r)
            r2["spike_reason"] = "no_wcs"
            kept.append(r2)
            continue
        # find nearest bright star
        dmin = 1e9
        m_near = None
        for b in bright:
            d = angsep_arcmin(ra, dec, b.ra, b.dec)
            if d < dmin:
                dmin = d
                m_near = b.rmag
        # default: keep if no bright star within search radius
        if not (dmin <= cfg.search_radius_arcmin and m_near is not None):
            r2 = dict(r)
            r2["spike_reason"] = ""
            kept.append(r2)
            continue
        # evaluate rules
        reject = False
        reason = []
        for rule in cfg.rules:
            if isinstance(rule, SpikeRuleConst):
                if m_near <= rule.const_max_mag:
                    reject = True
                    reason.append(f"CONST(m*={m_near:.2f}<= {rule.const_max_mag:.2f})")
            elif isinstance(rule, SpikeRuleLine):
                thresh = rule.a * dmin + rule.b
                if m_near <= thresh:
                    reject = True
                    reason.append(
                        f"LINE(m*={m_near:.2f}<= {rule.a:.3f}*{dmin:.2f}+{rule.b:.2f}={thresh:.2f})"
                    )
        r2 = dict(r)
        if reject:
            r2["spike_reason"] = ";".join(reason)
            r2["spike_d_arcmin"] = round(dmin, 3)
            r2["spike_m_near"] = m_near
            rejected.append(r2)
        else:
            r2["spike_reason"] = ""
            r2["spike_d_arcmin"] = round(dmin, 3)
            r2["spike_m_near"] = m_near
            kept.append(r2)
    return kept, rejected


# --- I/O helpers ---


def read_ecsv(path: Path) -> List[Dict[str, Any]]:
    """Read an ECSV catalog robustly (accept pathlib.Path on older Astropy).
    Falls back to LDAC if ECSV read fails and a .ldac is present next to the file.
    """
    from astropy.table import Table

    p = str(path)
    try:
        tab = Table.read(p, format="ascii.ecsv")
    except Exception as e:
        # try LDAC fallback if a sibling pass2.ldac exists
        ldac = path.with_name("pass2.ldac")
        if ldac.exists():
            try:
                from astropy.io import fits

                # Minimal LDAC reader: read first binary table HDU
                with fits.open(str(ldac)) as hdul:
                    for hdu in hdul:
                        if getattr(hdu, "data", None) is not None:
                            tab = Table(hdu.data)
                            break
                if tab is None:
                    raise e
            except Exception:
                raise e
        else:
            raise e
    # Convert rows to plain dicts
    rows: List[Dict[str, Any]] = []
    for row in tab:
        d: Dict[str, Any] = {}
        for col in tab.colnames:
            val = row[col]
            try:
                # numpy scalar to python scalar
                val = val.item()
            except Exception:
                pass
            d[col] = val
        rows.append(d)
    return rows


def write_ecsv(rows: List[Dict[str, Any]], path: Path):
    from astropy.table import Table

    if not rows:
        path.write_text("", encoding="utf-8")
        return
    cols = sorted(rows[0].keys())
    tab = Table(rows=rows, names=cols)
    tab.write(str(path), format="ascii.ecsv", overwrite=True)


# Added: Bright star mask using USNO-B1.0 (placeholder)
def apply_usno_b1_mask(catalog_path, ra, dec, radius_deg=0.5):
    """Apply bright star mask using USNO-B1.0 catalog.
    catalog_path: path to USNO-B1.0 data (CSV or FITS)
    ra, dec: target coordinates in degrees
    radius_deg: search radius in degrees
    """
    print(
        f"[INFO] Applying USNO-B1.0 mask around RA={ra}, Dec={dec}, radius={radius_deg} deg"
    )
    # TODO: Implement actual query and masking logic
