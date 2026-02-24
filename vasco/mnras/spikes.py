# MNRAS-2022 arcsec spike rule: updated Jan 2026
from __future__ import annotations

import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, List, Optional, Dict, Tuple

# ---------------- angle helpers ----------------
def angsep_arcsec(ra1_deg: float, dec1_deg: float, ra2_deg: float, dec2_deg: float) -> float:
    """Great-circle separation in arcsec (accurate for small/large angles)."""
    ra1 = math.radians(ra1_deg); dec1 = math.radians(dec1_deg)
    ra2 = math.radians(ra2_deg); dec2 = math.radians(dec2_deg)
    s = 2 * math.asin(math.sqrt(
        math.sin((dec2 - dec1) / 2) ** 2 +
        math.cos(dec1) * math.cos(dec2) * math.sin((ra2 - ra1) / 2) ** 2
    ))
    return math.degrees(s) * 3600.0

def angsep_arcmin(ra1_deg: float, dec1_deg: float, ra2_deg: float, dec2_deg: float) -> float:
    # kept for compatibility; arcsec is the primary unit used by the paper
    return angsep_arcsec(ra1_deg, dec1_deg, ra2_deg, dec2_deg) / 60.0

# ---------------- data classes ----------------
@dataclass
class BrightStar:
    ra: float
    dec: float
    rmag: float  # proxy magnitude (PS1 rMeanPSFMag or MAPS magdO, etc.)

# ---------------- PS1 bright-star fetch ----------------
def fetch_bright_ps1(
    ra_deg: float,
    dec_deg: float,
    radius_arcmin: float = 90.0,
    rmag_max: float = 16.0,
    mindetections: int = 2,
) -> List[BrightStar]:
    """Fetch bright stars from Pan-STARRS DR2 (MAST mean.csv API).

    Returns list of BrightStar(ra, dec, rmag) with rMeanPSFMag <= rmag_max
    within the given radius.
    """
    import urllib.parse, urllib.request, ssl, csv as _csv
    radius_deg = float(radius_arcmin) / 60.0
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
    out: List[BrightStar] = []
    with urllib.request.urlopen(url, context=ctx, timeout=120) as resp:
        text = resp.read().decode("utf-8", "replace")
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

# ---------------- spike rules ----------------
@dataclass
class SpikeRuleConst:
    # Reject if bright-star magnitude <= const_max_mag
    const_max_mag: float

@dataclass
class SpikeRuleLine:
    # Reject if bright-star magnitude < a * d_arcsec + b (strict inequality by convention)
    # Paper form: Rmag <= -0.09 * d_arcsec + 15.3 ; equality treated as KEEP to avoid over-rejecting
    a: float  # typically -0.09
    b: float  # typically 15.3

@dataclass
class SpikeConfig:
    rmag_key: str = "rMeanPSFMag"
    rules: List[Any] | None = None
    search_radius_arcmin: float = 90.0
    rmag_max_catalog: float = 16.0

    @staticmethod
    def from_yaml(path: Path) -> "SpikeConfig":
        import yaml
        cfg = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        s = cfg.get("spikes", {})
        rules: List[Any] = []
        for r in s.get("rules", []):
            if r.get("type") == "const":
                rules.append(SpikeRuleConst(const_max_mag=float(r["max_mag"])))
            elif r.get("type") == "line":
                rules.append(SpikeRuleLine(a=float(r["a"]), b=float(r["b"])))
        return SpikeConfig(
            rmag_key=s.get("mag_key", "rMeanPSFMag"),
            rules=rules,
            search_radius_arcmin=float(s.get("search_radius_arcmin", 90.0)),
            rmag_max_catalog=float(s.get("rmag_max_catalog", 16.0)),
        )

# ---------------- apply rules (LEGACY scalar) ----------------
def apply_spike_cuts_scalar(
    tile_rows: Iterable[Dict[str, Any]],
    bright: List[BrightStar],
    cfg: SpikeConfig,
    src_ra_key: str = "ALPHA_J2000",
    src_dec_key: str = "DELTA_J2000",
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Return (kept_rows, rejected_rows) with reason annotations, paper-aligned.

    Legacy scalar implementation (O(N_det * N_bright)). Kept for fallback/debug.
    """
    kept: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []

    for r in tile_rows:
        # Parse detection coordinates
        try:
            ra = float(r[src_ra_key]); dec = float(r[src_dec_key])
        except Exception:
            r2 = dict(r); r2["spike_reason"] = "no_wcs"
            kept.append(r2)
            continue

        # Find nearest bright star (by ARCSECONDS, per paper)
        dmin_arcsec = float("inf")
        m_near: Optional[float] = None
        for b in bright:
            da = angsep_arcsec(ra, dec, b.ra, b.dec)
            if da < dmin_arcsec:
                dmin_arcsec = da
                m_near = b.rmag

        # If none within search radius (convert arcmin->arcsec), keep
        if not (dmin_arcsec <= cfg.search_radius_arcmin * 60.0 and m_near is not None):
            r2 = dict(r); r2["spike_reason"] = ""
            kept.append(r2)
            continue

        reject = False
        reasons: List[str] = []

        for rule in (cfg.rules or []):
            if isinstance(rule, SpikeRuleConst):
                # equality should reject (<=)
                if m_near <= rule.const_max_mag:
                    reject = True
                    reasons.append(f"CONST(m*={m_near:.2f} <= {rule.const_max_mag:.2f})")

            elif isinstance(rule, SpikeRuleLine):
                # strict inequality on the line rule to keep equality
                thresh = (rule.a) * dmin_arcsec + rule.b
                if m_near < thresh:
                    reject = True
                    reasons.append(
                        f"LINE(m*={m_near:.2f} < {rule.a:.3f}*{dmin_arcsec:.1f}+{rule.b:.2f}={thresh:.2f})"
                    )

        r2 = dict(r)
        r2["spike_d_arcmin"] = round(dmin_arcsec / 60.0, 3)
        r2["spike_m_near"] = m_near if m_near is not None else float("nan")
        r2["spike_reason"] = ";".join(reasons) if reject else ""

        if reject:
            rejected.append(r2)
        else:
            kept.append(r2)

    return kept, rejected

# ---------------- apply rules (DEFAULT router) ----------------
def apply_spike_cuts(
    tile_rows: Iterable[Dict[str, Any]],
    bright: List[BrightStar],
    cfg: SpikeConfig,
    src_ra_key: str = "ALPHA_J2000",
    src_dec_key: str = "DELTA_J2000",
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Production entrypoint.

    Default: vectorized implementation (fast).
    Fallback: set VASCO_SPIKES_ENGINE=scalar to use legacy scalar engine.
    """
    engine = os.getenv("VASCO_SPIKES_ENGINE", "vectorized").strip().lower()

    if os.getenv("VASCO_SPIKES_DEBUG"):
        print(f"[spikes] engine={engine}")

    if engine in ("scalar", "legacy", "slow"):
        return apply_spike_cuts_scalar(tile_rows, bright, cfg, src_ra_key=src_ra_key, src_dec_key=src_dec_key)

    # Lazy import avoids circular import issues.
    from vasco.mnras.apply_spike_cuts_vectorized import apply_spike_cuts_vectorized
    return apply_spike_cuts_vectorized(tile_rows, bright, cfg, src_ra_key=src_ra_key, src_dec_key=src_dec_key)

# ---------------- ECSV helpers (compatibility) ----------------
def read_ecsv(path: Path) -> List[Dict[str, Any]]:
    """Read an ECSV or fallback to LDAC if needed; returns list of dict rows."""
    from astropy.table import Table
    p = str(path)
    try:
        tab = Table.read(p, format="ascii.ecsv")
    except Exception as e:
        ldac = path.with_name("pass2.ldac")
        if ldac.exists():
            try:
                from astropy.io import fits
                with fits.open(str(ldac)) as hdul:
                    tab = None
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

    rows: List[Dict[str, Any]] = []
    for row in tab:
        d: Dict[str, Any] = {}
        for col in tab.colnames:
            val = row[col]
            try:
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

# ---------------- optional USNO-B mask placeholder ----------------
def apply_usno_b1_mask(catalog_path, ra, dec, radius_deg=0.5):
    """Placeholder for USNO-B1.0-based masking if you want literal paper mags.
    Currently not implemented; PS1 fetch is the default source for spike rules.
    """
    print(f"[INFO] (placeholder) USNO-B1.0 mask around RA={ra}, Dec={dec}, R={radius_deg} deg")