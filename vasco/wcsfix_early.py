from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple, List

import numpy as np

from vasco.utils.stilts_wrapper import stilts_xmatch


@dataclass
class WcsFixConfig:
    # Bootstrap match radius (arcsec) to build tie points from local Gaia cache
    bootstrap_radius_arcsec: float = 5.0
    # Polynomial degree (2 => 2D quadratic)
    degree: int = 2
    # Minimum tie points required
    min_matches: int = 20
    # Robust fit iterations
    robust_iters: int = 3
    # Residual clipping (arcsec)
    clip_arcsec_min: float = 1.5
    clip_arcsec_sigma: float = 3.0
    clip_arcsec_max: float = 10.0
    # Output filename
    out_name: str = "sextractor_pass2.wcsfix.csv"
    # Status JSON name
    status_name: str = "wcsfix_status.json"


def _wrap_deg_pm180(x: np.ndarray) -> np.ndarray:
    """Wrap degrees to [-180, +180)."""
    return (x + 180.0) % 360.0 - 180.0


def _wrap_deg_0_360(x: np.ndarray) -> np.ndarray:
    """Wrap degrees to [0, 360)."""
    return x % 360.0


def _deg_to_arcsec(d: np.ndarray) -> np.ndarray:
    return d * 3600.0


def _sep_arcsec(ra1_deg: np.ndarray, dec1_deg: np.ndarray, ra2_deg: np.ndarray, dec2_deg: np.ndarray) -> np.ndarray:
    """
    Great-circle separation in arcsec (vectorized).
    """
    ra1 = np.radians(ra1_deg)
    dec1 = np.radians(dec1_deg)
    ra2 = np.radians(ra2_deg)
    dec2 = np.radians(dec2_deg)
    s = 2.0 * np.arcsin(
        np.sqrt(
            np.sin((dec2 - dec1) / 2.0) ** 2
            + np.cos(dec1) * np.cos(dec2) * np.sin((ra2 - ra1) / 2.0) ** 2
        )
    )
    return np.degrees(s) * 3600.0


def _pick_sex_radec_cols(header: List[str]) -> Tuple[str, str]:
    cols = set(header)
    # Prefer windowed world coords for stability; raw as fallback
    if "ALPHAWIN_J2000" in cols and "DELTAWIN_J2000" in cols:
        return "ALPHAWIN_J2000", "DELTAWIN_J2000"
    if "ALPHA_J2000" in cols and "DELTA_J2000" in cols:
        return "ALPHA_J2000", "DELTA_J2000"
    if "X_WORLD" in cols and "Y_WORLD" in cols:
        return "X_WORLD", "Y_WORLD"
    raise ValueError("Could not find SExtractor RA/Dec columns in header.")


def _read_csv_header(path: Path) -> List[str]:
    with path.open(newline="", encoding="utf-8", errors="ignore") as f:
        r = csv.reader(f)
        hdr = next(r, [])
    return [h.strip().lstrip("﻿") for h in hdr]


def _bootstrap_match(tile_dir: Path,
                     sex_csv: Path,
                     gaia_csv: Path,
                     ra_col: str,
                     dec_col: str,
                     cfg: WcsFixConfig) -> Path:
    """
    Create a lightweight bootstrap match file between SExtractor CSV and Gaia neighbourhood cache.
    Uses STILTS to avoid pulling big data into memory.
    """
    out = tile_dir / "catalogs" / "_wcsfix_bootstrap_gaia.csv"
    out.parent.mkdir(parents=True, exist_ok=True)

    # Join 1and2 with "best" match within bootstrap radius
    stilts_xmatch(
        str(sex_csv),
        str(gaia_csv),
        str(out),
        ra1=ra_col,
        dec1=dec_col,
        ra2="ra",
        dec2="dec",
        radius_arcsec=float(cfg.bootstrap_radius_arcsec),
        join_type="1and2",
        find="best",
        ofmt="csv",
    )
    return out


def _poly_features(dra_deg: np.ndarray, ddec_deg: np.ndarray, degree: int) -> np.ndarray:
    """
    Build 2D polynomial feature matrix up to degree 2 (or 1).
    For degree=2: [1, x, y, x^2, x*y, y^2]
    For degree=1: [1, x, y]
    """
    x = dra_deg
    y = ddec_deg
    if degree <= 1:
        return np.column_stack([np.ones_like(x), x, y])
    # degree 2
    return np.column_stack([np.ones_like(x), x, y, x * x, x * y, y * y])


def _robust_fit_offsets(dra_det: np.ndarray,
                        ddec_det: np.ndarray,
                        dra_off: np.ndarray,
                        ddec_off: np.ndarray,
                        degree: int,
                        cfg: WcsFixConfig) -> Tuple[np.ndarray, np.ndarray, dict]:
    """
    Robust least squares fit of offsets (Gaia - detection) as a polynomial in (dra_det, ddec_det).
    Returns coefficients for RA offset and Dec offset.
    """
    X = _poly_features(dra_det, ddec_det, degree=degree)

    # Start with all rows valid
    mask = np.isfinite(dra_off) & np.isfinite(ddec_off) & np.all(np.isfinite(X), axis=1)

    info = {"iters": 0, "kept": int(mask.sum()), "dropped": int((~mask).sum())}

    if mask.sum() < cfg.min_matches:
        raise RuntimeError(f"too few usable matches after NaN filter ({mask.sum()} < {cfg.min_matches})")

    for it in range(cfg.robust_iters):
        Xm = X[mask]
        y_ra = dra_off[mask]
        y_de = ddec_off[mask]

        # Fit via least squares
        coef_ra, *_ = np.linalg.lstsq(Xm, y_ra, rcond=None)
        coef_de, *_ = np.linalg.lstsq(Xm, y_de, rcond=None)

        # Compute residuals in arcsec
        pred_ra = X @ coef_ra
        pred_de = X @ coef_de
        res_arcsec = _deg_to_arcsec(np.sqrt((dra_off - pred_ra) ** 2 + (ddec_off - pred_de) ** 2))

        # Robust sigma estimate (MAD)
        r = res_arcsec[np.isfinite(res_arcsec)]
        if r.size == 0:
            break
        med = np.median(r)
        mad = np.median(np.abs(r - med))
        sigma = 1.4826 * mad if mad > 0 else (np.std(r) if r.size > 10 else 0.0)

        # Clip threshold
        thr = max(cfg.clip_arcsec_min, cfg.clip_arcsec_sigma * sigma) if sigma > 0 else cfg.clip_arcsec_max
        thr = min(thr, cfg.clip_arcsec_max)

        new_mask = mask & (res_arcsec <= thr)
        info.update({"iters": it + 1, "sigma_arcsec": float(sigma), "clip_thr_arcsec": float(thr),
                     "kept": int(new_mask.sum()), "dropped": int((~new_mask).sum())})

        # Stop if stable
        if new_mask.sum() == mask.sum():
            mask = new_mask
            break

        mask = new_mask

        if mask.sum() < cfg.min_matches:
            raise RuntimeError(f"too few matches after robust clipping ({mask.sum()} < {cfg.min_matches})")

    # Final fit with final mask
    Xm = X[mask]
    y_ra = dra_off[mask]
    y_de = ddec_off[mask]
    coef_ra, *_ = np.linalg.lstsq(Xm, y_ra, rcond=None)
    coef_de, *_ = np.linalg.lstsq(Xm, y_de, rcond=None)

    return coef_ra, coef_de, info


def ensure_wcsfix_catalog(tile_dir: Path,
                          sex_csv: Path,
                          gaia_csv: Path,
                          *,
                          center: Optional[Tuple[float, float]] = None,
                          cfg: Optional[WcsFixConfig] = None,
                          force: bool = False) -> Tuple[Path, dict]:
    """
    Ensure a WCSFIX-augmented SExtractor catalog exists for this tile.

    Inputs:
      - sex_csv: catalogs/sextractor_pass2.csv (big)
      - gaia_csv: catalogs/gaia_neighbourhood.csv (cache)

    Output:
      - catalogs/sextractor_pass2.wcsfix.csv containing all original columns + RA_corr,Dec_corr
      - catalogs/wcsfix_status.json describing success/failure and fit diagnostics

    Returns: (path_used_for_downstream, status_dict)
    """
    tile_dir = Path(tile_dir)
    sex_csv = Path(sex_csv)
    gaia_csv = Path(gaia_csv)
    cfg = cfg or WcsFixConfig()

    out_csv = tile_dir / "catalogs" / cfg.out_name
    status_path = tile_dir / "catalogs" / cfg.status_name
    status: dict = {
        "ok": False,
        "reason": None,
        "sex_csv": str(sex_csv),
        "gaia_csv": str(gaia_csv),
        "out_csv": str(out_csv),
        "bootstrap_radius_arcsec": cfg.bootstrap_radius_arcsec,
        "degree": cfg.degree,
        "min_matches": cfg.min_matches,
    }

    # If already exists and not forcing, use it
    if out_csv.exists() and out_csv.stat().st_size > 0 and not force:
        status.update({"ok": True, "reason": "cached"})
        try:
            status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
        except Exception:
            pass
        return out_csv, status

    # Preconditions
    if not sex_csv.exists() or sex_csv.stat().st_size == 0:
        status.update({"ok": False, "reason": "missing sextractor_pass2.csv"})
        try:
            status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
        except Exception:
            pass
        return sex_csv, status

    if not gaia_csv.exists() or gaia_csv.stat().st_size == 0:
        status.update({"ok": False, "reason": "missing gaia_neighbourhood.csv"})
        try:
            status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
        except Exception:
            pass
        return sex_csv, status

    # Determine center if not provided
    if center is None:
        # Try to parse from tile folder name: tile-RA259.267-DEC+51.582
        name = tile_dir.name
        try:
            if name.startswith("tile-RA") and "-DEC" in name:
                ra0 = float(name[len("tile-RA"): name.index("-DEC")])
                dec0 = float(name[name.index("-DEC") + len("-DEC"):])
                center = (ra0, dec0)
        except Exception:
            center = None

    if center is None:
        status.update({"ok": False, "reason": "missing tile center"})
        try:
            status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
        except Exception:
            pass
        return sex_csv, status

    ra0, dec0 = float(center[0]), float(center[1])
    status.update({"tile_center_ra": ra0, "tile_center_dec": dec0})

    # Determine sextractor coordinate columns
    try:
        hdr = _read_csv_header(sex_csv)
        ra_col, dec_col = _pick_sex_radec_cols(hdr)
    except Exception as e:
        status.update({"ok": False, "reason": f"cannot pick sextractor radec cols: {e}"})
        try:
            status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
        except Exception:
            pass
        return sex_csv, status

    status.update({"sex_ra_col": ra_col, "sex_dec_col": dec_col})

    # Bootstrap match
    try:
        boot = _bootstrap_match(tile_dir, sex_csv, gaia_csv, ra_col, dec_col, cfg)
    except Exception as e:
        status.update({"ok": False, "reason": f"bootstrap match failed: {e}"})
        try:
            status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
        except Exception:
            pass
        return sex_csv, status

    # Read tie points
    try:
        with boot.open(newline="", encoding="utf-8", errors="ignore") as f:
            r = csv.DictReader(f)
            rows = list(r)
    except Exception as e:
        status.update({"ok": False, "reason": f"cannot read bootstrap csv: {e}"})
        try:
            status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
        except Exception:
            pass
        return sex_csv, status

    if not rows:
        status.update({"ok": False, "reason": "bootstrap match produced 0 rows"})
        try:
            status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
        except Exception:
            pass
        return sex_csv, status

    # Extract numeric arrays
    det_ra = []
    det_de = []
    g_ra = []
    g_de = []

    for row in rows:
        try:
            dra = float(row[ra_col])
            dde = float(row[dec_col])
            gra = float(row["ra"])
            gde = float(row["dec"])
        except Exception:
            continue
        det_ra.append(dra)
        det_de.append(dde)
        g_ra.append(gra)
        g_de.append(gde)

    det_ra = np.asarray(det_ra, dtype=float)
    det_de = np.asarray(det_de, dtype=float)
    g_ra = np.asarray(g_ra, dtype=float)
    g_de = np.asarray(g_de, dtype=float)

    n = det_ra.size
    status.update({"bootstrap_rows": int(len(rows)), "tie_points": int(n)})

    if n < cfg.min_matches:
        status.update({"ok": False, "reason": f"too few tie points ({n} < {cfg.min_matches})"})
        try:
            status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
        except Exception:
            pass
        return sex_csv, status

    # Work in local (wrapped) coordinates around tile center
    dra_det = _wrap_deg_pm180(det_ra - ra0)
    ddec_det = (det_de - dec0)

    # Offsets to Gaia (wrapped small RA offset)
    dra_off = _wrap_deg_pm180(g_ra - det_ra)
    ddec_off = (g_de - det_de)

    # Robust fit
    try:
        coef_ra, coef_de, fit_info = _robust_fit_offsets(
            dra_det, ddec_det, dra_off, ddec_off,
            degree=cfg.degree, cfg=cfg
        )
    except Exception as e:
        status.update({"ok": False, "reason": f"fit failed: {e}"})
        try:
            status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
        except Exception:
            pass
        return sex_csv, status

    status.update({
        "fit": fit_info,
        "coef_ra": [float(x) for x in coef_ra.tolist()],
        "coef_de": [float(x) for x in coef_de.tolist()],
    })

    # Apply to full SExtractor catalog and write output
    try:
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        tmp = out_csv.with_suffix(out_csv.suffix + ".tmp")

        with sex_csv.open(newline="", encoding="utf-8", errors="ignore") as fin, tmp.open("w", newline="", encoding="utf-8") as fout:
            rdr = csv.DictReader(fin)
            fieldnames = list(rdr.fieldnames or [])
            # Avoid duplicate columns if re-running
            for extra in ("RA_corr", "Dec_corr"):
                if extra not in fieldnames:
                    fieldnames.append(extra)

            w = csv.DictWriter(fout, fieldnames=fieldnames)
            w.writeheader()

            for row in rdr:
                try:
                    ra_det_row = float(row.get(ra_col, "nan"))
                    dec_det_row = float(row.get(dec_col, "nan"))
                except Exception:
                    ra_det_row = float("nan")
                    dec_det_row = float("nan")

                if not (math.isfinite(ra_det_row) and math.isfinite(dec_det_row)):
                    row["RA_corr"] = ""
                    row["Dec_corr"] = ""
                    w.writerow(row)
                    continue

                dra = _wrap_deg_pm180(np.array([ra_det_row - ra0], dtype=float))[0]
                dde = (dec_det_row - dec0)

                X = _poly_features(np.array([dra], dtype=float), np.array([dde], dtype=float), degree=cfg.degree)
                off_ra = float(X @ coef_ra)
                off_de = float(X @ coef_de)

                ra_corr = _wrap_deg_0_360(ra_det_row + off_ra)
                dec_corr = dec_det_row + off_de

                row["RA_corr"] = f"{ra_corr:.10f}"
                row["Dec_corr"] = f"{dec_corr:.10f}"
                w.writerow(row)

        tmp.replace(out_csv)

        status.update({"ok": True, "reason": "wrote", "out_rows": "unknown"})
        try:
            status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
        except Exception:
            pass

        return out_csv, status

    except Exception as e:
        status.update({"ok": False, "reason": f"write failed: {e}"})
        try:
            status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
        except Exception:
            pass
        return sex_csv, status