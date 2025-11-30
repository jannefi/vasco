
from __future__ import annotations
import logging, shutil
from pathlib import Path
from typing import Tuple, Optional
from .utils.subprocess import run_cmd

logger = logging.getLogger("vasco")

class ToolMissingError(RuntimeError):
    pass

_REQUIRED_CONFIGS = [
    "sex_pass1.sex","sex_pass2.sex","default.param","default.conv","default.nnw","psfex.conf"
]

def _prepare_run_configs(config_root: str | Path, run_dir: str | Path) -> None:
    cfg_root = Path(config_root).resolve()
    rdir = Path(run_dir).resolve()
    rdir.mkdir(parents=True, exist_ok=True)
    for name in _REQUIRED_CONFIGS:
        src = cfg_root / name
        dst = rdir / name
        if not src.exists():
            if name == "default.nnw":
                continue
            raise FileNotFoundError(f"Missing config file: {src}")
        shutil.copy2(src, dst)
        logger.info("[INFO] Staged config: %s", dst.name)

def _ensure_fits_in_run_dir(fits_path: str | Path, run_dir: str | Path) -> str:
    src = Path(fits_path).resolve()
    rdir = Path(run_dir).resolve()
    rdir.mkdir(parents=True, exist_ok=True)
    dst = rdir / src.name
    if not dst.exists():
        shutil.copy2(src, dst)
        logger.info("[INFO] Copied FITS into run_dir: %s", dst.name)
    return src.name

def _assert_exists(path: Path, step: str) -> None:
    if not path.exists():
        raise RuntimeError(f"{step} did not produce expected file: {path}")

def _ensure_tool(tool: str) -> None:
    import shutil as _sh
    if _sh.which(tool) is None:
        raise ToolMissingError(f"Required tool '{tool}' not found in PATH.")

def _discover_psf_file(run_dir: Path) -> Path:
    preferred = run_dir / "pass1.psf"
    if preferred.exists():
        return preferred
    candidates = list(run_dir.glob("*.psf"))
    if not candidates:
        raise RuntimeError("PSFEx did not produce any .psf file in run directory")
    return max(candidates, key=lambda p: p.stat().st_mtime)

# -------------------- Existing two-pass PSF-aware extraction --------------------

def run_psf_two_pass(
    fits_path: str | Path,
    run_dir: str | Path,
    config_root: str | Path = "configs",
    sex_bin: str | None = None,
) -> Tuple[str, str, str]:
    rdir = Path(run_dir).resolve()
    rdir.mkdir(parents=True, exist_ok=True)
    import shutil as _sh
    sex_name = sex_bin or _sh.which('sex') or _sh.which('sextractor')
    if not sex_name:
        raise ToolMissingError("SExtractor not found on PATH (sex/sextractor)")
    _ensure_tool("psfex")

    logger.info("[INFO] Preparing configs in run directory ...")
    _prepare_run_configs(config_root, rdir)
    fits_basename = _ensure_fits_in_run_dir(fits_path, rdir)

    logger.info("[INFO] PASS 1: SExtractor starting ...")
    run_cmd([sex_name, fits_basename, '-c', 'sex_pass1.sex'], cwd=str(rdir))
    pass1_cat = rdir / 'pass1.ldac'
    _assert_exists(pass1_cat, "SExtractor PASS 1")
    logger.info("[INFO] PASS 1 complete: %s", pass1_cat.name)

    logger.info("[INFO] PSFEx: building PSF model ...")
    run_cmd(['psfex', 'pass1.ldac', '-c', 'psfex.conf'], cwd=str(rdir))
    psf_model = _discover_psf_file(rdir)
    logger.info("[INFO] PSFEx complete: %s", psf_model.name)

    logger.info("[INFO] PASS 2: SExtractor with PSF model ...")
    run_cmd([sex_name, fits_basename, '-c', 'sex_pass2.sex'], cwd=str(rdir))
    pass2_cat = rdir / 'pass2.ldac'
    _assert_exists(pass2_cat, "SExtractor PASS 2")
    logger.info("[INFO] PASS 2 complete: %s", pass2_cat.name)

    return str(pass1_cat), str(psf_model), str(pass2_cat)

# -------------------- NEW: STILTS wiring for cross-matching --------------------

from .utils.stilts_wrapper import stilts_xmatch  # Janne has STILTS in PATH

# Export SExtractor LDAC (FITS binary table) to CSV via STILTS tcopy (preferred) or Astropy fallback.

def export_ldac_to_csv(ldac_path: Path | str, out_csv: Path | str, *, columns: Optional[str] = None) -> Path:
    ldac_path = str(ldac_path)
    out_csv = Path(out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    try:
        _ensure_tool('stilts')
        cmd = ['stilts', 'tcopy', f'in={ldac_path}', f'out={str(out_csv)}', 'ofmt=csv']
        if columns:
            # keep only selected columns (expression filter)
            # Example: columns="ALPHA_J2000,DELTA_J2000,FLUX_AUTO"
            cmd += [f'ocmd=keepcols "{columns}"']
        run_cmd(cmd)
        return out_csv
    except Exception as e:
        logger.warning("[WARN] STILTS tcopy failed (%s); falling back to Astropy for LDAC→CSV", e)

    # Fallback: Astropy
    try:
        from astropy.io import fits
        import csv
        with fits.open(ldac_path, memmap=False) as hdul:
            # Find first BINTABLE HDU with data
            hdu = next((h for h in hdul if h.is_image is False and len(getattr(h, 'columns', []))>0), None)
            if hdu is None:
                raise RuntimeError("No table HDU found in LDAC")
            names = [c.name for c in hdu.columns]
            rows = hdu.data
        with open(out_csv, 'w', newline='') as f:
            w = csv.writer(f)
            w.writerow(names)
            for r in rows:
                w.writerow([r[n] for n in names])
        return out_csv
    except Exception as e:
        raise RuntimeError(f"Failed to export LDAC to CSV: {e}")


def run_crossmatch_with_stilts(
    run_dir: str | Path,
    pass2_ldac: str | Path,
    *,
    gaia_table: Optional[str | Path] = None,
    ps1_table: Optional[str | Path] = None,
    sex_ra_col: str = 'ALPHA_J2000',
    sex_dec_col: str = 'DELTA_J2000',
    ext_ra_col: str = 'ra',
    ext_dec_col: str = 'dec',
    radius_arcsec: float = 5.0,
) -> Tuple[Optional[str], Optional[str], str]:
    """
    Convert SExtractor PASS2 LDAC to CSV and cross-match with external catalogs via STILTS.

    Returns (xmatch_gaia, xmatch_ps1, sextractor_csv). xmatch_* may be None if table not supplied.
    """
    rdir = Path(run_dir)
    xdir = rdir / 'xmatch'
    xdir.mkdir(parents=True, exist_ok=True)

    sex_csv = rdir / 'catalogs' / 'sextractor_pass2.csv'
    sex_csv.parent.mkdir(parents=True, exist_ok=True)

    # Export LDAC to CSV for portability and easy inspection
    export_ldac_to_csv(pass2_ldac, sex_csv)

    x_gaia = x_ps1 = None

    if gaia_table is not None:
        x_gaia_path = xdir / 'sex_gaia_xmatch.csv'
        stilts_xmatch(
            str(sex_csv), str(gaia_table), str(x_gaia_path),
            ra1=sex_ra_col, dec1=sex_dec_col,
            ra2=ext_ra_col,  dec2=ext_dec_col,
            radius_arcsec=radius_arcsec,
            join_type='1and2',
            ofmt='csv',
        )
        x_gaia = str(x_gaia_path)

    if ps1_table is not None:
        x_ps1_path = xdir / 'sex_ps1_xmatch.csv'
        stilts_xmatch(
            str(sex_csv), str(ps1_table), str(x_ps1_path),
            ra1=sex_ra_col, dec1=sex_dec_col,
            ra2=ext_ra_col,  dec2=ext_dec_col,
            radius_arcsec=radius_arcsec,
            join_type='1and2',
            ofmt='csv',
        )
        x_ps1 = str(x_ps1_path)

    return x_gaia, x_ps1, str(sex_csv)

# Convenience: end-to-end run that performs the two-pass extraction and cross-matching.

def run_psf_two_pass_and_xmatch(
    fits_path: str | Path,
    run_dir: str | Path,
    *,
    config_root: str | Path = "configs",
    sex_bin: Optional[str] = None,
    gaia_table: Optional[str | Path] = None,
    ps1_table: Optional[str | Path] = None,
    sex_ra_col: str = 'ALPHA_J2000',
    sex_dec_col: str = 'DELTA_J2000',
    ext_ra_col: str = 'ra',
    ext_dec_col: str = 'dec',
    radius_arcsec: float = 5.0,
) -> Tuple[str, str, str, Optional[str], Optional[str], str]:
    pass1, psf, pass2 = run_psf_two_pass(fits_path, run_dir, config_root=config_root, sex_bin=sex_bin)
    x_gaia, x_ps1, sex_csv = run_crossmatch_with_stilts(
        run_dir, pass2,
        gaia_table=gaia_table,
        ps1_table=ps1_table,
        sex_ra_col=sex_ra_col,
        sex_dec_col=sex_dec_col,
        ext_ra_col=ext_ra_col,
        ext_dec_col=ext_dec_col,
        radius_arcsec=radius_arcsec,
    )
    return pass1, psf, pass2, x_gaia, x_ps1, sex_csv


# ------------------------------------------------------------
# CDS X-Match (cdsskymatch) with hardwired VizieR table IDs
# + write validation files filtered within 5 arcsec
# ------------------------------------------------------------

# Hardwired VizieR table IDs (change in code if needed later)
GAIA_VIZIER_TABLE_ID = 'I/350/gaiaedr3'   # Gaia EDR3 (example ID; confirm exact)
PS1_VIZIER_TABLE_ID  = 'II/389/ps1_dr2'   # Pan-STARRS DR2 (confirmed)

def _run_cdsskymatch(in_table_csv: Path, out_csv: Path, *, ra_col: str, dec_col: str,
                      cdstable: str, radius_arcsec: float = 5.0) -> None:
    """Run STILTS cdsskymatch against a VizieR/SIMBAD table and write CSV.
    Uses omode=out and ofmt=csv. No 'join' parameter (not supported for cdsskymatch).
    """
    _ensure_tool('stilts')
    cmd = [
        'stilts', 'cdsskymatch',
        f'in={str(in_table_csv)}',
        f'ra={ra_col}', f'dec={dec_col}',
        f'cdstable={cdstable}',
        f'radius={radius_arcsec}',
        'find=best',
        'omode=out',
        f'out={str(out_csv)}',
        'ofmt=csv',
    ]
    run_cmd(cmd)


def _validate_within_5_arcsec(xmatch_csv: Path) -> Path:
    """Create a validated CSV keeping only rows within <= 5 arcsec.
    Assumes xmatch_csv contains 'angDist' (distance) column from CDS X‑Match.
    We add 'angDist_arcsec = angDist*3600' (if angDist is degrees) and select <= 5.
    Returns path to the new CSV.
    """
    _ensure_tool('stilts')
    out = xmatch_csv.with_name(xmatch_csv.stem + '_within5arcsec.csv')
    cmd = [
        'stilts', 'tpipe',
        f'in={str(xmatch_csv)}',
        "cmd=addcol angDist_arcsec angDist*3600; select angDist_arcsec<=5",
        f'out={str(out)}', 'ofmt=csv'
    ]
    run_cmd(cmd)
    return out


def run_cds_xmatch(run_dir: str | Path, pass2_ldac: str | Path, *,
                    radius_arcsec: float = 5.0,
                    sex_ra_col: str = 'ALPHA_J2000',
                    sex_dec_col: str = 'DELTA_J2000') -> tuple[str | None, str | None, str]:
    """Convert PASS2 LDAC to CSV and perform CDS X‑Match against Gaia and PS1 (hardwired IDs).
    Also writes '*_within5arcsec.csv' validation files.
    Returns (gaia_out, ps1_out, sextractor_csv) where gaia_out/ps1_out may be None.
    """
    rdir = Path(run_dir)
    xdir = rdir / 'xmatch'
    xdir.mkdir(parents=True, exist_ok=True)
    sex_csv = rdir / 'catalogs' / 'sextractor_pass2.csv'
    sex_csv.parent.mkdir(parents=True, exist_ok=True)

    # Export LDAC -> CSV (SExtractor detections)
    export_ldac_to_csv(pass2_ldac, sex_csv)

    gaia_out = None
    ps1_out  = None

    # Gaia via CDS X‑Match
    if GAIA_VIZIER_TABLE_ID:
        gaia_path = xdir / 'sex_gaia_xmatch_cdss.csv'
        try:
            _run_cdsskymatch(sex_csv, gaia_path, ra_col=sex_ra_col, dec_col=sex_dec_col,
                              cdstable=GAIA_VIZIER_TABLE_ID, radius_arcsec=radius_arcsec)
            logger.info('[POST][CDS] Gaia xmatch -> %s', gaia_path)
            _validate_within_5_arcsec(gaia_path)
            gaia_out = str(gaia_path)
        except Exception as e:
            logger.warning('[POST][WARN] CDS Gaia xmatch failed: %s', e)

    # PS1 via CDS X‑Match
    if PS1_VIZIER_TABLE_ID:
        ps1_path = xdir / 'sex_ps1_xmatch_cdss.csv'
        try:
            _run_cdsskymatch(sex_csv, ps1_path, ra_col=sex_ra_col, dec_col=sex_dec_col,
                              cdstable=PS1_VIZIER_TABLE_ID, radius_arcsec=radius_arcsec)
            logger.info('[POST][CDS] PS1 xmatch -> %s', ps1_path)
            _validate_within_5_arcsec(ps1_path)
            ps1_out = str(ps1_path)
        except Exception as e:
            logger.warning('[POST][WARN] CDS PS1 xmatch failed: %s', e)

    return gaia_out, ps1_out, str(sex_csv)


def run_psf_two_pass_and_cds_xmatch(
    fits_path: str | Path,
    run_dir: str | Path,
    *,
    config_root: str | Path = 'configs',
    sex_bin: Optional[str] = None,
    sex_ra_col: str = 'ALPHA_J2000',
    sex_dec_col: str = 'DELTA_J2000',
    radius_arcsec: float = 5.0,
) -> tuple[str, str, str, Optional[str], Optional[str], str]:
    """Convenience wrapper: run two‑pass SExtractor+PSFEx, then CDS X‑Match and validation."""
    pass1, psf, pass2 = run_psf_two_pass(fits_path, run_dir, config_root=config_root, sex_bin=sex_bin)
    gaia_out, ps1_out, sex_csv = run_cds_xmatch(run_dir, pass2, radius_arcsec=radius_arcsec,
                                                sex_ra_col=sex_ra_col, sex_dec_col=sex_dec_col)
    return pass1, psf, pass2, gaia_out, ps1_out, sex_csv
