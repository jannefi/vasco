# pipeline.py — restored config staging + unit-tolerant within-5" validator
from __future__ import annotations
import logging, shutil
from pathlib import Path
from typing import Tuple, Optional
from .utils.subprocess import run_cmd

logger = logging.getLogger("vasco")


class ToolMissingError(RuntimeError):
    pass


_REQUIRED_CONFIGS = [
    "sex_pass1.sex",
    "sex_pass2.sex",
    "default.param",
    "default.conv",
    "default.nnw",
    "psfex.conf",
]


def _prepare_run_configs(config_root: str | Path, run_dir: str | Path) -> None:
    """Copy required SExtractor/PSFEx configs from config_root into run_dir."""
    cfg_root = Path(config_root).resolve()
    rdir = Path(run_dir).resolve()
    rdir.mkdir(parents=True, exist_ok=True)
    for name in _REQUIRED_CONFIGS:
        src = cfg_root / name
        dst = rdir / name
        if not src.exists():
            # default.nnw can be optional depending on your .sex includes
            if name == "default.nnw":
                continue
            raise FileNotFoundError(f"Missing config file: {src}")
        shutil.copy2(src, dst)
        logger.info("[INFO] Staged config: %s", dst.name)


def _ensure_fits_in_run_dir(fits_path: str | Path, run_dir: str | Path) -> str:
    """Copy FITS into run_dir if not present; return basename used for sex/psfex."""
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


# ---- Two-pass PSF-aware extraction (restored) ----


def run_psf_two_pass(
    fits_path: str | Path,
    run_dir: str | Path,
    config_root: str | Path = "configs",
    sex_bin: str | None = None,
) -> Tuple[str, str, str]:
    rdir = Path(run_dir).resolve()
    rdir.mkdir(parents=True, exist_ok=True)
    import shutil as _sh

    sex_name = sex_bin or _sh.which("sex") or _sh.which("sextractor")
    if not sex_name:
        raise ToolMissingError("SExtractor not found on PATH (sex/sextractor)")
    _ensure_tool("psfex")

    logger.info("[INFO] Preparing configs in run directory ...")
    _prepare_run_configs(config_root, rdir)

    fits_basename = _ensure_fits_in_run_dir(fits_path, rdir)

    logger.info("[INFO] PASS 1: SExtractor starting ...")
    run_cmd([sex_name, fits_basename, "-c", "sex_pass1.sex"], cwd=str(rdir))
    pass1_cat = rdir / "pass1.ldac"
    _assert_exists(pass1_cat, "SExtractor PASS 1")
    logger.info("[INFO] PASS 1 complete: %s", pass1_cat.name)

    logger.info("[INFO] PSFEx: building PSF model ...")
    run_cmd(["psfex", "pass1.ldac", "-c", "psfex.conf"], cwd=str(rdir))
    psf_model = _discover_psf_file(rdir)
    logger.info("[INFO] PSFEx complete: %s", psf_model.name)

    logger.info("[INFO] PASS 2: SExtractor with PSF model ...")
    run_cmd([sex_name, fits_basename, "-c", "sex_pass2.sex"], cwd=str(rdir))
    pass2_cat = rdir / "pass2.ldac"
    _assert_exists(pass2_cat, "SExtractor PASS 2")
    logger.info("[INFO] PASS 2 complete: %s", pass2_cat.name)

    return str(pass1_cat), str(psf_model), str(pass2_cat)


# ---- Unit-tolerant within-5" validator for CDS outputs ----


def _validate_within_5_arcsec(xmatch_csv: Path) -> Path:
    """Create <stem>_within5arcsec.csv keeping only rows within 5 arcsec.
    - If 'angDist' exists: try ARCSECONDS (angDist<=5). If zero, fallback to DEGREES (3600*angDist<=5).
    - Else: compute via skyDistanceDegrees(ALPHA_J2000,DELTA_J2000,<ext_ra>,<ext_dec>) and select <=5
    """
    _ensure_tool("stilts")
    import csv, subprocess

    xmatch_csv = Path(xmatch_csv)
    out = xmatch_csv.with_name(xmatch_csv.stem + "_within5arcsec.csv")
    with open(xmatch_csv, newline="") as f:
        header = next(csv.reader(f), [])
    cols = set(header)

    def _write_empty():
        subprocess.run(
            [
                "stilts",
                "tpipe",
                f"in={str(xmatch_csv)}",
                "cmd=select false",
                f"out={str(out)}",
                "ofmt=csv",
            ],
            check=True,
        )
        return out

    if "angDist" in cols:
        p = subprocess.run(
            [
                "stilts",
                "tpipe",
                f"in={str(xmatch_csv)}",
                "cmd=select angDist<=5",
                "omode=count",
            ],
            capture_output=True,
            text=True,
        )
        try:
            # handle either "rows: N" or just "N"
            c = int((p.stdout or "0").strip().split()[-1])
        except Exception:
            c = 0
        if c > 0:
            subprocess.run(
                [
                    "stilts",
                    "tpipe",
                    f"in={str(xmatch_csv)}",
                    "cmd=select angDist<=5",
                    f"out={str(out)}",
                    "ofmt=csv",
                ],
                check=True,
            )
            return out
        subprocess.run(
            [
                "stilts",
                "tpipe",
                f"in={str(xmatch_csv)}",
                "cmd=select 3600*angDist<=5",
                f"out={str(out)}",
                "ofmt=csv",
            ],
            check=True,
        )
        return out
    for a, b in [
        ("ra", "dec"),
        ("RAJ2000", "DEJ2000"),
        ("RA_ICRS", "DE_ICRS"),
        ("RA", "DEC"),
    ]:
        if a in cols and b in cols:
            cmd = (
                "cmd=addcol angDist_arcsec "
                f"3600*skyDistanceDegrees(ALPHA_J2000,DELTA_J2000,{a},{b}); "
                "select angDist_arcsec<=5"
            )
            subprocess.run(
                [
                    "stilts",
                    "tpipe",
                    f"in={str(xmatch_csv)}",
                    cmd,
                    f"out={str(out)}",
                    "ofmt=csv",
                ],
                check=True,
            )
            return out
    return _write_empty()
