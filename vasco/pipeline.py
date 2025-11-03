from __future__ import annotations
import logging, shutil
from pathlib import Path
from typing import Tuple
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
