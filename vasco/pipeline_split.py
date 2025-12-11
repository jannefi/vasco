from __future__ import annotations
import os, shutil, subprocess
from pathlib import Path
from typing import Tuple, List

class ToolMissingError(Exception):
    pass

def _ensure_tool(name: str) -> None:
    if shutil.which(name) is None:
        raise ToolMissingError(f"Required tool '{name}' not found in PATH.")


def _find_binary(candidates):
    for c in candidates:
        if shutil.which(c):
            return c
    return None


def _fitspath(tile_dir: Path) -> Path:
    raw = tile_dir / 'raw'
    if not raw.exists():
        raise FileNotFoundError(f"raw/ not found under {tile_dir}")
    fits = next((p for p in sorted(raw.glob('*.fits'))), None)
    if fits is None:
        raise FileNotFoundError(f"No FITS under {raw}")
    return fits


def _stage_to_run_folder(tile_dir: Path, config_root: Path, names: List[str]) -> None:
    """
    Copy required config files into <tile_root> (run folder) with expected bare names.
    Search order per name:
      <tile_root>/<name>
      <tile_root>/configs/<name>
      <config_root>/<name>
      <repo_root>/configs/<name>
    """
    tile_dir = Path(tile_dir)
    config_root = Path(config_root)
    repo_root = Path(__file__).resolve().parents[1]

    candidates_dirs = [
        tile_dir,
        tile_dir / 'configs',
        config_root,
        repo_root / 'configs',
    ]
    for name in names:
        src = next(((d / name) for d in candidates_dirs if (d / name).exists()), None)
        if src is None:
            raise FileNotFoundError(
                "Missing config file '" + name + "' in any of: "
                + ", ".join(str(d) for d in candidates_dirs)
            )
        dst = tile_dir / name
        if src.resolve() != dst.resolve():
            shutil.copy2(src, dst)


def _make_img_rel_to_run(fits_path: Path, tile_dir: Path) -> Path:
    """Return the image path relative to <tile_root> CWD for SExtractor/PSFEx runs."""
    fits_path = Path(fits_path)
    tile_dir = Path(tile_dir)
    if fits_path.is_absolute():
        try:
            return fits_path.relative_to(tile_dir)
        except Exception:
            # Fallback: if it's under raw/, use raw/<filename>; else use the absolute path
            raw_candidate = tile_dir / 'raw' / fits_path.name
            return Path('raw') / fits_path.name if raw_candidate.exists() else fits_path
    else:
        # If it's a repo-relative path like data/tiles/.../raw/..., reduce to raw/...
        parts = fits_path.parts
        if 'raw' in parts:
            idx = parts.index('raw')
            return Path(*parts[idx:])
        return Path('raw') / fits_path.name


def run_pass1(fits_path: str | Path, tile_dir: Path, *, config_root: str = 'configs') -> Tuple[Path, Path]:
    tile_dir = Path(tile_dir)
    fits_path = Path(fits_path)

    sex_bin = _find_binary(['sex', 'sextractor'])
    if sex_bin is None:
        _ensure_tool('sex')  # try the most common name; error if missing

    # Stage configs into the run folder so bare names inside .sex resolve
    _stage_to_run_folder(tile_dir, Path(config_root), ['sex_pass1.sex', 'default.param', 'default.nnw', 'default.conv'])

    # Use names relative to cwd=tile_dir
    conf = Path('sex_pass1.sex')
    img_rel = _make_img_rel_to_run(fits_path, tile_dir)

    # Outputs & logs (catalogs relative to cwd; logs are opened by Python)
    pass1_ldac = Path('pass1.ldac')
    log = tile_dir / 'sex.out'
    err = tile_dir / 'sex.err'

    cmd = [sex_bin or 'sex', str(img_rel), '-c', str(conf),
           '-CATALOG_NAME', str(pass1_ldac), '-CATALOG_TYPE', 'FITS_LDAC']

    with open(log, 'w') as l, open(err, 'w') as e:
        rc = subprocess.run(cmd, stdout=l, stderr=e, cwd=str(tile_dir)).returncode
    if rc != 0:
        try:
            tail = ''.join(Path(err).read_text(encoding='utf-8', errors='ignore').splitlines()[-25:])
            raise RuntimeError(f'SExtractor pass1 failed (rc={rc}). See sex.err tail:{tail}')
        except Exception:
            raise RuntimeError(f'SExtractor pass1 failed: rc={rc}')

    # Keeping proto path for API parity, though pass1 config may not emit these
    proto = tile_dir / 'proto_pass1.fits'
    return tile_dir / pass1_ldac, proto


def run_psfex(pass1_ldac: str | Path, tile_dir: Path, *, config_root: str = 'configs') -> Path:
    tile_dir = Path(tile_dir)

    _ensure_tool('psfex')
    _stage_to_run_folder(tile_dir, Path(config_root), ['psfex.conf'])

    # Always refer to files relative to cwd
    ldac_rel = Path('pass1.ldac')
    conf = Path('psfex.conf')
    psf = Path('pass1.psf')

    out = tile_dir / 'psfex.out'
    err = tile_dir / 'psfex.err'

    cmd = ['psfex', str(ldac_rel), '-c', str(conf), '-OUTFILE_NAME', str(psf)]

    with open(out, 'w') as o, open(err, 'w') as e:
        rc = subprocess.run(cmd, stdout=o, stderr=e, cwd=str(tile_dir)).returncode
    if rc != 0:
        raise RuntimeError(f'PSFEx failed: rc={rc}')

    return tile_dir / psf


def run_pass2(fits_path: str | Path, tile_dir: Path, psf_path: str | Path, *, config_root: str = 'configs') -> Path:
    tile_dir = Path(tile_dir)
    fits_path = Path(fits_path)
    psf_path = Path(psf_path)

    sex_bin = _find_binary(['sex', 'sextractor'])
    if sex_bin is None:
        _ensure_tool('sex')

    _stage_to_run_folder(tile_dir, Path(config_root), ['sex_pass2.sex', 'default.param', 'default.nnw', 'default.conv'])

    conf = Path('sex_pass2.sex')
    img_rel = _make_img_rel_to_run(fits_path, tile_dir)
    pass2_ldac = Path('pass2.ldac')

    log = tile_dir / 'sex.out'
    err = tile_dir / 'sex.err'

    # PSF path should be referenced relative to cwd; use just the name
    cmd = [sex_bin or 'sex', str(img_rel), '-c', str(conf),
           '-CATALOG_NAME', str(pass2_ldac), '-CATALOG_TYPE', 'FITS_LDAC',
           '-PSF_NAME', psf_path.name]

    with open(log, 'a') as l, open(err, 'a') as e:
        rc = subprocess.run(cmd, stdout=l, stderr=e, cwd=str(tile_dir)).returncode
    if rc != 0:
        raise RuntimeError(f'SExtractor pass2 failed: rc={rc}')

    return tile_dir / pass2_ldac
