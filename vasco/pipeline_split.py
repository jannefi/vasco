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
    missing = []
    for name in names:
        src = next(((d / name) for d in candidates_dirs if (d / name).exists()), None)
        if src is None:
            missing.append(name)
        else:
            dst = tile_dir / name
            if src.resolve() != dst.resolve():
                shutil.copy2(src, dst)
    if missing:
        raise FileNotFoundError(
            "Missing config files: " + ", ".join(missing) + " in any of: " + 
            ", ".join(str(d) for d in candidates_dirs)
        )


def _make_img_rel_to_run(fits_path: Path, tile_dir: Path) -> Path:
    """Return the image path relative to <tile_root> CWD for SExtractor/PSFEx runs."""
    fits_path = Path(fits_path)
    tile_dir = Path(tile_dir)
    if fits_path.is_absolute():
        try:
            return fits_path.relative_to(tile_dir)
        except Exception:
            raw_candidate = tile_dir / 'raw' / fits_path.name
            return Path('raw') / fits_path.name if raw_candidate.exists() else fits_path
    else:
        parts = fits_path.parts
        if 'raw' in parts:
            idx = parts.index('raw')
            return Path(*parts[idx:])
        return Path('raw') / fits_path.name

# --- NEW: tiny debug logger ---

def _dbg_write(tile_dir: Path, text: str) -> None:
    try:
        (Path(tile_dir) / 'debug_pass1.log').write_text(text, encoding='utf-8')
    except Exception:
        pass


def run_pass1(fits_path: str | Path, tile_dir: Path, *, config_root: str = 'configs') -> Tuple[Path, Path]:
    tile_dir = Path(tile_dir)
    fits_path = Path(fits_path)
    sex_bin = _find_binary(['sex', 'sextractor'])
    if sex_bin is None:
        _ensure_tool('sex')

    # Stage pass-1 config + NON-PSF parameters into the tile run folder
    _stage_to_run_folder(
        tile_dir, Path(config_root),
        ['sex_pass1.sex', 'sex_default.param', 'default.nnw', 'default.conv']
    )

    conf = Path('sex_pass1.sex')
    img_rel = _make_img_rel_to_run(fits_path, tile_dir)
    pass1_ldac = Path('pass1.ldac')
    log = tile_dir / 'sex.out'
    err = tile_dir / 'sex.err'

    # Explicitly disable PSF at CLI level (even if a config accidentally sets it)
    cmd = [sex_bin or 'sex', str(img_rel), '-c', str(conf),
           '-CATALOG_NAME', str(pass1_ldac), '-CATALOG_TYPE', 'FITS_LDAC',
           '-PSF_NAME', '']

    # (optional) debug preflight
    try:
        listing = ''.join(sorted(p.name for p in tile_dir.iterdir()))
        dbg = (
            f"[DBG] tile_dir={tile_dir}"
            f"[DBG] cwd for subprocess={tile_dir}"
            f"[DBG] cmd={' '.join(cmd)}"
            f"[DBG] exists(conf? { (tile_dir/conf).exists() }) path={tile_dir/conf}"
            f"[DBG] will write catalog to {tile_dir/pass1_ldac}"
            f"[DBG] tile listing (top-level):{listing}"
        )
        _dbg_write(tile_dir, dbg)
    except Exception:
        pass

    with open(log, 'w') as l, open(err, 'w') as e:
        rc = subprocess.run(cmd, stdout=l, stderr=e, cwd=str(tile_dir)).returncode
    if rc != 0:
        try:
            tail = ''.join(Path(err).read_text(encoding='utf-8', errors='ignore').splitlines()[-25:])
            raise RuntimeError(f'SExtractor pass1 failed (rc={rc}). See sex.err tail:{tail}')
        except Exception:
            raise RuntimeError(f'SExtractor pass1 failed: rc={rc}')

    proto = tile_dir / 'proto_pass1.fits'
    return tile_dir / pass1_ldac, proto


def run_psfex(pass1_ldac: str | Path, tile_dir: Path, *, config_root: str = 'configs') -> Path:
    tile_dir = Path(tile_dir)

    _ensure_tool('psfex')
    _stage_to_run_folder(tile_dir, Path(config_root), ['psfex.conf'])

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

    cmd = [sex_bin or 'sex', str(img_rel), '-c', str(conf),
           '-CATALOG_NAME', str(pass2_ldac), '-CATALOG_TYPE', 'FITS_LDAC',
           '-PSF_NAME', psf_path.name]

    with open(log, 'a') as l, open(err, 'a') as e:
        rc = subprocess.run(cmd, stdout=l, stderr=e, cwd=str(tile_dir)).returncode
    if rc != 0:
        raise RuntimeError(f'SExtractor pass2 failed: rc={rc}')

    return tile_dir / pass2_ldac
