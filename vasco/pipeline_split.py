from __future__ import annotations
import os, shutil, subprocess
from pathlib import Path
from typing import Tuple

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

def run_pass1(fits_path: str | Path, tile_dir: Path, *, config_root: str = 'configs') -> Tuple[Path, Path]:
    # Run SExtractor first pass only; returns (pass1_ldac, proto_pass1_fits)
    tile_dir = Path(tile_dir)
    fits_path = Path(fits_path)
    sex_bin = _find_binary(['sex','sextractor'])
    if sex_bin is None:
        _ensure_tool('sex')
    conf = tile_dir / 'sex_pass1.sex'
    if not conf.exists():
        conf = Path(config_root) / 'sex_pass1.sex'
    if not conf.exists():
        raise FileNotFoundError('sex_pass1.sex not found')
    pass1_ldac = tile_dir / 'pass1.ldac'
    proto = tile_dir / 'proto_pass1.fits'
    chi = tile_dir / 'chi_pass1.fits'
    resi = tile_dir / 'resi_pass1.fits'
    samp = tile_dir / 'samp_pass1.fits'
    log = tile_dir / 'sex.out'
    err = tile_dir / 'sex.err'
    cmd = [sex_bin or 'sex', str(fits_path), '-c', str(conf), '-CATALOG_NAME', str(pass1_ldac), '-CATALOG_TYPE', 'FITS_LDAC', '-CHECKIMAGE_NAME', f'{proto},{chi},{resi},{samp}']
    with open(log,'w') as l, open(err,'w') as e:
        rc = subprocess.run(cmd, stdout=l, stderr=e).returncode
        if rc != 0:
            raise RuntimeError(f'SExtractor pass1 failed: rc={rc}')
    return pass1_ldac, proto

def run_psfex(pass1_ldac: str | Path, tile_dir: Path, *, config_root: str = 'configs') -> Path:
    tile_dir = Path(tile_dir)
    pass1_ldac = Path(pass1_ldac)
    _ensure_tool('psfex')
    conf = tile_dir / 'psfex.conf'
    if not conf.exists():
        conf = Path(config_root) / 'psfex.conf'
    if not conf.exists():
        raise FileNotFoundError('psfex.conf not found')
    psf = tile_dir / 'pass1.psf'
    out = tile_dir / 'psfex.out'
    err = tile_dir / 'psfex.err'
    cmd = ['psfex', str(pass1_ldac), '-c', str(conf), '-OUTFILE_NAME', str(psf)]
    with open(out,'w') as o, open(err,'w') as e:
        rc = subprocess.run(cmd, stdout=o, stderr=e).returncode
        if rc != 0:
            raise RuntimeError(f'PSFEx failed: rc={rc}')
    return psf

def run_pass2(fits_path: str | Path, tile_dir: Path, psf_path: str | Path, *, config_root: str = 'configs') -> Path:
    tile_dir = Path(tile_dir)
    fits_path = Path(fits_path)
    psf_path = Path(psf_path)
    sex_bin = _find_binary(['sex','sextractor'])
    if sex_bin is None:
        _ensure_tool('sex')
    conf = tile_dir / 'sex_pass2.sex'
    if not conf.exists():
        conf = Path(config_root) / 'sex_pass2.sex'
    if not conf.exists():
        raise FileNotFoundError('sex_pass2.sex not found')
    pass2_ldac = tile_dir / 'pass2.ldac'
    log = tile_dir / 'sex.out'
    err = tile_dir / 'sex.err'
    cmd = [sex_bin or 'sex', str(fits_path), '-c', str(conf), '-CATALOG_NAME', str(pass2_ldac), '-CATALOG_TYPE', 'FITS_LDAC', '-PSF_NAME', str(psf_path)]
    with open(log,'a') as l, open(err,'a') as e:
        rc = subprocess.run(cmd, stdout=l, stderr=e).returncode
        if rc != 0:
            raise RuntimeError(f'SExtractor pass2 failed: rc={rc}')
    return pass2_ldac