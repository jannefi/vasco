
"""
STILTS wrapper for sky cross-matching.
- Prefers `tskymatch2`, falls back to `tmatch2` (matcher=sky).
- Preflight checks for input files; infers formats for CSV/FITS/VOTable.
"""
from __future__ import annotations
import os
import subprocess
from typing import Optional

class StiltsError(RuntimeError):
    pass

_DEF_RA = 'ra'
_DEF_DEC = 'dec'


def _exists(p: str) -> bool:
    try:
        return os.path.exists(p)
    except Exception:
        return False


def _infer_fmt(path: str) -> Optional[str]:
    e = os.path.splitext(path)[1].lower()
    if e in ('.csv', '.ecsv'): return 'csv'
    if e in ('.fit', '.fits'): return 'fits'
    if e in ('.vot', '.votable', '.xml'): return 'votable'
    return None


def _run(cmd: list[str]) -> None:
    try:
        subprocess.run(cmd, check=True, text=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        msg = e.stderr or e.stdout or str(e)
        raise StiltsError(msg)


def stilts_xmatch(
    table1: str,
    table2: str,
    out_table: str,
    *,
    ra1: str = _DEF_RA,
    dec1: str = _DEF_DEC,
    ra2: str = _DEF_RA,
    dec2: str = _DEF_DEC,
    radius_arcsec: float = 1.0,
    join_type: str = '1and2',
    find: Optional[str] = None,
    ofmt: Optional[str] = None,
) -> None:
    """Cross-match two catalogs by sky position using STILTS.

    RA/Dec columns must be in **degrees**.
    """
    if not _exists(table1):
        raise StiltsError(f'in1 does not exist: {table1}')
    if not _exists(table2):
        raise StiltsError(f'in2 does not exist: {table2}')

    if ofmt is None:
        ofmt = _infer_fmt(out_table)

    ifmt1 = _infer_fmt(table1)
    ifmt2 = _infer_fmt(table2)

    # Try tskymatch2 first
    cmd = ['stilts', 'tskymatch2']
    if ifmt1: cmd.append(f'ifmt1={ifmt1}')
    if ifmt2: cmd.append(f'ifmt2={ifmt2}')
    cmd += [
        f'in1={table1}', f'in2={table2}', f'out={out_table}',
        f'ra1={ra1}', f'dec1={dec1}', f'ra2={ra2}', f'dec2={dec2}',
        f'error={radius_arcsec}', f'join={join_type}',
    ]
    if ofmt: cmd.append(f'ofmt={ofmt}')
    if find: cmd.append(f'find={find}')

    try:
        _run(cmd)
        return
    except StiltsError as e:
        if 'No such task' not in str(e):
            raise

    # Fallback to tmatch2
    cmd = ['stilts', 'tmatch2']
    if ifmt1: cmd.append(f'ifmt1={ifmt1}')
    if ifmt2: cmd.append(f'ifmt2={ifmt2}')
    cmd += [
        f'in1={table1}', f'in2={table2}', f'out={out_table}',
        'matcher=sky', f'params={radius_arcsec}',
        f"values1={ra1} {dec1}", f"values2={ra2} {dec2}",
        f'join={join_type}',
    ]
    if ofmt: cmd.append(f'ofmt={ofmt}')
    if find: cmd.append(f'find={find}')

    _run(cmd)
