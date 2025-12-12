
# vasco/utils/cdsskymatch.py
from __future__ import annotations

import csv
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional, Sequence, Tuple, List

__all__ = ["cdsskymatch", "CdsXmatchError"]

# -------------------- Exceptions --------------------
class CdsXmatchError(RuntimeError):
    """Raised for CDS XMatch transient errors that warrant retry/backoff."""

# -------------------- STILTS CLI --------------------
_STILTS = shutil.which("stilts") or "stilts"

# -------------------- Environment knobs --------------------
# Modes:
#  - single  : one cdsskymatch call + bounded retries (fast when service OK)
#  - chunked : manual chunking + bounded retries + safe CSV concat
_MODE = os.getenv("VASCO_CDS_MODE", "single").strip().lower()  # 'single' | 'chunked'

# Chunking (only used if _MODE == 'chunked')
_CHUNK_ROWS = int(os.getenv("VASCO_CDS_CHUNK_ROWS", "500"))

# blocksize handling:
#  - integer value -> pass blocksize=<int> to STILTS
#  - "omit" or unset -> do not pass blocksize at all
_BLOCKSIZE_ENV = os.getenv("VASCO_CDS_BLOCKSIZE", "").strip().lower()
_BLOCKSIZE: Optional[int] = None if _BLOCKSIZE_ENV in ("", "omit", "none") else int(_BLOCKSIZE_ENV)

# Retry/backoff
_MAX_RETRIES = int(os.getenv("VASCO_CDS_MAX_RETRIES", "2"))
_BASE_BACKOFF = float(os.getenv("VASCO_CDS_BASE_BACKOFF", "1.5"))  # seconds
_INTER_DELAY = float(os.getenv("VASCO_CDS_INTER_CHUNK_DELAY", "0.2"))
_JITTER = float(os.getenv("VASCO_CDS_JITTER", "0.2"))

# -------------------- Helpers --------------------
def _sleep(base: float) -> None:
    time.sleep(max(0.0, base) + random.uniform(0, _JITTER))

def _run_stilts(cmd: Sequence[str], label: str) -> None:
    """
    Run STILTS; re-map common CDS transient errors to CdsXmatchError for retry.
    """
    try:
        subprocess.run(cmd, check=True, text=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        msg = (e.stderr or e.stdout or str(e)).strip()
        transient_signals = (
            "Too many jobs", "try later", "503", "Service Unavailable",
            "Connection reset", "Read timed out", "SAXParseException", "Service Error"
        )
        if any(s in msg for s in transient_signals):
            raise CdsXmatchError(msg)
        raise

def _retry_loop(callable_fn, *, label: str) -> None:
    attempt = 0
    while True:
        try:
            return callable_fn()
        except CdsXmatchError as e:
            if attempt >= _MAX_RETRIES:
                raise
            backoff = _BASE_BACKOFF * (2 ** attempt)
            print(f"[CDS][RETRY] {label}: {e} — retry {attempt+1}/{_MAX_RETRIES} in ~{backoff:.1f}s",
                  file=sys.stderr)
            _sleep(backoff)
            attempt += 1

# -------------------- Chunking utilities --------------------
def _split_csv(path: Path, chunk_rows: int, tempdir: Path) -> List[Path]:
    """
    Split a CSV into ~equal-sized parts of up to 'chunk_rows' each (header preserved).
    Returns list of part file paths.
    """
    outs: List[Path] = []
    with Path(path).open(newline='') as f:
        r = csv.reader(f)
        header = next(r, [])
        idx = 0
        buf: List[List[str]] = []
        for row in r:
            buf.append(row)
            if len(buf) >= chunk_rows:
                out = tempdir / f"chunk_{idx:05d}.csv"
                with out.open("w", newline='') as g:
                    w = csv.writer(g)
                    w.writerow(header)
                    w.writerows(buf)
                outs.append(out)
                buf.clear()
                idx += 1
        if buf:
            out = tempdir / f"chunk_{idx:05d}.csv"
            with out.open("w", newline='') as g:
                w = csv.writer(g)
                w.writerow(header)
                w.writerows(buf)
            outs.append(out)
    return outs

def _concat_csv(parts: Sequence[Path], out: Path) -> None:
    """
    Safe CSV concatenation: union of headers, fill missing columns with empty strings.
    Avoids STILTS 'tcat' metadata type conflicts (e.g., Double vs Float).
    """
    parts = [Path(p) for p in parts]
    out = Path(out)
    if not parts:
        out.write_text("")
        return

    headers: List[List[str]] = []
    for p in parts:
        with p.open(newline='') as f:
            r = csv.reader(f)
            hdr = next(r, [])
            headers.append([h.strip() for h in hdr])

    # Union header with stable order (first file dominates)
    union: List[str] = []
    for h in headers[0]:
        if h not in union:
            union.append(h)
    for hdr in headers[1:]:
        for h in hdr:
            if h not in union:
                union.append(h)

    with out.open("w", newline='') as g:
        w = csv.writer(g)
        w.writerow(union)
        for p, hdr in zip(parts, headers):
            col_idx = {h: i for i, h in enumerate(hdr)}
            with p.open(newline='') as f:
                r = csv.reader(f)
                next(r, None)  # skip part header
                for row in r:
                    outrow = []
                    for h in union:
                        v = row[col_idx[h]] if (h in col_idx and col_idx[h] < len(row)) else ""
                        outrow.append(v)
                    w.writerow(outrow)

# -------------------- Single-call cdsskymatch --------------------
def _cdsskymatch_single(
    in_csv: Path,
    out_csv: Path,
    *,
    ra: str,
    dec: str,
    cdstable: str,
    radius_arcsec: float,
    find: str,
    ofmt: str,
    omode: str,
    blocksize: Optional[int],
) -> None:
    cmd = [
        _STILTS, "cdsskymatch",
        f"in={str(in_csv)}", f"ra={ra}", f"dec={dec}",
        f"cdstable={cdstable}",
        f"radius={radius_arcsec}",
        f"find={find}", f"omode={omode}",
        f"out={str(out_csv)}", f"ofmt={ofmt}",
    ]
    if blocksize is not None:
        cmd.append(f"blocksize={int(blocksize)}")
    _run_stilts(cmd, label=f"cdsskymatch:{cdstable}")

# -------------------- Public API --------------------
def cdsskymatch(
    in_table: str | Path,
    out_table: str | Path,
    *,
    ra: str,
    dec: str,
    cdstable: str,
    radius_arcsec: float,
    find: str = "best",
    ofmt: str = "csv",
    omode: str = "out",
    blocksize: Optional[int] = None,
) -> None:
    """
    CDS XMatch wrapper with two modes:
      - 'single'  : one cdsskymatch call + bounded retries (fast when CDS OK)
      - 'chunked' : manual chunking + retries + safe CSV concatenation
    Input must be CSV with RA/Dec columns in **degrees**.
    """
    in_table = Path(in_table)
    out_table = Path(out_table)
    out_table.parent.mkdir(parents=True, exist_ok=True)

    # Determine blocksize behavior
    blk = blocksize if blocksize is not None else _BLOCKSIZE

    if _MODE == "single":
        def _do_single():
            _cdsskymatch_single(
                in_csv=in_table, out_csv=out_table,
                ra=ra, dec=dec, cdstable=cdstable,
                radius_arcsec=radius_arcsec, find=find,
                ofmt=ofmt, omode=omode, blocksize=blk
            )
        _retry_loop(_do_single, label=f"cdsskymatch:{cdstable}:single")
        return

    # --- chunked mode ---
    with tempfile.TemporaryDirectory(prefix="cdschunks_") as tdir:
        tdir_path = Path(tdir)
        chunks = _split_csv(in_table, max(50, _CHUNK_ROWS), tdir_path)
        if not chunks:
            out_table.write_text("")
            return

        outputs: List[Path] = []
        total = len(chunks)
        for idx, ch in enumerate(chunks, start=1):
            ch_out = tdir_path / f"{ch.stem}_out.csv"

            def _do_chunk():
                _cdsskymatch_single(
                    in_csv=ch, out_csv=ch_out,
                    ra=ra, dec=dec, cdstable=cdstable,
                    radius_arcsec=radius_arcsec, find=find,
                    ofmt=ofmt, omode=omode, blocksize=blk
                )

            _retry_loop(_do_chunk, label=f"cdsskymatch:{cdstable}:chunk{idx}/{total}")
            outputs.append(ch_out)
            _sleep(_INTER_DELAY)

        _concat_csv(outputs, out_table)
