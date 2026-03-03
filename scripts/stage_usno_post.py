#!/usr/bin/env python3
"""
Post-pipeline USNO-B reduction stage (chunked input -> single stage output).

Goal
----
Given a run directory (e.g. ./work/runs/run-S1-20260302_185423) containing
chunked positional upload CSVs (e.g. upload_positional_chunk_*.csv or
upload_positional_S2_chunk_*.csv), crossmatch each chunk against USNO-B1.0
(VizieR table I/284/out) within a configurable radius (default 5 arcsec).

Outputs
-------
1) stage_<STAGE>_USNO.csv
   - The kept remainder AFTER USNO elimination (rows WITHOUT a USNO match).
   - Columns: src_id, ra, dec

2) stage_<STAGE>_USNO_flags.csv
   - Full audit table for ALL input rows.
   - Columns: src_id, ra, dec, has_usnob_match, best_sep_arcsec, source_chunk

3) stage_<STAGE>_USNO_ledger.json
   - Counts + per-chunk stats + parameters used.

Notes
-----
- Uses STILTS CDS XMatch (cdsskymatch) directly; does NOT depend on per-tile
  usnob_neighbourhood.csv caches.
- Designed to work with your src_id/ra/dec upload chunk convention (S1→S2→S3…).
  (See your “Shrinking Set” contract notes.) 
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

DEFAULT_CDSTABLE = "I/284/out"  # USNO-B1.0 table id for CDS XMatch / STILTS


def _ensure_tool(name: str) -> str:
    path = shutil.which(name)
    if path:
        return path
    raise RuntimeError(f"Required tool '{name}' not found in PATH")


def _read_header(path: Path) -> List[str]:
    with path.open(newline="", encoding="utf-8", errors="ignore") as f:
        r = csv.reader(f)
        return [c.strip().lstrip("\ufeff") for c in next(r, [])]


def _detect_cols(cols: List[str]) -> Tuple[str, str, str]:
    # Required: src_id, ra, dec. Preserve common variants.
    cset = {c.strip() for c in cols}

    # src_id
    if "src_id" in cset:
        src = "src_id"
    elif "row_id" in cset:
        src = "row_id"
    else:
        raise RuntimeError("Input CSV missing required id column 'src_id' (or 'row_id')")

    # ra/dec (degrees)
    ra_candidates = ["ra", "RA", "RA_corr", "ALPHAWIN_J2000", "ALPHA_J2000"]
    dec_candidates = ["dec", "DEC", "Dec", "Dec_corr", "DELTAWIN_J2000", "DELTA_J2000"]

    ra = next((c for c in ra_candidates if c in cset), None)
    dec = next((c for c in dec_candidates if c in cset), None)
    if not ra or not dec:
        raise RuntimeError(
            "Input CSV missing RA/Dec columns (expected one of ra/dec, RA/DEC, RA_corr/Dec_corr, etc.)"
        )
    return src, ra, dec


def _count_data_rows(path: Path) -> int:
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            n = sum(1 for _ in f)
        return max(0, n - 1)
    except Exception:
        return 0


def _try_import_vasco_cdsskymatch():
    """Try to use vasco.utils.cdsskymatch wrapper if available (adds retries/backoff)."""
    try:
        from vasco.utils.cdsskymatch import cdsskymatch  # type: ignore
        return cdsskymatch
    except Exception:
        return None


def _run_stilts_cdsskymatch(
    stilts_bin: str,
    in_csv: Path,
    out_csv: Path,
    *,
    ra_col: str,
    dec_col: str,
    cdstable: str,
    radius_arcsec: float,
    find: str = "best",
    blocksize: Optional[int] = None,
) -> None:
    cmd = [
        stilts_bin,
        "cdsskymatch",
        "ifmt=csv",
        f"in={str(in_csv)}",
        f"ra={ra_col}",
        f"dec={dec_col}",
        f"cdstable={cdstable}",
        f"radius={radius_arcsec}",
        f"find={find}",
        "ofmt=csv",
        f"out={str(out_csv)}",
    ]
    if blocksize is not None:
        cmd.append(f"blocksize={int(blocksize)}")

    subprocess.run(cmd, check=True, text=True, capture_output=True)


@dataclass
class ChunkStats:
    chunk: str
    input_rows: int
    matched_rows: int
    kept_rows: int


def _load_matches(match_csv: Path, src_col: str) -> Dict[str, float]:
    """Return mapping src_id -> best_sep_arcsec from STILTS cdsskymatch output."""
    if not match_csv.exists() or match_csv.stat().st_size == 0:
        return {}
    with match_csv.open(newline="", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f)
        if not r.fieldnames:
            return {}
        fields = set(r.fieldnames)
        dist_field = "angDist" if "angDist" in fields else None

        out: Dict[str, float] = {}
        for row in r:
            sid = row.get(src_col, "") or row.get("src_id", "") or row.get("row_id", "")
            if not sid:
                continue
            if dist_field:
                try:
                    out[sid] = float(row.get(dist_field, "nan"))
                except Exception:
                    out[sid] = float("nan")
            else:
                out[sid] = float("nan")
        return out


def _iter_input_rows(path: Path, src_col: str, ra_col: str, dec_col: str) -> Iterable[Tuple[str, str, str]]:
    with path.open(newline="", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f)
        for row in r:
            sid = (row.get(src_col) or "").strip()
            ra = (row.get(ra_col) or "").strip()
            dec = (row.get(dec_col) or "").strip()
            if not sid or not ra or not dec:
                continue
            yield sid, ra, dec


def main() -> int:
    ap = argparse.ArgumentParser(description="Post-pipeline USNO-B reduction stage (chunked inputs).")
    ap.add_argument("--run-dir", required=True, help="Run folder, e.g. ./work/runs/run-S1-20260302_185423")
    ap.add_argument(
        "--input-glob",
        default="upload_positional*_chunk_*.csv",
        help="Glob pattern (relative to run-dir) for input chunk CSVs. Default: upload_positional*_chunk_*.csv",
    )
    ap.add_argument("--stage", default="S0", help="Stage label used in output filenames. Default: S0")
    ap.add_argument("--out-dir", default=None, help="Output directory. Default: <run-dir>/stages")
    ap.add_argument("--cdstable", default=DEFAULT_CDSTABLE, help=f"CDS table id. Default: {DEFAULT_CDSTABLE}")
    ap.add_argument("--radius-arcsec", type=float, default=5.0, help="Match radius in arcsec. Default: 5")
    ap.add_argument("--blocksize", type=int, default=1000, help="STILTS blocksize for cdsskymatch. Default: 1000")
    ap.add_argument("--keep-temp", action="store_true", help="Keep per-chunk match files (debug).")
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    if not run_dir.exists():
        raise SystemExit(f"run-dir not found: {run_dir}")

    out_dir = Path(args.out_dir) if args.out_dir else (run_dir / "stages")
    out_dir.mkdir(parents=True, exist_ok=True)

    chunks = sorted(run_dir.glob(args.input_glob))
    if not chunks:
        raise SystemExit(f"No input chunks matched: {run_dir}/{args.input_glob}")

    hdr = _read_header(chunks[0])
    src_col, ra_col, dec_col = _detect_cols(hdr)

    cdsskymatch_wrapper = _try_import_vasco_cdsskymatch()
    stilts_bin = _ensure_tool("stilts")

    stage = args.stage
    out_kept = out_dir / f"stage_{stage}_USNO.csv"
    out_flags = out_dir / f"stage_{stage}_USNO_flags.csv"
    out_ledger = out_dir / f"stage_{stage}_USNO_ledger.json"

    flags_fields = ["src_id", "ra", "dec", "has_usnob_match", "best_sep_arcsec", "source_chunk"]

    total_in = total_match = total_kept = 0
    per_chunk: List[ChunkStats] = []

    with out_kept.open("w", newline="", encoding="utf-8") as f_kept, out_flags.open(
        "w", newline="", encoding="utf-8"
    ) as f_flags:
        kept_w = csv.DictWriter(f_kept, fieldnames=["src_id", "ra", "dec"])
        kept_w.writeheader()

        flags_w = csv.DictWriter(f_flags, fieldnames=flags_fields)
        flags_w.writeheader()

        with tempfile.TemporaryDirectory(prefix="usno_stage_") as tdir:
            tdir_p = Path(tdir)

            for ch in chunks:
                in_rows = _count_data_rows(ch)
                if in_rows == 0:
                    per_chunk.append(ChunkStats(ch.name, 0, 0, 0))
                    continue

                match_out = tdir_p / f"{ch.stem}__usno_xmatch.csv"

                # Run cdsskymatch per chunk
                if cdsskymatch_wrapper is not None:
                    cdsskymatch_wrapper(
                        str(ch),
                        str(match_out),
                        ra=ra_col,
                        dec=dec_col,
                        cdstable=args.cdstable,
                        radius_arcsec=float(args.radius_arcsec),
                        find="best",
                        ofmt="csv",
                        omode="out",
                        blocksize=int(args.blocksize) if args.blocksize else None,
                    )
                else:
                    _run_stilts_cdsskymatch(
                        stilts_bin,
                        ch,
                        match_out,
                        ra_col=ra_col,
                        dec_col=dec_col,
                        cdstable=args.cdstable,
                        radius_arcsec=float(args.radius_arcsec),
                        find="best",
                        blocksize=int(args.blocksize) if args.blocksize else None,
                    )

                matches = _load_matches(match_out, src_col)
                mcount = len(matches)
                kcount = 0

                for sid, ra, dec in _iter_input_rows(ch, src_col, ra_col, dec_col):
                    has = 1 if sid in matches else 0
                    sep = matches.get(sid)

                    flags_w.writerow(
                        {
                            "src_id": sid,
                            "ra": ra,
                            "dec": dec,
                            "has_usnob_match": has,
                            "best_sep_arcsec": (f"{sep:.6f}" if (sep is not None and sep == sep) else ""),
                            "source_chunk": ch.name,
                        }
                    )

                    if has == 0:
                        kept_w.writerow({"src_id": sid, "ra": ra, "dec": dec})
                        kcount += 1

                per_chunk.append(ChunkStats(ch.name, in_rows, mcount, kcount))
                total_in += in_rows
                total_match += mcount
                total_kept += kcount

                if args.keep_temp:
                    dbg_dir = out_dir / "tmp_usno_matches"
                    dbg_dir.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(match_out, dbg_dir / match_out.name)

    ledger = {
        "run_dir": str(run_dir),
        "input_glob": args.input_glob,
        "stage": stage,
        "cdstable": args.cdstable,
        "radius_arcsec": float(args.radius_arcsec),
        "blocksize": int(args.blocksize) if args.blocksize else None,
        "input_chunks": [p.name for p in chunks],
        "totals": {"input_rows": total_in, "matched_rows": total_match, "kept_rows": total_kept},
        "per_chunk": [cs.__dict__ for cs in per_chunk],
        "outputs": {"kept_csv": str(out_kept), "flags_csv": str(out_flags), "ledger_json": str(out_ledger)},
        "columns_detected": {"src_id_col": src_col, "ra_col": ra_col, "dec_col": dec_col},
    }
    out_ledger.write_text(json.dumps(ledger, indent=2), encoding="utf-8")

    print(f"[USNO] chunks={len(chunks)} input_rows={total_in} matched={total_match} kept={total_kept}")
    print(f"[USNO] wrote: {out_kept}")
    print(f"[USNO] wrote: {out_flags}")
    print(f"[USNO] wrote: {out_ledger}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
