#!/usr/bin/env python3
"""stage_ps1_and_sh_post.py

Run-scoped postprocess stage: PS1 DR2 veto (within radius) + optional Southern Hemisphere cut.

Why this exists
--------------
Tile-local PS1 neighbourhood caches (catalogs/ps1_neighbourhood.csv) frequently hit a max-records cap,
which truncates dense fields and makes the tile-local PS1 veto incomplete. This stage performs the
PS1 elimination at the run level against the full CDS PS1 DR2 table (II/389/ps1_dr2) using STILTS
cdsskymatch (find=best), and optionally removes all objects below a declination threshold (default 0°).

Inputs
------
A run directory containing chunked positional upload CSVs, e.g.:
  upload_positional_chunk_*.csv
or any glob you provide.
Each input CSV must contain at least:
  - src_id (or row_id)
  - ra, dec (or compatible variants)

Outputs (written under <run-dir>/stages by default)
---------------------------------------------------
1) stage_<STAGE>_PS1SH.csv
   The kept remainder AFTER (a) SH cut and (b) PS1 elimination.
   Columns: src_id, ra, dec

2) stage_<STAGE>_PS1SH_flags.csv
   Full audit table for ALL input rows.
   Columns: src_id, ra, dec, removed_by_sh_cut, has_ps1_match, best_sep_arcsec, source_chunk

3) stage_<STAGE>_PS1SH_ledger.json
   Totals + per-chunk stats + parameters.

Notes
-----
- Uses STILTS cdsskymatch against CDS table II/389/ps1_dr2.
- Elimination semantics: any best match within radius means the candidate is removed.
- SH cut is applied first (default: dec < 0.0 removed). Disable with --no-sh-cut.
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

DEFAULT_CDSTABLE = "II/389/ps1_dr2"  # PS1 DR2 (VizieR) table id for CDS XMatch / STILTS


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
    """Detect src_id + ra/dec columns from a header."""
    cset = {c.strip() for c in cols}

    # src_id
    if "src_id" in cset:
        src = "src_id"
    elif "row_id" in cset:
        src = "row_id"
    else:
        raise RuntimeError("Input CSV missing required id column 'src_id' (or 'row_id')")

    # ra/dec (degrees)
    ra_candidates = [
        "ra",
        "RA",
        "RA_corr",
        "ALPHAWIN_J2000",
        "ALPHA_J2000",
        "RA_ICRS",
        "RAJ2000",
    ]
    dec_candidates = [
        "dec",
        "DEC",
        "Dec",
        "Dec_corr",
        "DELTAWIN_J2000",
        "DELTA_J2000",
        "DE_ICRS",
        "DEJ2000",
    ]
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
            sid = (
                row.get(src_col, "")
                or row.get("src_id", "")
                or row.get("row_id", "")
                or ""
            )
            sid = sid.strip()
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


def _iter_input_rows(
    path: Path, src_col: str, ra_col: str, dec_col: str
) -> Iterable[Tuple[str, str, str]]:
    with path.open(newline="", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f)
        for row in r:
            sid = (row.get(src_col) or "").strip()
            ra = (row.get(ra_col) or "").strip()
            dec = (row.get(dec_col) or "").strip()
            if not sid or not ra or not dec:
                continue
            yield sid, ra, dec


@dataclass
class ChunkStats:
    chunk: str
    input_rows: int
    sh_dropped_rows: int
    matched_rows: int
    kept_rows: int


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Run-scoped PS1 DR2 veto stage + optional Southern Hemisphere cut (chunked inputs)."
    )
    ap.add_argument(
        "--run-dir",
        required=True,
        help="Run folder, e.g. ./work/runs/run-S1-20260302_185423",
    )
    ap.add_argument(
        "--input-glob",
        default="upload_positional*_chunk_*.csv",
        help="Glob pattern (relative to run-dir) for input chunk CSVs. Default: upload_positional*_chunk_*.csv",
    )
    ap.add_argument(
        "--stage",
        default="S0",
        help="Stage label used in output filenames. Default: S0",
    )
    ap.add_argument(
        "--out-dir",
        default=None,
        help="Output directory. Default: <run-dir>/stages",
    )
    ap.add_argument(
        "--cdstable",
        default=DEFAULT_CDSTABLE,
        help=f"CDS table id for PS1. Default: {DEFAULT_CDSTABLE}",
    )
    ap.add_argument(
        "--radius-arcsec",
        type=float,
        default=5.0,
        help="Match radius in arcsec. Default: 5",
    )
    ap.add_argument(
        "--blocksize",
        type=int,
        default=1000,
        help="STILTS blocksize for cdsskymatch. Default: 1000",
    )

    # Southern hemisphere cut
    ap.add_argument(
        "--sh-cut-dec-lt",
        type=float,
        default=0.0,
        help="Declination threshold for SH cut: remove rows with dec < this value. Default: 0.0",
    )
    ap.add_argument(
        "--no-sh-cut",
        action="store_true",
        help="Disable Southern Hemisphere cut entirely.",
    )

    ap.add_argument(
        "--keep-temp",
        action="store_true",
        help="Keep per-chunk match files (debug).",
    )
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

    stilts_bin = _ensure_tool("stilts")

    stage = args.stage
    out_kept = out_dir / f"stage_{stage}_PS1SH.csv"
    out_flags = out_dir / f"stage_{stage}_PS1SH_flags.csv"
    out_ledger = out_dir / f"stage_{stage}_PS1SH_ledger.json"

    flags_fields = [
        "src_id",
        "ra",
        "dec",
        "removed_by_sh_cut",
        "has_ps1_match",
        "best_sep_arcsec",
        "source_chunk",
    ]

    do_sh_cut = not bool(args.no_sh_cut)
    sh_dec_lt = float(args.sh_cut_dec_lt)

    total_in = 0
    total_sh = 0
    total_match = 0
    total_kept = 0
    per_chunk: List[ChunkStats] = []

    with out_kept.open("w", newline="", encoding="utf-8") as f_kept, out_flags.open(
        "w", newline="", encoding="utf-8"
    ) as f_flags:
        kept_w = csv.DictWriter(f_kept, fieldnames=["src_id", "ra", "dec"])
        kept_w.writeheader()

        flags_w = csv.DictWriter(f_flags, fieldnames=flags_fields)
        flags_w.writeheader()

        with tempfile.TemporaryDirectory(prefix="ps1sh_stage_") as tdir:
            tdir_p = Path(tdir)

            for ch in chunks:
                in_rows = _count_data_rows(ch)
                if in_rows == 0:
                    per_chunk.append(ChunkStats(ch.name, 0, 0, 0, 0))
                    continue

                # Build a filtered candidates file for PS1 matching (after SH cut)
                cand_csv = tdir_p / f"{ch.stem}__candidates_after_sh.csv"
                cand_fields = ["src_id", "ra", "dec"]

                sh_dropped = 0
                kept_for_match = 0

                # Read input rows once, apply SH cut and write candidate file
                rows_cache: List[Tuple[str, str, str, bool]] = []
                # tuple: (sid, ra, dec, removed_by_sh)
                with cand_csv.open("w", newline="", encoding="utf-8") as fc:
                    cw = csv.DictWriter(fc, fieldnames=cand_fields)
                    cw.writeheader()
                    for sid, ra, dec in _iter_input_rows(ch, src_col, ra_col, dec_col):
                        removed = False
                        if do_sh_cut:
                            try:
                                if float(dec) < sh_dec_lt:
                                    removed = True
                            except Exception:
                                # If dec is malformed, do NOT remove by SH cut; let PS1 stage handle it.
                                removed = False
                        rows_cache.append((sid, ra, dec, removed))
                        if removed:
                            sh_dropped += 1
                        else:
                            cw.writerow({"src_id": sid, "ra": ra, "dec": dec})
                            kept_for_match += 1

                # If no candidates remain after SH cut, just emit flags and move on.
                matches: Dict[str, float] = {}
                if kept_for_match > 0:
                    match_out = tdir_p / f"{ch.stem}__ps1_cdsskymatch.csv"
                    _run_stilts_cdsskymatch(
                        stilts_bin,
                        cand_csv,
                        match_out,
                        ra_col="ra",
                        dec_col="dec",
                        cdstable=args.cdstable,
                        radius_arcsec=float(args.radius_arcsec),
                        find="best",
                        blocksize=int(args.blocksize) if args.blocksize else None,
                    )
                    matches = _load_matches(match_out, "src_id")
                    if args.keep_temp:
                        dbg_dir = out_dir / "tmp_ps1sh_matches"
                        dbg_dir.mkdir(parents=True, exist_ok=True)
                        shutil.copyfile(match_out, dbg_dir / match_out.name)

                mcount = len(matches)
                kcount = 0

                # Emit flags for all rows and emit kept remainder
                for sid, ra, dec, removed in rows_cache:
                    has = 0
                    sep = None
                    if not removed and sid in matches:
                        has = 1
                        sep = matches.get(sid)

                    flags_w.writerow(
                        {
                            "src_id": sid,
                            "ra": ra,
                            "dec": dec,
                            "removed_by_sh_cut": 1 if removed else 0,
                            "has_ps1_match": has,
                            "best_sep_arcsec": (
                                f"{sep:.6f}" if (sep is not None and sep == sep) else ""
                            ),
                            "source_chunk": ch.name,
                        }
                    )

                    # Keep if not removed by SH cut and no PS1 match
                    if (not removed) and (has == 0):
                        kept_w.writerow({"src_id": sid, "ra": ra, "dec": dec})
                        kcount += 1

                per_chunk.append(ChunkStats(ch.name, in_rows, sh_dropped, mcount, kcount))
                total_in += in_rows
                total_sh += sh_dropped
                total_match += mcount
                total_kept += kcount

    ledger = {
        "run_dir": str(run_dir),
        "input_glob": args.input_glob,
        "stage": stage,
        "cdstable": args.cdstable,
        "radius_arcsec": float(args.radius_arcsec),
        "blocksize": int(args.blocksize) if args.blocksize else None,
        "sh_cut": {
            "enabled": bool(do_sh_cut),
            "dec_lt": float(sh_dec_lt),
        },
        "input_chunks": [p.name for p in chunks],
        "totals": {
            "input_rows": total_in,
            "sh_dropped_rows": total_sh,
            "matched_rows": total_match,
            "kept_rows": total_kept,
        },
        "per_chunk": [cs.__dict__ for cs in per_chunk],
        "outputs": {
            "kept_csv": str(out_kept),
            "flags_csv": str(out_flags),
            "ledger_json": str(out_ledger),
        },
        "columns_detected": {"src_id_col": src_col, "ra_col": ra_col, "dec_col": dec_col},
    }

    out_ledger.write_text(json.dumps(ledger, indent=2), encoding="utf-8")

    print(
        f"[PS1SH] chunks={len(chunks)} input_rows={total_in} "
        f"sh_dropped={total_sh} matched={total_match} kept={total_kept}"
    )
    print(f"[PS1SH] wrote: {out_kept}")
    print(f"[PS1SH] wrote: {out_flags}")
    print(f"[PS1SH] wrote: {out_ledger}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
