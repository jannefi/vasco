#!/usr/bin/env python3
"""stage_vsx_post.py

Run-scoped postprocess stage: VSX known variables/transients flagging using a LOCAL VSX mirror
(FITS) and STILTS tskymatch2, then shrinking-set elimination (drop any candidate with a VSX
counterpart within radius).

This replaces legacy chunked bash fetchers that produced Parquet and relied on older schemas.

Inputs
------
- One CSV or glob of CSVs (relative to --run-dir) containing at minimum:
    src_id (or row_id), ra, dec

Outputs (written under <run-dir>/stages by default)
---------------------------------------------------
1) stage_<STAGE>_VSX.csv
   Kept remainder AFTER VSX elimination (non-matches).
   Columns: src_id, ra, dec

2) stage_<STAGE>_VSX_flags.csv
   Audit table for ALL input rows.
   Columns: src_id, ra, dec, is_known_variable_or_transient, matched_count, best_sep_arcsec, source_part

3) stage_<STAGE>_VSX_ledger.json
   Totals + per-part stats + parameters.

Notes
-----
- Uses STILTS tskymatch2 with join=1and2 find=best
- VSX mirror FITS is expected to contain columns RAdeg and DEdeg (as in vsx_master_slim.fits)
- Shrinking rule: drop matches (is_known_variable_or_transient==1)
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

DEFAULT_VSX_FITS = "./data/local-cats/_external_catalogs/vsx/vsx_master_slim.fits"


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
    cset = {c.strip() for c in cols}
    if "src_id" in cset:
        src = "src_id"
    elif "row_id" in cset:
        src = "row_id"
    else:
        raise RuntimeError("Input CSV missing required id column 'src_id' (or 'row_id')")

    ra_candidates = ["ra", "RA", "RA_corr", "ALPHAWIN_J2000", "ALPHA_J2000", "RA_ICRS", "RAJ2000"]
    dec_candidates = ["dec", "DEC", "Dec", "Dec_corr", "DELTAWIN_J2000", "DELTA_J2000", "DE_ICRS", "DEJ2000"]

    ra = next((c for c in ra_candidates if c in cset), None)
    dec = next((c for c in dec_candidates if c in cset), None)
    if not ra or not dec:
        raise RuntimeError("Input CSV missing RA/Dec columns (expected ra/dec or common variants)")

    return src, ra, dec


def _iter_rows(path: Path, src_col: str, ra_col: str, dec_col: str) -> Iterable[Tuple[str, float, float]]:
    with path.open(newline="", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f)
        for row in r:
            sid = (row.get(src_col) or "").strip()
            if not sid:
                continue
            try:
                ra = float((row.get(ra_col) or "").strip())
                dec = float((row.get(dec_col) or "").strip())
            except Exception:
                continue
            yield sid, ra, dec


def _write_csv(path: Path, fieldnames: List[str], rows: Iterable[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


@dataclass
class PartStats:
    part: str
    input_rows: int
    matched_rows: int
    kept_rows: int


def _run_tskymatch2(stilts: str, in_csv: Path, vsx_fits: Path, out_csv: Path, radius_arcsec: float) -> None:
    subprocess.run(
        [
            stilts,
            "tskymatch2",
            f"in1={str(in_csv)}",
            "ra1=ra",
            "dec1=dec",
            f"in2={str(vsx_fits)}",
            "ra2=RAdeg",
            "dec2=DEdeg",
            f"error={radius_arcsec}",
            "join=1and2",
            "find=best",
            f"out={str(out_csv)}",
            "ofmt=csv",
        ],
        check=True,
        text=True,
        capture_output=True,
    )


def _load_matches(match_csv: Path) -> Tuple[Dict[str, int], Dict[str, float]]:
    """Return (count_by_src_id, best_sep_arcsec_by_src_id) from tskymatch2 output."""
    if not match_csv.exists() or match_csv.stat().st_size == 0:
        return {}, {}
    with match_csv.open(newline="", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f)
        if not r.fieldnames:
            return {}, {}
        fields = set(r.fieldnames)
        # STILTS commonly emits 'Separation' (unit can vary; we treat as arcsec if <= error)
        sep_col = None
        for cand in ("Separation", "angDist", "sep_arcsec", "sep"):
            if cand in fields:
                sep_col = cand
                break
        counts: Dict[str, int] = {}
        best: Dict[str, float] = {}
        for row in r:
            sid = (row.get("src_id") or row.get("row_id") or "").strip()
            if not sid:
                continue
            counts[sid] = counts.get(sid, 0) + 1
            if sep_col:
                try:
                    val = float(row.get(sep_col, "nan"))
                    # heuristic: if very small, might be degrees; convert to arcsec
                    sep_as = val if val > 0.1 else val * 3600.0
                except Exception:
                    sep_as = float("nan")
                if sep_as == sep_as:
                    if (sid not in best) or (sep_as < best[sid]):
                        best[sid] = sep_as
        return counts, best


def main() -> int:
    ap = argparse.ArgumentParser(description="Run-scoped VSX local stage (FITS mirror + tskymatch2).")
    ap.add_argument("--run-dir", required=True, help="Run folder, e.g. ./work/runs/run-S1-...")
    ap.add_argument(
        "--input-glob",
        default="stages/stage_S4_PTF.csv",
        help="Glob (relative to run-dir) for input stage CSV(s). Default: stages/stage_S4_PTF.csv",
    )
    ap.add_argument("--stage", default="S5", help="Stage label used in output filenames. Default: S5")
    ap.add_argument("--out-dir", default=None, help="Output directory. Default: <run-dir>/stages")

    ap.add_argument("--radius-arcsec", type=float, default=5.0, help="Match radius in arcsec. Default: 5")
    ap.add_argument(
        "--vsx-fits",
        default=DEFAULT_VSX_FITS,
        help=f"Path to local VSX FITS mirror. Default: {DEFAULT_VSX_FITS}",
    )
    ap.add_argument(
        "--chunk-size",
        type=int,
        default=5000,
        help="Chunk size for matching (rows). Default: 5000",
    )
    ap.add_argument("--keep-temp", action="store_true", help="Keep per-part match CSVs under <out-dir>/tmp_vsx")

    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    if not run_dir.exists():
        raise SystemExit(f"run-dir not found: {run_dir}")

    out_dir = Path(args.out_dir) if args.out_dir else (run_dir / "stages")
    out_dir.mkdir(parents=True, exist_ok=True)

    inputs = sorted(run_dir.glob(args.input_glob))
    if not inputs:
        raise SystemExit(f"No inputs matched: {run_dir}/{args.input_glob}")

    vsx_fits = Path(args.vsx_fits)
    if not vsx_fits.exists() or vsx_fits.stat().st_size == 0:
        raise SystemExit(f"VSX FITS not found or empty: {vsx_fits}")

    hdr = _read_header(inputs[0])
    src_col, ra_col, dec_col = _detect_cols(hdr)

    stilts = _ensure_tool("stilts")

    # Load and de-dup
    rows: List[Tuple[str, float, float]] = []
    for p in inputs:
        rows.extend(list(_iter_rows(p, src_col, ra_col, dec_col)))

    seen = set()
    uniq: List[Tuple[str, float, float]] = []
    for sid, ra, dec in rows:
        if sid in seen:
            continue
        seen.add(sid)
        uniq.append((sid, ra, dec))
    rows = uniq

    stage = args.stage
    out_kept = out_dir / f"stage_{stage}_VSX.csv"
    out_flags = out_dir / f"stage_{stage}_VSX_flags.csv"
    out_ledger = out_dir / f"stage_{stage}_VSX_ledger.json"

    flags_fields = ["src_id", "ra", "dec", "is_known_variable_or_transient", "matched_count", "best_sep_arcsec", "source_part"]

    tmp_root: Optional[Path] = None
    if args.keep_temp:
        tmp_root = out_dir / "tmp_vsx"
        tmp_root.mkdir(parents=True, exist_ok=True)

    total_in = len(rows)
    total_matched = 0
    total_kept = 0
    per_part: List[PartStats] = []

    chunk_size = max(1, int(args.chunk_size))
    rarc = float(args.radius_arcsec)

    with out_kept.open("w", newline="", encoding="utf-8") as f_kept, out_flags.open(
        "w", newline="", encoding="utf-8"
    ) as f_flags:
        kept_w = csv.DictWriter(f_kept, fieldnames=["src_id", "ra", "dec"])
        kept_w.writeheader()
        flags_w = csv.DictWriter(f_flags, fieldnames=flags_fields)
        flags_w.writeheader()

        for part_idx in range(0, len(rows), chunk_size):
            part_rows = rows[part_idx : part_idx + chunk_size]
            part_name = f"part_{(part_idx // chunk_size) + 1:04d}"

            with tempfile.TemporaryDirectory(prefix="vsx_part_") as tdir:
                tdir_p = Path(tdir)
                in_csv = tdir_p / f"{part_name}.csv"
                match_csv = tdir_p / f"{part_name}_match.csv"

                _write_csv(
                    in_csv,
                    ["src_id", "ra", "dec"],
                    ({"src_id": sid, "ra": f"{ra:.12f}", "dec": f"{dec:.12f}"} for sid, ra, dec in part_rows),
                )

                _run_tskymatch2(stilts, in_csv, vsx_fits, match_csv, rarc)

                if tmp_root is not None:
                    shutil.copyfile(match_csv, tmp_root / match_csv.name)

                cnt_by, best_by = _load_matches(match_csv)

                matched_rows = 0
                kept_rows = 0

                for sid, ra, dec in part_rows:
                    n = cnt_by.get(sid, 0)
                    is_vsx = 1 if n > 0 else 0
                    if is_vsx:
                        matched_rows += 1
                    else:
                        kept_rows += 1
                        kept_w.writerow({"src_id": sid, "ra": f"{ra:.12f}", "dec": f"{dec:.12f}"})

                    sep = best_by.get(sid)
                    flags_w.writerow(
                        {
                            "src_id": sid,
                            "ra": f"{ra:.12f}",
                            "dec": f"{dec:.12f}",
                            "is_known_variable_or_transient": is_vsx,
                            "matched_count": int(n),
                            "best_sep_arcsec": f"{sep:.6f}" if (sep is not None and sep == sep) else "",
                            "source_part": part_name,
                        }
                    )

                per_part.append(PartStats(part_name, len(part_rows), matched_rows, kept_rows))
                total_matched += matched_rows
                total_kept += kept_rows

    ledger = {
        "run_dir": str(run_dir),
        "input_glob": args.input_glob,
        "stage": stage,
        "radius_arcsec": float(args.radius_arcsec),
        "vsx_fits": str(vsx_fits),
        "backend": "LOCAL VSX (slim FITS)",
        "chunk_size": int(args.chunk_size),
        "totals": {"input_rows": total_in, "matched_rows": total_matched, "kept_rows": total_kept},
        "per_part": [ps.__dict__ for ps in per_part],
        "outputs": {"kept_csv": str(out_kept), "flags_csv": str(out_flags), "ledger_json": str(out_ledger)},
        "columns_detected": {"src_id_col": src_col, "ra_col": ra_col, "dec_col": dec_col},
        "temp_outputs": str(tmp_root) if tmp_root is not None else None,
    }

    out_ledger.write_text(json.dumps(ledger, indent=2), encoding="utf-8")

    print(f"[VSX] parts={len(per_part)} input_rows={total_in} matched={total_matched} kept={total_kept}")
    print(f"[VSX] wrote: {out_kept}")
    print(f"[VSX] wrote: {out_flags}")
    print(f"[VSX] wrote: {out_ledger}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
