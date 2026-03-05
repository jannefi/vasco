#!/usr/bin/env python3
"""stage_supercosmos_post.py

Run-scoped postprocess stage: SuperCOSMOS cross-scan consistency check via GAVO TAP
(supercosmos.sources), with configurable elimination semantics.

Background / intended scientific meaning
--------------------------------------
MNRAS 2022 describes using a TAP query against the SuperCOSMOS digitization to remove
scan-related artifacts by *keeping* candidates that have a counterpart in the SuperCOSMOS
digitization within 5 arcsec. Likewise, Watters et al. classify the set that appears in DSS but
not SuperCOSMOS as scan/plate artifacts.

Therefore, for the "cross-scan artifact removal" use case, the default mode here is:
  - KEEP matches (has_supercosmos_match == 1)
  - DROP non-matches

If you intentionally want the opposite (rare; e.g., you are building a set of DSS-only features),
use --mode drop_matches.

Inputs
------
- A CSV (or set of CSVs via glob) containing at minimum: src_id, ra, dec
  (row_id is accepted as an alias for src_id)

Outputs (written under <run-dir>/stages by default)
---------------------------------------------------
1) stage_<STAGE>_SCOS.csv
   The kept remainder after applying the configured SCOS rule.
   Columns: src_id, ra, dec

2) stage_<STAGE>_SCOS_flags.csv
   Audit table for ALL input rows.
   Columns: src_id, ra, dec, has_supercosmos_match, matched_count, source_part

3) stage_<STAGE>_SCOS_ledger.json
   Totals + per-part stats + parameters.

Notes
-----
- Uses STILTS tapquery against GAVO TAP:
    tapurl=https://dc.g-vo.org/__system__/tap/run
  and joins against:
    supercosmos.sources
- Upload table name: TAP_UPLOAD.t1
- Query returns per-uploaded row_id match counts (nmatch).
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

DEFAULT_TAPURL = "https://dc.g-vo.org/__system__/tap/run"
DEFAULT_TABLE = "supercosmos.sources"


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


def _load_match_counts(csv_path: Path) -> Dict[str, int]:
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return {}
    with csv_path.open(newline="", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f)
        if not r.fieldnames:
            return {}
        out: Dict[str, int] = {}
        for row in r:
            rid = (row.get("row_id") or row.get("src_id") or "").strip()
            if not rid:
                continue
            try:
                n = int(float(row.get("nmatch", "1")))
            except Exception:
                n = 1
            out[rid] = n
        return out


@dataclass
class PartStats:
    part: str
    input_rows: int
    matched_rows: int
    kept_rows: int


def _write_csv(path: Path, fieldnames: List[str], rows: Iterable[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main() -> int:
    ap = argparse.ArgumentParser(description="Run-scoped SuperCOSMOS stage (TAP upload + shrink).")
    ap.add_argument("--run-dir", required=True, help="Run folder, e.g. ./work/runs/run-S1-20260302_185423")
    ap.add_argument(
        "--input-glob",
        default="stages/stage_S0_PS1SH.csv",
        help="Glob (relative to run-dir) for input CSV(s). Default: stages/stage_S0_PS1SH.csv",
    )
    ap.add_argument("--stage", default="S1", help="Stage label used in output filenames. Default: S1")
    ap.add_argument("--out-dir", default=None, help="Output directory. Default: <run-dir>/stages")

    ap.add_argument("--tapurl", default=DEFAULT_TAPURL, help=f"TAP URL. Default: {DEFAULT_TAPURL}")
    ap.add_argument("--table", default=DEFAULT_TABLE, help=f"SCOS table. Default: {DEFAULT_TABLE}")
    ap.add_argument("--radius-arcsec", type=float, default=5.0, help="Match radius in arcsec. Default: 5")

    ap.add_argument(
        "--chunk-size",
        type=int,
        default=5000,
        help="Upload chunk size (rows) to avoid TAP/upload limits. Default: 5000",
    )
    ap.add_argument(
        "--mode",
        choices=["keep_matches", "drop_matches"],
        default="keep_matches",
        help="Shrinking rule. Default keep_matches (cross-scan artifact removal).",
    )
    ap.add_argument("--keep-temp", action="store_true", help="Keep intermediate TAP outputs under <out-dir>/tmp_scos")

    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    if not run_dir.exists():
        raise SystemExit(f"run-dir not found: {run_dir}")

    out_dir = Path(args.out_dir) if args.out_dir else (run_dir / "stages")
    out_dir.mkdir(parents=True, exist_ok=True)

    inputs = sorted(run_dir.glob(args.input_glob))
    if not inputs:
        raise SystemExit(f"No inputs matched: {run_dir}/{args.input_glob}")

    hdr = _read_header(inputs[0])
    src_col, ra_col, dec_col = _detect_cols(hdr)

    stilts = _ensure_tool("stilts")

    rows: List[Tuple[str, float, float]] = []
    for p in inputs:
        rows.extend(list(_iter_rows(p, src_col, ra_col, dec_col)))

    # Uniqueness within stage input
    seen = set()
    uniq_rows: List[Tuple[str, float, float]] = []
    for sid, ra, dec in rows:
        if sid in seen:
            continue
        seen.add(sid)
        uniq_rows.append((sid, ra, dec))
    rows = uniq_rows

    stage = args.stage
    out_kept = out_dir / f"stage_{stage}_SCOS.csv"
    out_flags = out_dir / f"stage_{stage}_SCOS_flags.csv"
    out_ledger = out_dir / f"stage_{stage}_SCOS_ledger.json"

    flags_fields = ["src_id", "ra", "dec", "has_supercosmos_match", "matched_count", "source_part"]

    tmp_root: Optional[Path] = None
    if args.keep_temp:
        tmp_root = out_dir / "tmp_scos"
        tmp_root.mkdir(parents=True, exist_ok=True)

    total_in = len(rows)
    total_matched = 0
    total_kept = 0
    per_part: List[PartStats] = []

    keep_matches = args.mode == "keep_matches"

    with out_kept.open("w", newline="", encoding="utf-8") as f_kept, out_flags.open(
        "w", newline="", encoding="utf-8"
    ) as f_flags:
        kept_w = csv.DictWriter(f_kept, fieldnames=["src_id", "ra", "dec"])
        kept_w.writeheader()
        flags_w = csv.DictWriter(f_flags, fieldnames=flags_fields)
        flags_w.writeheader()

        chunk_size = max(1, int(args.chunk_size))
        for part_idx in range(0, len(rows), chunk_size):
            part_rows = rows[part_idx : part_idx + chunk_size]
            part_name = f"part_{(part_idx // chunk_size) + 1:04d}"

            with tempfile.TemporaryDirectory(prefix="scos_part_") as tdir:
                tdir_p = Path(tdir)
                upload_csv = tdir_p / f"{part_name}.csv"
                upload_vot = tdir_p / f"{part_name}.vot"
                tap_out = tdir_p / f"{part_name}_tap.csv"

                _write_csv(
                    upload_csv,
                    ["row_id", "ra", "dec"],
                    (
                        {"row_id": sid, "ra": f"{ra:.12f}", "dec": f"{dec:.12f}"}
                        for sid, ra, dec in part_rows
                    ),
                )

                subprocess.run(
                    [stilts, "tcopy", f"in={str(upload_csv)}", "ifmt=csv", f"out={str(upload_vot)}", "ofmt=votable"],
                    check=True,
                    text=True,
                    capture_output=True,
                )

                rarc = float(args.radius_arcsec)
                adql = (
                    "SELECT u.row_id AS row_id, COUNT(*) AS nmatch "
                    "FROM TAP_UPLOAD.t1 AS u "
                    f"JOIN {args.table} AS s "
                    "ON 1 = CONTAINS( "
                    "POINT('ICRS', s.raj2000, s.dej2000), "
                    f"CIRCLE('ICRS', u.ra, u.dec, {rarc}/3600.0) "
                    ") "
                    "GROUP BY u.row_id"
                )

                subprocess.run(
                    [
                        stilts,
                        "tapquery",
                        f"tapurl={args.tapurl}",
                        "nupload=1",
                        f"upload1={str(upload_vot)}",
                        "upname1=t1",
                        "ufmt1=votable",
                        f"adql={adql}",
                        f"out={str(tap_out)}",
                        "ofmt=csv",
                    ],
                    check=True,
                    text=True,
                    capture_output=True,
                )

                if tmp_root is not None:
                    shutil.copyfile(tap_out, tmp_root / tap_out.name)

                match_counts = _load_match_counts(tap_out)

                matched_rows = 0
                kept_rows = 0

                for sid, ra, dec in part_rows:
                    nmatch = match_counts.get(sid, 0)
                    has = 1 if nmatch and nmatch > 0 else 0
                    if has:
                        matched_rows += 1

                    # Decision: keep matches (default) or keep non-matches
                    keep = (has == 1) if keep_matches else (has == 0)
                    if keep:
                        kept_rows += 1
                        kept_w.writerow({"src_id": sid, "ra": f"{ra:.12f}", "dec": f"{dec:.12f}"})

                    flags_w.writerow(
                        {
                            "src_id": sid,
                            "ra": f"{ra:.12f}",
                            "dec": f"{dec:.12f}",
                            "has_supercosmos_match": has,
                            "matched_count": int(nmatch) if nmatch else 0,
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
        "tapurl": args.tapurl,
        "table": args.table,
        "radius_arcsec": float(args.radius_arcsec),
        "chunk_size": int(args.chunk_size),
        "mode": args.mode,
        "totals": {"input_rows": total_in, "matched_rows": total_matched, "kept_rows": total_kept},
        "per_part": [ps.__dict__ for ps in per_part],
        "outputs": {"kept_csv": str(out_kept), "flags_csv": str(out_flags), "ledger_json": str(out_ledger)},
        "columns_detected": {"src_id_col": src_col, "ra_col": ra_col, "dec_col": dec_col},
        "temp_outputs": str(tmp_root) if tmp_root is not None else None,
    }

    out_ledger.write_text(json.dumps(ledger, indent=2), encoding="utf-8")

    print(f"[SCOS] parts={len(per_part)} input_rows={total_in} matched={total_matched} kept={total_kept} mode={args.mode}")
    print(f"[SCOS] wrote: {out_kept}")
    print(f"[SCOS] wrote: {out_flags}")
    print(f"[SCOS] wrote: {out_ledger}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
