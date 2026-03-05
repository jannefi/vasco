#!/usr/bin/env python3
"""stage_ptf_post.py

Run-scoped postprocess stage: PTF / other-archive veto using IRSA TAP (/sync), with ngoodobs gate.

Why
---
Legacy PTF scripts were written for older parquet-based flows and relied on NUMBER/objectnumber handling.
In the current shrinking-set CSV workflow we operate on stage CSVs with:
  src_id, ra, dec
and we want a simple, reproducible stage script that:
  1) uploads positions to IRSA TAP (/sync)
  2) applies a positional match within radius
  3) applies the MNRAS 2022 quality gate: COALESCE(p.ngoodobs,0) > 0
  4) flags matches ("has_ptf_match_ngood") and shrinks the stage by dropping matches

Inputs
------
- One CSV or a glob of CSVs (relative to --run-dir) containing at minimum:
    src_id (or row_id), ra, dec

Outputs (written under <run-dir>/stages by default)
---------------------------------------------------
1) stage_<STAGE>_PTF.csv
   Kept remainder (non-matches).
   Columns: src_id, ra, dec

2) stage_<STAGE>_PTF_flags.csv
   Audit table for ALL input rows.
   Columns: src_id, ra, dec, has_ptf_match_ngood, best_sep_arcsec(blank), source_part

3) stage_<STAGE>_PTF_ledger.json
   Totals + per-part stats + parameters.

Notes
-----
- Uses curl multipart upload to IRSA TAP /sync (same approach as the legacy ngood script).
- Uses STILTS only to convert CSV -> VOTable for UPLOAD.
- The ADQL is single-line and avoids reserved keywords (no NUMBER/number).
- This stage is designed to be easy to later swap from PTF to ZTF by changing --ptf-table
  and possibly the column names in the ADQL (keep the same I/O contract).

Reference
---------
Legacy scripts (now deprecated by this stage):
- fetch_ptf_objects_ngood_stilts.sh (IRSA TAP /sync + ngood gate)
- do_ptf_get_ngood.sh (chunk runner + parquet merge)
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

IRSA_TAP_SYNC_URL = "https://irsa.ipac.caltech.edu/TAP/sync"


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


def _csv_to_votable(stilts: str, in_csv: Path, out_vot: Path) -> None:
    subprocess.run(
        [stilts, "tcopy", f"in={str(in_csv)}", "ifmt=csv", f"out={str(out_vot)}", "ofmt=votable"],
        check=True,
        text=True,
        capture_output=True,
    )


def _irsa_query(
    curl: str,
    vot_path: Path,
    out_csv: Path,
    hdr_path: Path,
    *,
    adql: str,
    max_time: int,
    connect_timeout: int,
    retries: int,
) -> int:
    """Execute IRSA TAP /sync query. Returns HTTP status code (int) if available, else 0."""

    # Note: curl --retry-all-errors requires curl >= 7.71; if older, curl will ignore it.
    cmd = [
        curl,
        "-sS",
        "-X",
        "POST",
        IRSA_TAP_SYNC_URL,
        "--retry",
        str(retries),
        "--retry-delay",
        "2",
        "--retry-max-time",
        str(max_time),
        "--retry-all-errors",
        "--connect-timeout",
        str(connect_timeout),
        "--max-time",
        str(max_time),
        "-F",
        "REQUEST=doQuery",
        "-F",
        "LANG=ADQL",
        "-F",
        "FORMAT=csv",
        "-F",
        "UPLOAD=my_table,param:table",
        "-F",
        f"table=@{str(vot_path)};type=application/x-votable+xml",
        "-F",
        f"QUERY={adql}",
        "-D",
        str(hdr_path),
        "-w",
        "%{http_code}",
        "-o",
        str(out_csv),
    ]

    p = subprocess.run(cmd, text=True, capture_output=True)
    # stdout is http_code due to -w
    try:
        http = int((p.stdout or "").strip() or "0")
    except Exception:
        http = 0
    if p.returncode != 0:
        raise RuntimeError(f"curl failed rc={p.returncode} stderr={p.stderr[:200]}")
    return http


def _load_matches_csv(path: Path) -> set[str]:
    """IRSA CSV output: expects a single column 'row_id' (or alias). Returns set of matched src_id."""
    if not path.exists() or path.stat().st_size == 0:
        return set()
    with path.open(newline="", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f)
        if not r.fieldnames:
            return set()
        # Accept row_id or src_id as returned column name
        fields = {c.strip() for c in r.fieldnames}
        col = "row_id" if "row_id" in fields else ("src_id" if "src_id" in fields else None)
        if col is None:
            # Some TAP servers may return as 'objectnumber' etc; be defensive
            col = r.fieldnames[0]
        out = set()
        for row in r:
            sid = (row.get(col) or "").strip()
            if sid:
                out.add(sid)
        return out


@dataclass
class PartStats:
    part: str
    input_rows: int
    matched_rows: int
    kept_rows: int
    http_status: int


def main() -> int:
    ap = argparse.ArgumentParser(description="Run-scoped PTF ngood-gated stage via IRSA TAP (/sync).")
    ap.add_argument("--run-dir", required=True, help="Run folder, e.g. ./work/runs/run-S1-...")
    ap.add_argument(
        "--input-glob",
        default="stages/stage_S3_SCOS.csv",
        help="Glob (relative to run-dir) for input stage CSV(s). Default: stages/stage_S3_SCOS.csv",
    )
    ap.add_argument("--stage", default="S4", help="Stage label used in output filenames. Default: S4")
    ap.add_argument("--out-dir", default=None, help="Output directory. Default: <run-dir>/stages")

    ap.add_argument("--radius-arcsec", type=float, default=5.0, help="Match radius in arcsec. Default: 5")
    ap.add_argument(
        "--ptf-table",
        default="ptf_objects",
        help="IRSA TAP table name for PTF objects. Default: ptf_objects",
    )
    ap.add_argument(
        "--chunk-size",
        type=int,
        default=2000,
        help="Upload chunk size (rows). Default: 2000 (safe for TAP uploads)",
    )
    ap.add_argument("--max-time", type=int, default=300, help="curl max-time seconds per request. Default: 300")
    ap.add_argument("--connect-timeout", type=int, default=20, help="curl connect-timeout seconds. Default: 20")
    ap.add_argument("--retries", type=int, default=6, help="curl retry count. Default: 6")

    ap.add_argument("--keep-temp", action="store_true", help="Keep intermediate per-part responses under <out-dir>/tmp_ptf")

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
    curl = _ensure_tool("curl")

    # Load and de-dup stage input
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
    out_kept = out_dir / f"stage_{stage}_PTF.csv"
    out_flags = out_dir / f"stage_{stage}_PTF_flags.csv"
    out_ledger = out_dir / f"stage_{stage}_PTF_ledger.json"

    flags_fields = ["src_id", "ra", "dec", "has_ptf_match_ngood", "best_sep_arcsec", "source_part"]

    tmp_root: Optional[Path] = None
    if args.keep_temp:
        tmp_root = out_dir / "tmp_ptf"
        tmp_root.mkdir(parents=True, exist_ok=True)

    total_in = len(rows)
    total_matched = 0
    total_kept = 0
    per_part: List[PartStats] = []

    with out_kept.open("w", newline="", encoding="utf-8") as f_kept, out_flags.open(
        "w", newline="", encoding="utf-8"
    ) as f_flags:
        kept_w = csv.DictWriter(f_kept, fieldnames=["src_id", "ra", "dec"])
        kept_w.writeheader()
        flags_w = csv.DictWriter(f_flags, fieldnames=flags_fields)
        flags_w.writeheader()

        chunk_size = max(1, int(args.chunk_size))
        rarc = float(args.radius_arcsec)

        for part_idx in range(0, len(rows), chunk_size):
            part_rows = rows[part_idx : part_idx + chunk_size]
            part_name = f"part_{(part_idx // chunk_size) + 1:04d}"

            with tempfile.TemporaryDirectory(prefix="ptf_part_") as tdir:
                tdir_p = Path(tdir)
                upload_csv = tdir_p / f"{part_name}.csv"
                upload_vot = tdir_p / f"{part_name}.vot"
                resp_csv = tdir_p / f"{part_name}_resp.csv"
                hdr_txt = tdir_p / f"{part_name}_headers.txt"

                # Upload schema: row_id, ra, dec (row_id == src_id)
                _write_csv(
                    upload_csv,
                    ["row_id", "ra", "dec"],
                    (
                        {"row_id": sid, "ra": f"{ra:.12f}", "dec": f"{dec:.12f}"}
                        for sid, ra, dec in part_rows
                    ),
                )

                _csv_to_votable(stilts, upload_csv, upload_vot)

                # ADQL: return matching row_ids; apply ngoodobs gate
                # Note: PTF table is expected to expose p.ra, p.dec, and p.ngoodobs
                adql = (
                    "SELECT DISTINCT u.row_id AS row_id "
                    "FROM TAP_UPLOAD.my_table AS u, "
                    f"{args.ptf_table} AS p "
                    "WHERE CONTAINS(POINT('ICRS', p.ra, p.dec), CIRCLE('ICRS', u.ra, u.dec, "
                    f"{rarc}/3600.0)) = 1 "
                    "AND COALESCE(p.ngoodobs,0) > 0"
                )

                http = _irsa_query(
                    curl,
                    upload_vot,
                    resp_csv,
                    hdr_txt,
                    adql=adql,
                    max_time=int(args.max_time),
                    connect_timeout=int(args.connect_timeout),
                    retries=int(args.retries),
                )

                if tmp_root is not None:
                    shutil.copyfile(resp_csv, tmp_root / f"{part_name}_resp.csv")
                    shutil.copyfile(hdr_txt, tmp_root / f"{part_name}_headers.txt")

                matched_set = _load_matches_csv(resp_csv)

                matched_rows = 0
                kept_rows = 0

                for sid, ra, dec in part_rows:
                    has = 1 if sid in matched_set else 0
                    if has:
                        matched_rows += 1
                    else:
                        kept_rows += 1
                        kept_w.writerow({"src_id": sid, "ra": f"{ra:.12f}", "dec": f"{dec:.12f}"})

                    flags_w.writerow(
                        {
                            "src_id": sid,
                            "ra": f"{ra:.12f}",
                            "dec": f"{dec:.12f}",
                            "has_ptf_match_ngood": has,
                            # IRSA query returns only ids; sep not available here (leave blank)
                            "best_sep_arcsec": "",
                            "source_part": part_name,
                        }
                    )

                per_part.append(PartStats(part_name, len(part_rows), matched_rows, kept_rows, http))
                total_matched += matched_rows
                total_kept += kept_rows

    ledger = {
        "run_dir": str(run_dir),
        "input_glob": args.input_glob,
        "stage": stage,
        "ptf_table": args.ptf_table,
        "radius_arcsec": float(args.radius_arcsec),
        "chunk_size": int(args.chunk_size),
        "irsa_tap_sync_url": IRSA_TAP_SYNC_URL,
        "ngood_gate": "COALESCE(p.ngoodobs,0) > 0",
        "totals": {"input_rows": total_in, "matched_rows": total_matched, "kept_rows": total_kept},
        "per_part": [ps.__dict__ for ps in per_part],
        "outputs": {"kept_csv": str(out_kept), "flags_csv": str(out_flags), "ledger_json": str(out_ledger)},
        "columns_detected": {"src_id_col": src_col, "ra_col": ra_col, "dec_col": dec_col},
        "temp_outputs": str(tmp_root) if tmp_root is not None else None,
    }

    out_ledger.write_text(json.dumps(ledger, indent=2), encoding="utf-8")

    print(f"[PTF] parts={len(per_part)} input_rows={total_in} matched={total_matched} kept={total_kept}")
    print(f"[PTF] wrote: {out_kept}")
    print(f"[PTF] wrote: {out_flags}")
    print(f"[PTF] wrote: {out_ledger}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
