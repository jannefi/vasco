#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gaia DR3 + PS1 DR2 cross-matches for one survivors chunk (CDS/TAPVizieR) using cdsskymatch.

Outputs under OUTROOT:
  parts/flags_gaia_ps1__<chunk>.parquet
  audit/gaia_ps1_audit__<chunk>.json
  ledger/gaia_ps1_ledger__<chunk>.json

This mirrors the proven structure of vizier_vosa_like_chunk.py:
- strict chunk schema checks
- RA/Dec sanitization
- cdsskymatch(... blocksize=1000, find=best)
- deterministic empty outputs on errors
- per-row flags + audit + ledger

Tables:
  Gaia DR3: I/355/gaiadr3
  PS1 DR2:  II/389/ps1_dr2
"""

from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # repo root for "vasco" imports

import argparse, json, time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from vasco.utils.cdsskymatch import cdsskymatch  # uses TAPVizieR/CDS cross-match under the hood

GAIA_TABLE_DEFAULT = "I/355/gaiadr3"
PS1_TABLE_DEFAULT  = "II/389/ps1_dr2"


def _ensure_dirs(root: Path):
    parts = root / "parts"; parts.mkdir(parents=True, exist_ok=True)
    audit = root / "audit"; audit.mkdir(parents=True, exist_ok=True)
    ledger = root / "ledger"; ledger.mkdir(parents=True, exist_ok=True)
    tmp = root / "tmp"; tmp.mkdir(parents=True, exist_ok=True)
    return {"parts": parts, "audit": audit, "ledger": ledger, "tmp": tmp}


def _write_parquet(df: pd.DataFrame, out: Path):
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), out, compression="zstd")


def _load_chunk(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    low = {c.lower(): c for c in df.columns}
    need = {"row_id", "ra", "dec"}
    if not need.issubset(set(low.keys())):
        raise SystemExit(f"[ERROR] chunk missing columns {need - set(low.keys())}: {csv_path}")

    df = df.rename(columns={
        low["row_id"]: "row_id",
        low["ra"]: "ra",
        low["dec"]: "dec",
    })

    # sanitize coordinates – prevents RA-domain errors at services
    df = df.dropna(subset=["ra", "dec"]).copy()
    df["ra"] = (df["ra"].astype(float) % 360.0)
    df["dec"] = df["dec"].astype(float).clip(-90.0, 90.0)

    # normalize row_id to string (stable joins)
    df["row_id"] = df["row_id"].astype(str)
    return df


def _is_domain_error(exc: Exception) -> bool:
    """
    Deterministic, non-retryable service errors.
    Keep it simple and string-based like your VOSA script.
    """
    msg = str(exc)
    needles = [
        "field ra < 0 or > 360",
        "malformed coordinates",
        "out of range in column ra",
        "invalid literal for float",
        "SAXParseException",
        "HTTP 400",
    ]
    return any(s.lower() in msg.lower() for s in needles)


def _parse_hits_row_id(out_csv: Path) -> set[str]:
    """
    Return row_id values that truly have a counterpart.

    IMPORTANT: some xmatch services return one output row per INPUT row, with
    match fields null when no counterpart exists. We must not treat "row exists"
    as "matched".
    """
    try:
        m = pd.read_csv(out_csv)
        if m.empty:
            return set()

        # Normalize row_id column name
        if "row_id" not in m.columns:
            rid_col = next(
                (c for c in m.columns
                 if c.lower().replace("_", "") in ("rowid", "rowid1", "rowid2", "row_id1", "row_id2")),
                None
            )
            if rid_col:
                m = m.rename(columns={rid_col: "row_id"})

        if "row_id" not in m.columns:
            return set()

        # Prefer a distance/sep column if present
        dist_col = next((c for c in m.columns if c.lower() in ("angdist", "angdistarcsec", "dist", "distance", "sep", "separation")), None)
        if dist_col is not None:
            # Hit if distance is a finite number (non-null)
            dist = pd.to_numeric(m[dist_col], errors="coerce")
            hits = set(m.loc[dist.notna(), "row_id"].astype(str))
            return hits

        # Fallback: treat as hit if any non-input column is non-null.
        # Typical input columns we uploaded:
        input_cols = {"ra", "dec", "row_id"}
        other_cols = [c for c in m.columns if c.lower() not in input_cols]

        # If there are no other columns, we can't decide; be conservative (no hits)
        if not other_cols:
            return set()

        mask = m[other_cols].notna().any(axis=1)
        hits = set(m.loc[mask, "row_id"].astype(str))
        return hits

    except Exception:
        return set()


def run_one(chunk_csv: Path,
            out_root: Path,
            radius_arcsec: float,
            gaia_table: str,
            ps1_table: str,
            overwrite: bool,
            blocksize: int = 1000):

    t0 = time.time()
    chunk_csv = Path(chunk_csv)
    out_root = Path(out_root)
    dirs = _ensure_dirs(out_root)
    chunk = chunk_csv.stem  # e.g., chunk_0000002

    flags_path = dirs["parts"] / f"flags_gaia_ps1__{chunk}.parquet"
    if flags_path.exists() and flags_path.stat().st_size > 0 and not overwrite:
        print(f"[skip] {chunk} (flags part exists)")
        return

    df = _load_chunk(chunk_csv)
    print(f"[run] {chunk} rows_in={len(df)} radius={radius_arcsec}\" gaia={gaia_table} ps1={ps1_table}")

    upload_csv = dirs["tmp"] / f"upload_{chunk}.csv"
    # upload includes row_id so we can compute hits cleanly
    df[["ra", "dec", "row_id"]].to_csv(upload_csv, index=False)

    matches_dir = dirs["tmp"] / f"matches_{chunk}"
    matches_dir.mkdir(parents=True, exist_ok=True)

    per_catalog_counts: dict[str, int] = {}
    per_catalog_domain_err: dict[str, bool] = {}

    # --- Gaia ---
    out_gaia = matches_dir / f"match_{chunk}__{gaia_table.replace('/','_')}.csv"
    domain_err = False
    try:
        cdsskymatch(
            str(upload_csv), str(out_gaia),
            ra="ra", dec="dec",
            cdstable=gaia_table,
            radius_arcsec=float(radius_arcsec),
            find="best", ofmt="csv", omode="out",
            blocksize=int(blocksize),
        )
    except Exception as e:
        if _is_domain_error(e):
            domain_err = True
            out_gaia.write_text("", encoding="utf-8")
            print(f"[domain] {chunk} {gaia_table}: {e}")
        else:
            out_gaia.write_text("", encoding="utf-8")
            print(f"[warn] {chunk} {gaia_table}: {e}")

    gaia_hits = _parse_hits_row_id(out_gaia)
    per_catalog_counts[gaia_table] = len(gaia_hits)
    per_catalog_domain_err[gaia_table] = domain_err
    print(f"[ok] {chunk} {gaia_table} matches={len(gaia_hits)}")

    # --- PS1 ---
    out_ps1 = matches_dir / f"match_{chunk}__{ps1_table.replace('/','_')}.csv"
    domain_err = False
    try:
        cdsskymatch(
            str(upload_csv), str(out_ps1),
            ra="ra", dec="dec",
            cdstable=ps1_table,
            radius_arcsec=float(radius_arcsec),
            find="best", ofmt="csv", omode="out",
            blocksize=int(blocksize),
        )
    except Exception as e:
        if _is_domain_error(e):
            domain_err = True
            out_ps1.write_text("", encoding="utf-8")
            print(f"[domain] {chunk} {ps1_table}: {e}")
        else:
            out_ps1.write_text("", encoding="utf-8")
            print(f"[warn] {chunk} {ps1_table}: {e}")

    ps1_hits = _parse_hits_row_id(out_ps1)
    per_catalog_counts[ps1_table] = len(ps1_hits)
    per_catalog_domain_err[ps1_table] = domain_err
    print(f"[ok] {chunk} {ps1_table} matches={len(ps1_hits)}")

    # Build flags frame
    flags = pd.DataFrame({"row_id": df["row_id"].astype(str)})
    flags["has_gaia_dr3_match"] = flags["row_id"].isin(gaia_hits)
    flags["has_ps1_dr2_match"] = flags["row_id"].isin(ps1_hits)
    flags["has_any_gaia_or_ps1_match"] = flags[["has_gaia_dr3_match", "has_ps1_dr2_match"]].any(axis=1)

    _write_parquet(flags, flags_path)

    # audit + ledger
    audit = {
        "chunk": chunk,
        "radius_arcsec": float(radius_arcsec),
        "tables": {"gaia": gaia_table, "ps1": ps1_table},
        "per_table_counts": {k: int(v) for k, v in per_catalog_counts.items()},
        "per_table_domain_error": {k: bool(v) for k, v in per_catalog_domain_err.items()},
    }
    (dirs["audit"] / f"gaia_ps1_audit__{chunk}.json").write_text(json.dumps(audit, indent=2), encoding="utf-8")

    ledger = {
        "chunk": chunk,
        "rows_in": int(len(df)),
        "elapsed_s": round(time.time() - t0, 3),
        "params": {
            "radius_arcsec": float(radius_arcsec),
            "gaia_table": gaia_table,
            "ps1_table": ps1_table,
            "blocksize": int(blocksize),
        },
        "counts": {
            "has_any_true": int(flags["has_any_gaia_or_ps1_match"].sum()),
            "gaia_matches": int(flags["has_gaia_dr3_match"].sum()),
            "ps1_matches": int(flags["has_ps1_dr2_match"].sum()),
        },
        "domain_errors": {k.replace("/", "_"): v for k, v in per_catalog_domain_err.items()},
    }
    (dirs["ledger"] / f"gaia_ps1_ledger__{chunk}.json").write_text(json.dumps(ledger, indent=2), encoding="utf-8")

    print(f"[done] {chunk} -> {flags_path} any_true={ledger['counts']['has_any_true']}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--chunk-csv", required=True)
    ap.add_argument("--out-root", default="./work/gaia_ps1_flags")
    ap.add_argument("--radius-arcsec", type=float, default=1.0)
    ap.add_argument("--gaia-table", default=GAIA_TABLE_DEFAULT)
    ap.add_argument("--ps1-table", default=PS1_TABLE_DEFAULT)
    ap.add_argument("--blocksize", type=int, default=1000)
    ap.add_argument("--overwrite", action="store_true", help="Recompute even if part exists")
    args = ap.parse_args()

    run_one(Path(args.chunk_csv), Path(args.out_root), args.radius_arcsec,
            args.gaia_table, args.ps1_table, args.overwrite, blocksize=args.blocksize)


if __name__ == "__main__":
    main()
