#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VOSA-equivalent cross-matches for one survivors chunk at 5".

Outputs under OUTROOT:
  parts/flags_vosa_like__<chunk>.parquet
  audit/vosa_like_audit__<chunk>.json
  ledger/vosa_like_ledger__<chunk>.json
"""

from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # repo root for "vasco" imports

import argparse, json, time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# existing helper you already use elsewhere
from vasco.utils.cdsskymatch import cdsskymatch  # <— uses TAPVizieR/CDS cross-match under the hood

DEFAULT_TABLES = [
    "II/365/catwise",     # CatWISE2020
    "II/363/unwise",      # unWISE band-merged
    "II/328/allwise",     # AllWISE Source Catalog
    "II/246/out",         # 2MASS PSC
    "II/335/galex_ais",   # GALEX AIS (GUVcat GR6+7)
]

# --- small utils ---
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
    need = {"number", "row_id", "ra", "dec"}
    if not need.issubset(set(low.keys())):
        raise SystemExit(f"[ERROR] chunk missing columns {need - set(low.keys())}: {csv_path}")
    df = df.rename(columns={
        low["number"]: "number",
        low["row_id"]: "row_id",
        low["ra"]: "ra",
        low["dec"]: "dec",
    })
    # sanitize coordinates – prevents RA-domain errors at services
    df = df.dropna(subset=["ra","dec"]).copy()
    df["ra"]  = (df["ra"].astype(float) % 360.0)
    df["dec"] = df["dec"].astype(float).clip(-90.0, 90.0)
    return df

def _is_domain_error(exc: Exception) -> bool:
    """
    Classify deterministic, non-retryable service errors.
    We keep this simple and string-based — enough for GALEX RA message.
    """
    msg = str(exc)
    needles = [
        "field RA < 0 or > 360",                  # CDS Xmatch GALEX message
        "malformed coordinates",                  # generic tap parse domain
        "out of range in column RA",              # variants
        "invalid literal for float",              # VO-table parse on bad RA/Dec
        "SAXParseException",                      # XML parse → treat as domain if service includes the message above
    ]
    return any(s.lower() in msg.lower() for s in needles)

def run_one(chunk_csv: Path, out_root: Path, radius_arcsec: float, cat_tables: list[str], overwrite: bool):
    t0 = time.time()
    chunk_csv = Path(chunk_csv)
    out_root = Path(out_root)
    dirs = _ensure_dirs(out_root)
    chunk = chunk_csv.stem  # e.g., "chunk_0000002"

    flags_path = dirs["parts"] / f"flags_vosa_like__{chunk}.parquet"
    if flags_path.exists() and flags_path.stat().st_size > 0 and not overwrite:
        print(f"[skip] {chunk} (flags part exists)")
        return

    df = _load_chunk(chunk_csv)
    print(f"[run]  {chunk} rows_in={len(df)} cats={len(cat_tables)} radius={radius_arcsec}\"")

    upload_csv = dirs["tmp"] / f"upload_{chunk}.csv"
    df[["ra","dec","row_id"]].to_csv(upload_csv, index=False)

    per_catalog_hits: dict[str, set[str]] = {}
    per_catalog_counts: dict[str, int] = {}
    per_catalog_domain_err: dict[str, bool] = {}

    matches_dir = dirs["tmp"] / f"matches_{chunk}"
    matches_dir.mkdir(parents=True, exist_ok=True)

    for cat in cat_tables:
        out_csv = matches_dir / f"match_{chunk}__{cat.replace('/','_')}.csv"
        domain_err = False
        try:
            cdsskymatch(
                str(upload_csv), str(out_csv),
                ra="ra", dec="dec",
                cdstable=cat,
                radius_arcsec=float(radius_arcsec),
                find="best", ofmt="csv", omode="out",
                blocksize=1000,
            )
        except Exception as e:
            # classify deterministic domain errors (e.g., GALEX RA) as "no matches"
            if _is_domain_error(e):
                domain_err = True
                out_csv.write_text('', encoding='utf-8')  # explicit empty result
                print(f"[domain] {chunk} {cat}: {e}")
            else:
                # transient or unknown — keep behavior consistent: produce empty and continue
                out_csv.write_text('', encoding='utf-8')
                print(f"[warn]   {chunk} {cat}: {e}")

        # parse matches (empty file -> 0)
        try:
            m = pd.read_csv(out_csv)
            if "row_id" not in m.columns:
                # common fallbacks from various upload services
                rid_col = next((c for c in m.columns if c.lower().replace("_","") in ("rowid","rowid1","row_id1")), None)
                if rid_col:
                    m = m.rename(columns={rid_col: "row_id"})
            hits = set(m["row_id"].astype(str)) if "row_id" in m.columns else set()
        except Exception:
            hits = set()

        per_catalog_hits[cat] = hits
        per_catalog_counts[cat] = len(hits)
        per_catalog_domain_err[cat] = domain_err
        print(f"[ok]    {chunk} {cat} matches={len(hits)}")

    # Build flags frame
    flags = pd.DataFrame({"row_id": df["row_id"].astype(str)})
    name_map = {
        "II/365/catwise":   "has_catwise2020_match",
        "II/363/unwise":    "has_unwise_match",
        "II/328/allwise":   "has_allwise_match",
        "II/246/out":       "has_2mass_match",
        "II/335/galex_ais": "has_galex_match",
    }
    for cat in cat_tables:
        col = name_map.get(cat, f"has_{cat.replace('/','_')}_match")
        flags[col] = flags["row_id"].isin(per_catalog_hits[cat])
    cat_cols = [c for c in flags.columns if c.startswith("has_") and c.endswith("_match")]
    flags["has_vosa_like_match"] = flags[cat_cols].any(axis=1)

    _write_parquet(flags, flags_path)

    # audit + ledger
    audit = {
        "chunk": chunk,
        "radius_arcsec": float(radius_arcsec),
        "catalogs": cat_tables,
        "per_catalog_counts": per_catalog_counts,
        "per_catalog_domain_error": per_catalog_domain_err,
    }
    (dirs["audit"] / f"vosa_like_audit__{chunk}.json").write_text(json.dumps(audit, indent=2), encoding="utf-8")

    ledger = {
        "chunk": chunk,
        "rows_in": int(len(df)),
        "elapsed_s": round(time.time() - t0, 3),
        "params": {"radius_arcsec": float(radius_arcsec), "catalogs": cat_tables},
        "counts": {
            "any_vosa_like_true": int(flags["has_vosa_like_match"].sum()),
            **{k.replace("/","_"): v for k,v in per_catalog_counts.items()}
        },
        "domain_errors": {k.replace("/","_"): v for k,v in per_catalog_domain_err.items()},
    }
    (dirs["ledger"] / f"vosa_like_ledger__{chunk}.json").write_text(json.dumps(ledger, indent=2), encoding="utf-8")
    print(f"[done]  {chunk} → {flags_path}  any_vosa_like={ledger['counts']['any_vosa_like_true']}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--chunk-csv", required=True)
    ap.add_argument("--out-root", default="data/local-cats/_master_optical_parquet_flags/vosa_like")
    ap.add_argument("--radius-arcsec", type=float, default=5.0)
    ap.add_argument("--catalogs", nargs="*", default=DEFAULT_TABLES)
    ap.add_argument("--overwrite", action="store_true", help="Recompute even if part exists")
    args = ap.parse_args()
    run_one(Path(args.chunk_csv), Path(args.out_root), args.radius_arcsec, args.catalogs, args.overwrite)

if __name__ == "__main__":
    main()

