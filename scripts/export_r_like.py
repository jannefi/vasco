#!/usr/bin/env python3
"""
export_r_like.py — v3.0 (remainder-capable; no DuckDB required)

- Reads masked union (file or directory of parquet parts)
- Normalizes tile_id from row_id (always)
- Chooses coordinate stream safely:
    RA_row/Dec_row preferred, else RA/Dec, else ra/dec, else ALPHAWIN/DELTAWIN
- Optional: apply "remainder" predicate (SkyBoT-aware) to produce the ~150 rows
- Optional: core-only filter using tile_plate_edge_report.csv
- Writes Parquet + optional CSV + metrics JSON sidecar

Examples:
  # Inclusive remainder (the 150 list)
  python scripts/export_r_like.py \
    --masked ./work/survivors_masked_union \
    --out ./data/vasco-candidates/post16/survivors_R_like_inclusive.provisional.parquet \
    --remainder-only true \
    --emit-csv true

  # Core-only remainder
  python scripts/export_r_like.py \
    --masked ./work/survivors_masked_union \
    --out ./data/vasco-candidates/post16/survivors_R_like_core_only.provisional.parquet \
    --remainder-only true \
    --core-only true \
    --edge-report ./data/metadata/tile_plate_edge_report.csv \
    --emit-csv true
"""

from __future__ import annotations
import argparse, json
from pathlib import Path
import pandas as pd

# Coordinate preference ladder (per-row coords first)
COMMON_RA  = ['RA_row','ra_row','RA','ra','ALPHAWIN_J2000','ALPHA_J2000']
COMMON_DEC = ['Dec_row','dec_row','Dec','DEC','dec','DELTAWIN_J2000','DELTA_J2000']

def pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def extract_tile_id(row_id: str) -> str:
    return str(row_id).split(':', 1)[0]

def normalize_coords(df: pd.DataFrame) -> pd.DataFrame:
    ra = pick_col(df, COMMON_RA)
    dec = pick_col(df, COMMON_DEC)
    if ra is None or dec is None:
        raise RuntimeError(f"No usable coord columns found. Have: {list(df.columns)[:50]} ...")
    out = df.copy()
    if ra != 'RA':
        out = out.rename(columns={ra: 'RA'})
    if dec != 'Dec':
        out = out.rename(columns={dec: 'Dec'})
    return out

def apply_remainder_predicate(df: pd.DataFrame) -> pd.DataFrame:
    # SkyBoT-aware remainder predicate (match your canonical logic)
    # keep rows where all gates are false/zero
    def col(name, default):
        return df[name] if name in df.columns else default

    hv = col("has_vosa_like_match", False)
    hs = col("is_supercosmos_artifact", False)
    ptf = col("ptf_match_ngood", 0)
    vx = col("is_known_variable_or_transient", False)
    sb = col("skybot_strict", False)

    # ptf may be bool or int; normalize to int-ish
    ptf0 = pd.to_numeric(ptf, errors="coerce").fillna(0).astype(int) if not isinstance(ptf, bool) else ptf.astype(int)

    mask = (~hv.astype(bool)) & (~hs.astype(bool)) & (ptf0 == 0) & (~vx.astype(bool)) & (~sb.astype(bool))
    return df.loc[mask].copy()

def apply_core_only(df: pd.DataFrame, edge_csv: Path) -> pd.DataFrame:
    if not edge_csv.exists():
        raise FileNotFoundError(f"Edge report CSV missing: {edge_csv}")
    er = pd.read_csv(edge_csv)
    low = {c.lower(): c for c in er.columns}
    need = {"tile_id","number","class_px","class_arcsec"}
    if not need.issubset(set(low.keys())):
        raise RuntimeError(f"Edge report missing columns {need - set(low.keys())}; has {list(er.columns)}")

    er = er.rename(columns={
        low["tile_id"]: "tile_id",
        low["number"]: "number",
        low["class_px"]: "class_px",
        low["class_arcsec"]: "class_arcsec",
    })
    er["is_core"] = (er["class_px"].astype(str).str.lower().eq("core") |
                    er["class_arcsec"].astype(str).str.lower().eq("core"))

    # Join on (tile_id, NUMBER)
    if "NUMBER" not in df.columns:
        raise RuntimeError("Input missing NUMBER column; cannot apply core-only join.")
    j = df.merge(er[["tile_id","number","is_core"]], left_on=["tile_id","NUMBER"], right_on=["tile_id","number"], how="inner")
    j = j.loc[j["is_core"]].copy()
    j = j.drop(columns=["number","is_core"], errors="ignore")
    return j

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--masked", required=True, help="Masked union parquet (dir or file)")
    ap.add_argument("--out", required=True, help="Output parquet path")
    ap.add_argument("--csv-out", default="", help="Optional CSV path (default: same basename)")
    ap.add_argument("--emit-csv", default="true", choices=["true","false"])
    ap.add_argument("--remainder-only", default="false", choices=["true","false"], help="Apply remainder predicate")
    ap.add_argument("--core-only", default="false", choices=["true","false"], help="Apply core-only edge class")
    ap.add_argument("--edge-report", default="./data/metadata/tile_plate_edge_report.csv")
    args = ap.parse_args()

    emit_csv = (args.emit_csv.lower() == "true")
    remainder_only = (args.remainder_only.lower() == "true")
    core_only = (args.core_only.lower() == "true")

    src = Path(args.masked)
    if src.is_dir():
        files = sorted(src.rglob("*.parquet"))
        if not files:
            raise FileNotFoundError(f"No parquet under {src}")
        df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    else:
        df = pd.read_parquet(src)

    if "row_id" not in df.columns:
        raise RuntimeError("Input missing row_id — cannot export safely.")

    # Always enforce tile_id derived from row_id
    df["tile_id"] = df["row_id"].astype(str).apply(extract_tile_id)

    # Normalize coords to RA/Dec with safe preference
    df = normalize_coords(df)

    # Apply remainder filter if requested
    if remainder_only:
        df = apply_remainder_predicate(df)

    # Apply core-only filter if requested
    if core_only:
        df = apply_core_only(df, Path(args.edge_report))

    # Select output columns (keep only what’s needed + gates)
    keep = [
        "row_id","NUMBER","tile_id","plate_id","date_obs_iso",
        "has_vosa_like_match","is_supercosmos_artifact","ptf_match_ngood",
        "is_known_variable_or_transient","skybot_strict","skybot_wide",
        "RA","Dec"
    ]
    keep_cols = [c for c in keep if c in df.columns]
    out_df = df[keep_cols].copy()

    out_parq = Path(args.out)
    out_parq.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_parquet(out_parq, index=False)

    csv_path = ""
    if emit_csv:
        csv_path = args.csv_out if args.csv_out else str(out_parq.with_suffix(".csv"))
        out_df.to_csv(csv_path, index=False)

    metrics = {
        "rows": int(len(out_df)),
        "source_masked": str(src.resolve()),
        "parquet": str(out_parq.resolve()),
        "csv": csv_path,
        "remainder_only": remainder_only,
        "core_only": core_only,
        "edge_report": str(Path(args.edge_report).resolve()) if core_only else "",
    }
    out_parq.with_suffix(".metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    print("[OK] wrote:", out_parq)
    if emit_csv:
        print("[OK] wrote:", csv_path)
    print("[OK] rows:", len(out_df))

if __name__ == "__main__":
    main()