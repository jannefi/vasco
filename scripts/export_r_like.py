#!/usr/bin/env python3
# export_r_like.py — v2.0 (tile-safe)
# Outputs clean survivors CSV/Parquet using tile_id extracted from row_id

import argparse, json
from pathlib import Path
import pandas as pd


COMMON_RA  = ['RA_row','RA','ra','RA_corr','ALPHAWIN_J2000','ra_deg','alpha_j2000']
COMMON_DEC = ['Dec_row','Dec','DEC','dec','Dec_corr','DEC_corr','DELTAWIN_J2000','dec_deg','delta_j2000']

def pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def normalize_radec(df):
    out = df.copy()
    ra = pick_col(out, COMMON_RA)
    de = pick_col(out, COMMON_DEC)
    if ra and ra != 'RA':
        out = out.rename(columns={ra: 'RA'})
    if de and de != 'Dec':
        out = out.rename(columns={de: 'Dec'})
    return out

def extract_tile_id(row_id):
    return row_id.split(':', 1)[0]

def main():
    ap = argparse.ArgumentParser(description="Export clean survivors (tile-safe R-like)")
    ap.add_argument("--masked", required=True, help="Path to survivors parquet (dir or file)")
    ap.add_argument("--out", required=True, help="Output parquet path")
    ap.add_argument("--csv-out", default="", help="Optional CSV output path")
    ap.add_argument("--emit-csv", default="true", choices=["true","false"])
    args = ap.parse_args()

    emit_csv = (args.emit_csv.lower() == "true")

    print(f"[INFO] Loading survivors: {args.masked}")
    p = Path(args.masked)

    if p.is_dir():
        files = sorted(p.rglob("*.parquet"))
        if not files:
            raise FileNotFoundError(f"No .parquet files under {p}")
        df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    else:
        df = pd.read_parquet(p)

    print(f"[INFO] Loaded {len(df):,} rows, {len(df.columns)} columns")

    # Ensure row_id exists
    if "row_id" not in df.columns:
        raise RuntimeError("Survivor parquet missing row_id column — cannot export safely.")

    # Derive tile_id from row_id ALWAYS
    df["tile_id_clean"] = df["row_id"].apply(extract_tile_id)

    # If plate_id exists, KEEP it but DO NOT trust mismatched tile_id
    tile_mismatch = None
    if "tile_id" in df.columns:
        tile_mismatch = (df["tile_id"] != df["tile_id_clean"]).sum()
        print(f"[INFO] Existing tile_id mismatches detected: {tile_mismatch}")

    # Replace tile_id with clean version
    df["tile_id"] = df["tile_id_clean"]
    df = df.drop(columns=["tile_id_clean"], errors="ignore")

    # Normalize RA/Dec
    df = normalize_radec(df)

    # Choose final output columns
    keep_cols = [c for c in
                 ["tile_id","plate_id","row_id","NUMBER","RA","Dec","date_obs_iso",
                  "has_vosa_like_match","is_supercosmos_artifact","ptf_match_ngood",
                  "is_known_variable_or_transient","skybot_strict","skybot_wide"]
                 if c in df.columns]

    out_df = df[keep_cols].copy()

    out_parq = Path(args.out)
    out_parq.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_parquet(out_parq, index=False)
    print(f"[OK] Parquet written: {out_parq}")

    csv_path = ""
    if emit_csv:
        csv_path = args.csv_out if args.csv_out else str(out_parq.with_suffix(".csv"))
        out_df.to_csv(csv_path, index=False)
        print(f"[OK] CSV written: {csv_path}")

    metrics = {
        "rows": int(len(out_df)),
        "mismatched_tile_ids_before_fix": int(tile_mismatch) if tile_mismatch is not None else "N/A",
        "source_masked": str(Path(args.masked).resolve()),
        "parquet": str(out_parq.resolve()),
        "csv": csv_path,
    }

    with open(out_parq.with_suffix(".metrics.json"), "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    print(f"[OK] Metrics sidecar written")

if __name__ == "__main__":
    main()
