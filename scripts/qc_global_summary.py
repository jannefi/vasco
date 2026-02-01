#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Global QC summary for NEOWISE-SE strict matches (Post 1.6 compatible)

Usage:
  python qc_global_summary.py <flags_ALL.parquet> <summary.csv>

Optional:
  --radius-arcsec   Strict match radius in arcsec (default: 5.0)
  --markdown-out    Path to write a small Markdown summary (optional)

Accepts both minimal (Post 1.6) and legacy schemas:

Minimal (sidecar):
  - NUMBER (join key; equivalent to row_id)
  - has_ir_match (boolean)
  - dist_arcsec (float; optional)

Legacy:
  - row_id
  - ir_match_strict (boolean)
  - sep_arcsec (float)

Other optional columns are used if present:
  in_ra,in_dec,mjd,w1snr,w2snr,qual_frame,qi_fact,saa_sep,moon_masked,ra_bin,dec_bin
"""
import argparse
import math
import sys
from pathlib import Path
import numpy as np
import pandas as pd


def safe_num(df: pd.DataFrame, name: str, default: float = 0.0) -> pd.Series:
    if name in df.columns:
        return pd.to_numeric(df[name], errors="coerce")
    return pd.Series(np.full(len(df), default), index=df.index, dtype="float64")


def compute_matches(df: pd.DataFrame, radius_arcsec: float) -> pd.DataFrame:
    """
    Preference order for strict matches:
      1) has_ir_match (Post 1.6 minimal sidecar)
      2) ir_match_strict (legacy)
      3) sep_arcsec <= radius (legacy separation)
      4) dist_arcsec <= radius (minimal sidecar separation)
    """
    if "has_ir_match" in df.columns:
        strict = (
            df["has_ir_match"]
            .replace({"True": True, "False": False})
            .astype("boolean")
            .fillna(False)
        )
        return df[strict]

    if "ir_match_strict" in df.columns:
        strict = (
            df["ir_match_strict"]
            .replace({"True": True, "False": False})
            .astype("boolean")
            .fillna(False)
        )
        return df[strict]

    if "sep_arcsec" in df.columns:
        return df[pd.to_numeric(df["sep_arcsec"], errors="coerce").le(radius_arcsec)]

    if "dist_arcsec" in df.columns:
        return df[pd.to_numeric(df["dist_arcsec"], errors="coerce").le(radius_arcsec)]

    raise SystemExit(
        "Missing a usable strict-match signal: expected one of "
        "'has_ir_match', 'ir_match_strict', 'sep_arcsec', or 'dist_arcsec'."
    )


def main():
    ap = argparse.ArgumentParser(description="Global QC summary for NEOWISE-SE strict matches (Post 1.6 compatible)")
    ap.add_argument("flags_parquet", help="Path to *_flags_ALL.parquet")
    ap.add_argument("summary_csv", help="Path to write summary CSV")
    ap.add_argument("--radius-arcsec", type=float, default=5.0, help="Strict match radius (arcsec)")
    ap.add_argument("--markdown-out", type=str, default="", help="Optional Markdown summary path")
    args = ap.parse_args()

    flags_path = Path(args.flags_parquet)
    out_csv = Path(args.summary_csv)
    if not flags_path.exists():
        print(f"[ERROR] Flags parquet not found: {flags_path}", file=sys.stderr)
        sys.exit(2)

    # Read flags parquet
    try:
        df = pd.read_parquet(flags_path, engine="pyarrow")
    except Exception as e:
        print(f"[ERROR] Failed to read parquet: {e}", file=sys.stderr)
        sys.exit(2)

    # Normalize join key if the minimal file uses NUMBER
    if "row_id" not in df.columns and "NUMBER" in df.columns:
        df = df.rename(columns={"NUMBER": "row_id"})

    total_rows = int(len(df))
    if total_rows == 0:
        summary = {
            "total_rows": 0,
            "strict_matches": 0,
            "strict_match_rate": 0.0,
            "sep_arcsec_median": float("nan"),
            "sep_arcsec_p95": float("nan"),
            "w1_snr_ge5": 0,
            "w2_snr_ge5": 0,
            "any_snr_ge5": 0,
            "qual_frame_pos": 0,
            "qi_fact_pos": 0,
            "saa_sep_pos": 0,
            "moon_masked_eq_00": 0,
            "mjd_min": float("nan"),
            "mjd_max": float("nan"),
            "partitions_with_bins": 0,
            "rows_with_bins": 0,
        }
        pd.DataFrame([summary]).to_csv(out_csv, index=False)
        if args.markdown_out:
            Path(args.markdown_out).write_text("# NEOWISE-SE Global QC\n\n_No rows._\n", encoding="utf-8")
        print(f"[OK] Wrote summary (empty dataset): {out_csv}")
        return

    # Compute strict matches
    matches = compute_matches(df, radius_arcsec=args.radius_arcsec)
    mcount = int(len(matches))
    match_rate = (mcount / total_rows) if total_rows else 0.0

    # Separation stats (strict matches only): prefer sep_arcsec else dist_arcsec
    if "sep_arcsec" in matches.columns:
        sep = pd.to_numeric(matches["sep_arcsec"], errors="coerce").dropna()
    else:
        sep = pd.to_numeric(matches.get("dist_arcsec", pd.Series([], dtype="float64")), errors="coerce").dropna()
    sep_median = float(sep.median()) if len(sep) else float("nan")
    sep_p95 = float(sep.quantile(0.95)) if len(sep) else float("nan")

    # SNR bands (overall) — optional columns
    w1 = safe_num(df, "w1snr")
    w2 = safe_num(df, "w2snr")
    w1_ok = int((w1 >= 5).sum())
    w2_ok = int((w2 >= 5).sum())
    any_ok = int(((w1 >= 5) | (w2 >= 5)).sum())

    # Quality flags (overall) — optional columns
    qual_pos = int((safe_num(df, "qual_frame") > 0).sum())
    qif_pos = int((safe_num(df, "qi_fact") > 0).sum())
    saa_pos = int((safe_num(df, "saa_sep") > 0).sum())
    if "moon_masked" in df.columns:
        mm = df["moon_masked"].astype(str).str.strip()
        moon_ok = int(((mm == "00") | (mm == "0") | (mm == "000")).sum())
    else:
        moon_ok = 0

    # MJD coverage (overall) — optional
    mjd = safe_num(df, "mjd", default=float("nan")).dropna()
    mjd_min = float(mjd.min()) if len(mjd) else float("nan")
    mjd_max = float(mjd.max()) if len(mjd) else float("nan")

    # Partition awareness — optional
    if ("ra_bin" in df.columns) and ("dec_bin" in df.columns):
        rb = df["ra_bin"]; db = df["dec_bin"]
        rows_with_bins = int((rb.notna() & db.notna()).sum())
        pair_count = int(len(pd.DataFrame({"rb": rb, "db": db}).dropna().drop_duplicates()))
    else:
        rows_with_bins = 0
        pair_count = 0

    summary = {
        "total_rows": total_rows,
        "strict_matches": mcount,
        "strict_match_rate": round(match_rate, 6),
        "sep_arcsec_median": round(sep_median, 6) if not math.isnan(sep_median) else float("nan"),
        "sep_arcsec_p95": round(sep_p95, 6) if not math.isnan(sep_p95) else float("nan"),
        "w1_snr_ge5": w1_ok,
        "w2_snr_ge5": w2_ok,
        "any_snr_ge5": any_ok,
        "qual_frame_pos": qual_pos,
        "qi_fact_pos": qif_pos,
        "saa_sep_pos": saa_pos,
        "moon_masked_eq_00": moon_ok,
        "mjd_min": mjd_min,
        "mjd_max": mjd_max,
        "partitions_with_bins": pair_count,
        "rows_with_bins": rows_with_bins,
    }

    pd.DataFrame([summary]).to_csv(out_csv, index=False)
    print(f"[OK] Global QC summary written: {out_csv}")
    print(f"[INFO] totals={total_rows} strict={mcount} rate={match_rate:.3f} sep_med={sep_median:.3f} sep_p95={sep_p95:.3f}")

    if args.markdown_out:
        md_path = Path(args.markdown_out)
        md = (
            "# NEOWISE-SE Global QC Summary\n\n"
            f"- **Total rows**: {total_rows}\n"
            f"- **Strict matches (≤ {args.radius_arcsec:.1f}\")**: {mcount} ({match_rate:.3%})\n"
            f"- **Separation**: median={sep_median:.3f}\" p95={sep_p95:.3f}\"\n"
            f"- **SNR bands**: W1≥5={w1_ok}, W2≥5={w2_ok}, any≥5={any_ok}\n"
            f"- **Quality**: qual_frame>0={qual_pos}, qi_fact>0={qif_pos}, saa_sep>0={saa_pos}, moon_masked='00'={moon_ok}\n"
            f"- **MJD**: min={mjd_min:.3f} max={mjd_max:.3f}\n"
            f"- **Sidecar partitions**: pairs={pair_count}, rows_with_bins={rows_with_bins}\n"
        )
        md_path.write_text(md, encoding="utf-8")
        print(f"[OK] Markdown summary written: {md_path}")


if __name__ == "__main__":
    from pathlib import Path
    main()