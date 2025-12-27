
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
QC summary for a NEOWISE-SE chunk 'closest' CSV.

Inputs:
  - CSV columns: row_id (string), ra, dec,
    optional: in_ra, in_dec, sep_arcsec or sep_deg,
    cntr, mjd, w1snr, w2snr, qual_frame, qi_fact, saa_sep, moon_masked

Output (stdout):
  - Totals, matches within 5", median/95% sep,
    SNR band counts, and quality flag distributions.
"""

import sys
import math
import numpy as np
import pandas as pd


def sep_arcsec(ra1, dec1, ra2, dec2) -> float:
    """Great-circle separation in arcseconds."""
    d2r = math.pi / 180.0
    ra1, dec1, ra2, dec2 = ra1 * d2r, dec1 * d2r, ra2 * d2r, dec2 * d2r
    s = math.acos(
        max(
            -1.0,
            min(
                1.0,
                math.sin(dec1) * math.sin(dec2)
                + math.cos(dec1) * math.cos(dec2) * math.cos(ra1 - ra2),
            ),
        )
    )
    return s * (180.0 / math.pi) * 3600.0


def safe_num(df: pd.DataFrame, name: str) -> pd.Series:
    """
    Return a numeric Series for column `name`.
    If absent, return a Series of zeros (float).
    """
    if name in df.columns:
        return pd.to_numeric(df[name], errors="coerce")
    return pd.Series(np.zeros(len(df)), index=df.index, dtype="float64")


def main():
    if len(sys.argv) != 2:
        print("Usage: qc_chunk_summary.py <closest_csv>")
        sys.exit(1)

    infile = sys.argv[1]
    df = pd.read_csv(infile, dtype={"row_id": "string"})

    # Coerce likely-numeric columns to numeric (NaN on failure)
    for col in (
        "ra",
        "dec",
        "in_ra",
        "in_dec",
        "w1snr",
        "w2snr",
        "qual_frame",
        "qi_fact",
        "saa_sep",
        "sep_deg",
        "sep_arcsec",
        "mjd",
    ):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Ensure sep_arcsec exists
    if "sep_arcsec" not in df.columns or df["sep_arcsec"].isna().all():
        if "sep_deg" in df.columns:
            df["sep_arcsec"] = pd.to_numeric(df["sep_deg"], errors="coerce") * 3600.0
        elif {"in_ra", "in_dec", "ra", "dec"}.issubset(df.columns):
            df["sep_arcsec"] = [
                sep_arcsec(ra1, dec1, ra2, dec2)
                for ra1, dec1, ra2, dec2 in zip(
                    df["in_ra"], df["in_dec"], df["ra"], df["dec"]
                )
            ]
        else:
            df["sep_arcsec"] = np.nan  # quantiles will be NaN-safe below

    total = len(df)
    matches = df[df["sep_arcsec"].le(5.0)]
    mcount = len(matches)

    # Robust quantiles with dropna
    med_sep = (
        matches["sep_arcsec"].dropna().median() if mcount else float("nan")
    )
    p95_sep = (
        matches["sep_arcsec"].dropna().quantile(0.95) if mcount else float("nan")
    )

    # SNR bands (FIXED: vectorized OR for any_snr_ok)
    w1 = safe_num(df, "w1snr")
    w2 = safe_num(df, "w2snr")
    w1_ok = int((w1 >= 5).sum())
    w2_ok = int((w2 >= 5).sum())
    any_snr_ok = int(((w1 >= 5) | (w2 >= 5)).sum())

    # Quality flags
    qf_ok = int((safe_num(df, "qual_frame") > 0).sum())
    qif_ok = int((safe_num(df, "qi_fact") > 0).sum())
    saa_ok = int((safe_num(df, "saa_sep") > 0).sum())
    moon_ok = int((df.get("moon_masked", pd.Series([""], index=df.index)).astype(str) == "00").sum())

    # Print summary lines
    if total:
        print(
            f"[QC] file={infile}"
        )
        print(
            f"[QC] total_rows={total} matches_<=5arcsec={mcount} match_rate={mcount/total:.3f}"
        )
    else:
        print(f"[QC] file={infile}")
        print("[QC] total_rows=0 matches_<=5arcsec=0 match_rate=0.000")

    # Format numeric prints robustly
    print(f"[QC] sep_arcsec median={med_sep:.3f} p95={p95_sep:.3f}")
    print(f"[QC] SNR: W1>=5={w1_ok} W2>=5={w2_ok} any>=5={any_snr_ok}")
    print(
        f"[QC] flags: qual_frame>0={qf_ok} qi_fact>0={qif_ok} saa_sep>0={saa_ok} moon_masked='00'={moon_ok}"
    )


if __name__ == "__main__":
    main()