#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Comparator (AWS vs IRSA TAP) — fixed, with robust per‑chunk outputs.

This version:
  * Reads both closest CSVs, canonicalizes `row_id`, optional `--unique-cntr` de‑dup.
  * Adds side‑specific suffixes (`_aws`, `_tap`) to non‑key columns.
  * Applies the gating used in Post 1.5 (qual_frame>0, qi_fact>0, saa_sep>0, moon_masked=="00", w1snr>=5, mjd<=59198).
  * Computes angular separation in arcsec, `mjd` absolute diff, `w1snr` relative diff.
  * Writes per‑chunk artifacts:
       <out_prefix>_overlap.csv
       <out_prefix>_mismatches.csv
       <out_prefix>_missing_in_aws.csv
       <out_prefix>_missing_in_tap.csv
  * Appends a single‑line summary into sibling file `compare_summary.csv`.

Usage example:
  python scripts/comparator_aws_vs_tap_fixed.py \
    --tap ./data/local-cats/tmp/positions/TAP/02104/positions02104_closest.csv \
    --aws ./data/local-cats/tmp/positions/aws_compare_out/positions02104_closest.csv \
    --out-prefix ./data/local-cats/tmp/positions/aws_compare_out/compare_chunk02104 \
    --ra-dec-atol-arcsec 0.10 --mjd-atol 5e-5 --snr-rtol 1e-3
"""
import argparse
import math
import os
from pathlib import Path
import pandas as pd
import numpy as np
from decimal import Decimal, InvalidOperation

# ------------------------------- helpers -------------------------------

def canon_row_id(s: pd.Series) -> pd.Series:
    def _norm_one(x):
        t = str(x).strip()
        if t == "":
            return ""
        try:
            d = Decimal(t)
            d_n = d.normalize()
            if d_n == d_n.to_integral_value():
                return str(d_n.to_integral_value())
            s = format(d_n, 'f')
            s = s.rstrip('0').rstrip('.') if '.' in s else s
            return s
        except InvalidOperation:
            return t
    return s.apply(_norm_one)

def norm_moon_masked(s: pd.Series) -> pd.Series:
    return s.astype(str).str.replace(r'[^\\d]', '', regex=True).str.zfill(2)

def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [c.lower() for c in df.columns]
    if 'row_id' in df.columns:
        df['row_id'] = canon_row_id(df['row_id'])
    if 'moon_masked' in df.columns:
        df['moon_masked'] = norm_moon_masked(df['moon_masked'])
    return df

def suffixify(df: pd.DataFrame, suf: str, keys=("row_id","cntr")) -> pd.DataFrame:
    ren = {}
    for c in df.columns:
        if c not in keys:
            ren[c] = f"{c}{suf}"
    return df.rename(columns=ren)

def gates(df: pd.DataFrame, suffix: str) -> pd.Series:
    req = [f"qual_frame{suffix}", f"qi_fact{suffix}", f"saa_sep{suffix}",
           f"moon_masked{suffix}", f"w1snr{suffix}", f"mjd{suffix}"]
    if not set(req).issubset(df.columns):
        return pd.Series([True]*len(df), index=df.index)
    return ((df[f"qual_frame{suffix}"] > 0) &
            (df[f"qi_fact{suffix}"] > 0) &
            (df[f"saa_sep{suffix}"] > 0) &
            (df[f"moon_masked{suffix}"] == '00') &
            (df[f"w1snr{suffix}"] >= 5) &
            (df[f"mjd{suffix}"] <= 59198.0))

def ang_sep_arcsec(ra1_deg, dec1_deg, ra2_deg, dec2_deg):
    # great‑circle separation in arcsec
    ra1, dec1, ra2, dec2 = map(np.radians, [ra1_deg, dec1_deg, ra2_deg, dec2_deg])
    d = 2*np.arcsin(np.sqrt(np.sin((dec2-dec1)/2)**2 +
                            np.cos(dec1)*np.cos(dec2)*np.sin((ra2-ra1)/2)**2))
    return np.degrees(d)*3600.0

# ------------------------------- main -------------------------------

def main():
    ap = argparse.ArgumentParser(description='Compare AWS vs TAP closest outputs (fixed)')
    ap.add_argument('--tap', required=True)
    ap.add_argument('--aws', required=True)
    ap.add_argument('--out-prefix', required=True)
    ap.add_argument('--ra-dec-atol-arcsec', type=float, default=0.10)
    ap.add_argument('--mjd-atol', type=float, default=5e-5)
    ap.add_argument('--snr-rtol', type=float, default=1e-3)
    ap.add_argument('--unique-cntr', action='store_true',
                    help='Drop duplicate cntr rows on each side before overlap stats')
    args = ap.parse_args()

    tap_raw = load_csv(args.tap)
    aws_raw = load_csv(args.aws)

    # Optional de‑dup by cntr
    if args.unique_cntr and 'cntr' in tap_raw.columns:
        tap_raw = tap_raw.drop_duplicates(subset=['cntr'])
    if args.unique_cntr and 'cntr' in aws_raw.columns:
        aws_raw = aws_raw.drop_duplicates(subset=['cntr'])

    tap = suffixify(tap_raw, '_tap')
    aws = suffixify(aws_raw, '_aws')

    # Join key preference: row_id, fallback to cntr when row_id missing
    key = 'row_id' if 'row_id' in tap.columns and 'row_id' in aws.columns else ('cntr' if 'cntr' in tap.columns and 'cntr' in aws.columns else None)
    if key is None:
        raise SystemExit('Neither row_id nor cntr present on both sides; cannot compare.')

    # Outer merges to find missing rows
    merged_outer = pd.merge(aws[[key]], tap[[key]], on=key, how='outer', indicator=True)
    missing_in_aws = merged_outer[merged_outer['_merge'] == 'right_only'][[key]]
    missing_in_tap = merged_outer[merged_outer['_merge'] == 'left_only'][[key]]

    # Full inner merge for overlap
    merged = pd.merge(aws, tap, on=key, how='inner')

    # Apply gates side‑wise
    g_aws = gates(merged, '_aws')
    g_tap = gates(merged, '_tap')
    g_both = g_aws & g_tap

    # Pick RA/Dec/MJD/SNR columns with fallbacks
    def pick(col_base):
        for candidate in (f"{col_base}_aws", f"{col_base}_corr_aws", f"{col_base}corr_aws"):
            if candidate in merged.columns:
                a_col = candidate
                break
        else:
            a_col = None
        for candidate in (f"{col_base}_tap", f"{col_base}_corr_tap", f"{col_base}corr_tap"):
            if candidate in merged.columns:
                b_col = candidate
                break
        else:
            b_col = None
        return a_col, b_col

    ra_a, ra_b = pick('ra')
    dec_a, dec_b = pick('dec')
    mjd_a, mjd_b = pick('mjd')
    snr_a, snr_b = pick('w1snr')

    # Compute diffs only for rows where both sides are gated
    comp = merged.loc[g_both].copy()

    # Separation and diffs
    comp['sep_arcsec'] = ang_sep_arcsec(comp[ra_a], comp[dec_a], comp[ra_b], comp[dec_b]) if ra_a and ra_b and dec_a and dec_b else np.nan
    comp['delta_mjd'] = (comp[mjd_a] - comp[mjd_b]) if mjd_a and mjd_b else np.nan
    comp['snr_close'] = (np.isclose(comp[snr_a], comp[snr_b], rtol=args.snr_rtol)) if snr_a and snr_b else False

    comp['ra_dec_ok'] = comp['sep_arcsec'] <= args.ra_dec_atol_arcsec
    comp['mjd_ok'] = np.abs(comp['delta_mjd']) <= args.mjd_atol
    comp['all_ok'] = comp['ra_dec_ok'] & comp['mjd_ok'] & comp['snr_close']

    # Outputs
    out_prefix = Path(args.out_prefix)
    out_prefix.parent.mkdir(parents=True, exist_ok=True)

    # Overlap view with key columns
    keep_cols = [key,
                 ra_a, dec_a, mjd_a, snr_a,
                 ra_b, dec_b, mjd_b, snr_b,
                 'sep_arcsec','delta_mjd','snr_close','ra_dec_ok','mjd_ok','all_ok']
    keep_cols = [c for c in keep_cols if c is not None]
    comp_out = comp[keep_cols]
    comp_out.to_csv(f"{out_prefix}_overlap.csv", index=False)

    mismatches = comp_out[~comp['all_ok']]
    mismatches.to_csv(f"{out_prefix}_mismatches.csv", index=False)

    missing_in_aws.to_csv(f"{out_prefix}_missing_in_aws.csv", index=False)
    missing_in_tap.to_csv(f"{out_prefix}_missing_in_tap.csv", index=False)

    # Summary row appended to sibling compare_summary.csv
    summary_path = out_prefix.parent / 'compare_summary.csv'
    row = {
        'out_prefix': str(out_prefix.name),
        'key': key,
        'n_overlap_total': int(len(merged)),
        'n_overlap_gated': int(len(comp)),
        'n_match': int(comp['all_ok'].sum()),
        'n_mismatch': int((~comp['all_ok']).sum()),
        'n_missing_in_aws': int(len(missing_in_aws)),
        'n_missing_in_tap': int(len(missing_in_tap)),
        'ra_dec_atol_arcsec': args.ra_dec_atol_arcsec,
        'mjd_atol': args.mjd_atol,
        'snr_rtol': args.snr_rtol,
    }
    df_row = pd.DataFrame([row])
    if summary_path.exists():
        df_row.to_csv(summary_path, mode='a', header=False, index=False)
    else:
        df_row.to_csv(summary_path, index=False)

    print(f"[SUMMARY] {row}")

if __name__ == '__main__':
    main()
