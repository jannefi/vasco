#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Compare AWS vs TAP 'closest' outputs.

Improvements:
  - Fixed row_id canonicalization to avoid false pair mismatches.
  - Optional --unique-cntr to deduplicate by cntr before overlap stats.

Usage:
  python scripts/comparator_aws_vs_tap.py \
    --tap <TAP closest CSV> \
    --aws <AWS closest CSV> \
    --out-prefix ./data/local-cats/tmp/positions/aws_compare_out/compare_chunk00005 \
    --ra-dec-atol-arcsec 0.10 --mjd-atol 5e-5 --snr-rtol 1e-3 \
    --unique-cntr
"""
import argparse
import pandas as pd
import numpy as np
from decimal import Decimal, InvalidOperation

def canon_row_id(s: pd.Series) -> pd.Series:
    """
    Canonicalize row_id strings:
      - Strip whitespace
      - If numeric:
          * If integer-valued -> canonical integer string (e.g., '123.0' -> '123')
          * Else -> normalized decimal without trailing zeros (e.g., '123.4500' -> '123.45')
      - Else: leave as-is
    """
    def _norm_one(x):
        t = str(x).strip()
        if t == "":
            return ""
        # Try Decimal for exact normalization
        try:
            d = Decimal(t)
            # Normalize removes trailing zeros
            d_n = d.normalize()
            # If exponent >= 0 -> treat as integer string
            if d_n == d_n.to_integral_value():
                return str(d_n.to_integral_value())
            # Else keep decimal representation as minimal string
            # Convert to string without scientific notation, if possible
            s = format(d_n, 'f')
            # strip trailing zeros again (Decimal sometimes keeps them with 'f')
            s = s.rstrip('0').rstrip('.') if '.' in s else s
            return s
        except InvalidOperation:
            # Not numeric -> keep original trimmed
            return t
    return s.apply(_norm_one)

def norm_moon_masked(s: pd.Series) -> pd.Series:
    # Normalize 'moon_masked' to two-digit string '00' if 0-like
    return s.astype(str).str.replace(r'[^\d]', '', regex=True).str.zfill(2)

def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [c.lower() for c in df.columns]
    if 'row_id' in df.columns:
        df['row_id'] = canon_row_id(df['row_id'])
    if 'moon_masked' in df.columns:
        df['moon_masked'] = norm_moon_masked(df['moon_masked'])
    return df

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

def summarize(merged, col, suf=("_aws","_tap"), atol=0.0, rtol=0.0):
    a, b = merged.get(col+suf[0]), merged.get(col+suf[1])
    if a is None or b is None:
        return None
    a, b = a.astype(float), b.astype(float)
    d = (a - b).to_numpy()
    bad = ~np.isclose(a, b, atol=atol, rtol=rtol)
    return dict(n=int(d.size),
                mean=float(np.nanmean(d)),
                std=float(np.nanstd(d)),
                min=float(np.nanmin(d)),
                max=float(np.nanmax(d)),
                violations=int(bad.sum()))

def main():
    ap = argparse.ArgumentParser(description='Compare AWS vs TAP closest outputs')
    ap.add_argument('--tap', required=True)
    ap.add_argument('--aws', required=True)
    ap.add_argument('--out-prefix', default='./data/local-cats/tmp/positions/new/compare')
    ap.add_argument('--ra-dec-atol-arcsec', type=float, default=0.10)
    ap.add_argument('--mjd-atol', type=float, default=5e-5)
    ap.add_argument('--snr-rtol', type=float, default=1e-3)
    ap.add_argument('--unique-cntr', action='store_true',
                    help='Drop duplicate cntr rows on each side before overlap stats')
    a = ap.parse_args()

    tap = load_csv(a.tap)
    aws = load_csv(a.aws)

