#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Explain TAP-only cntrs by probing NASA Parquet leaves under TAP-equivalent gates.

Usage:
  # List of cntrs
  python scripts/explain_tap_only.py \
    --tap ./data/local-cats/tmp/positions/TAP/00005/positions00005_closest.csv \
    --aws ./data/local-cats/tmp/positions/aws_compare_out/positions00005_closest.csv \
    --cntrs "12345, 67890" --radius 5

  # From comparator CSV (auto-extract 'cntr' column)
  python scripts/explain_tap_only.py \
    --tap ./data/local-cats/tmp/positions/TAP/00005/positions00005_closest.csv \
    --aws ./data/local-cats/tmp/positions/aws_compare_out/positions00005_closest.csv \
    --cntrs ./data/local-cats/tmp/positions/aws_compare_out/compare_chunk00005.tap_only_by_cntr.csv \
    --radius 5
"""
import argparse, math, numpy as np, pandas as pd, pyarrow as pa
import pyarrow.dataset as pds, pyarrow.compute as pc
from pyarrow import fs as pafs

def arcsec2rad(a): return a / 206264.806

def bbox(ra0, dec0, r_arcsec):
    ddeg = math.degrees(arcsec2rad(r_arcsec))
    ra = pc.field('ra'); dec = pc.field('dec')
    ra_min, ra_max = (ra0 - ddeg) % 360.0, (ra0 + ddeg) % 360.0
    if ra_min <= ra_max:
        fra = (ra >= ra_min) & (ra <= ra_max)
    else:
        fra = (ra >= ra_min) | (ra <= ra_max)
    fdec = (dec >= dec0 - ddeg) & (dec <= dec0 + ddeg)
    return fra & fdec

def years_all(): return [f"year{i}" for i in range(1,12)] + ["addendum"]

def leaf(year, k5):
    k0 = k5 // 1024
    return f"nasa-irsa-wise/wise/neowiser/catalogs/p1bs_psd/healpix_k5/{year}/" \
           f"neowiser-healpix_k5-{year}.parquet/healpix_k0={k0}/healpix_k5={k5}/"

def k5_index(ra_deg, dec_deg):
    # Try healpy, then astropy_healpix, then hpgeom
    try:
        import healpy as hp
        nside = 2**5
        theta = np.deg2rad(90.0 - dec_deg)
        phi   = np.deg2rad(ra_deg % 360.0)
        return int(hp.ang2pix(nside, theta, phi, nest=True))
    except Exception:
        pass
    try:
        from astropy_healpix import HEALPix
        nside = 2**5
        return int(HEALPix(nside=nside, order='nested').lonlat_to_healpix(
            np.deg2rad(ra_deg), np.deg2rad(dec_deg)))
    except Exception:
        pass
    try:
        import hpgeom as hpg, healpy as hp
        th, ph = hpg.lonlat_to_thetaphi(ra_deg, dec_deg, degrees=True)
        return int(hpg.thetaphi_to_healpix(th, ph, order=5, nest=True)) \
               if hasattr(hpg,"thetaphi_to_healpix") else int(hp.ang2pix(2**5, th, ph, nest=True))
    except Exception:
        pass
    raise RuntimeError("HEALPix indexing failed.")

def parse_cntrs_arg(arg: str) -> list:
    """
    Return a list of int cntrs from a string or a CSV file path.
    - If arg is a file path: read CSV and use 'cntr' column if present; else
      try to find an integer-like column.
    - Else: parse tokens split by commas/space and keep only integers.
    """
    import os
    if os.path.exists(arg):
        df = pd.read_csv(arg)
        cols_lower = {c.lower(): c for c in df.columns}
        if 'cntr' in cols_lower:
            col = cols_lower['cntr']
            return [int(x) for x in df[col].dropna().astype(int).tolist()]
        # try to find an integer-like column
        for c in df.columns:
            try:
                return [int(x) for x in df[c].dropna().astype(int).tolist()]
            except Exception:
                continue
        return []
    # not a file -> token list
    toks = [t.strip() for t in arg.replace(',', ' ').split() if t.strip()]
    cntrs = []
    for t in toks:
        try:
            cntrs.append(int(t))
        except Exception:
            # ignore non-integer tokens
            pass
    return cntrs

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--tap', required=True)
    ap.add_argument('--aws', required=True)
    ap.add_argument('--cntrs', required=True,
                    help='Comma/space-separated list of cntrs OR CSV path with a "cntr" column')
    ap.add_argument('--radius', type=float, default=5.0)
    ap.add_argument('--years', type=str, default="")
    a = ap.parse_args()

    tap = pd.read_csv(a.tap)
    aws = pd.read_csv(a.aws)
    tap.columns = [c.lower() for c in tap.columns]
    aws.columns = [c.lower() for c in aws.columns]

    # derive seed RA/DEC per cntr from TAP input
    if not {'in_ra','in_dec','cntr'}.issubset(tap.columns):
        raise RuntimeError("TAP CSV missing required columns in_ra,in_dec,cntr")

    cntrs = parse_cntrs_arg(a.cntrs)
    if not cntrs:
        raise RuntimeError("No integer cntrs parsed from --cntrs.")

    years = [p.strip() for p in a.years.replace(',',' ').split() if p.strip()] if a.years else years_all()

    fs = pafs.S3FileSystem(anonymous=True, region='us-west-2')
    # TAP-equivalent gates (numeric-only pushdown; moon_masked normalization handled post-read if needed)
    gates = ((pc.field('qual_frame') > 0) &
             (pc.field('qi_fact')    > 0.0) &
             (pc.field('saa_sep')    > 0.0) &
             (pc.field('w1snr')      >= 5.0) &
             (pc.field('mjd')        <= 59198.0))

    for cn in cntrs:
        rows = tap.loc[tap['cntr'] == cn, ['in_ra','in_dec','cntr']]
        if rows.empty:
            print(f"\ncntr={cn}: not present in TAP input rows (skipping).")
            continue
        ra0 = float(rows['in_ra'].iloc[0]) % 360.0
        dec0 = float(rows['in_dec'].iloc[0])
        k5 = k5_index(ra0, dec0)
        print(f"\ncntr={cn} seed=({ra0:.9f},{dec0:.9f}) k5={k5}")

        hits = 0
        for yr in years:
            path = leaf(yr, k5)
            try:
                ds = pds.dataset(path, format='parquet', filesystem=fs,
                                 partitioning='hive', exclude_invalid_files=True)
            except Exception:
                # Missing leaf is expected for some years
                continue
            tbl = ds.to_table(filter=bbox(ra0, dec0, a.radius) & gates,
                              columns=['cntr','ra','dec','mjd','w1snr','w2snr','moon_masked'])
            if tbl.num_rows == 0:
                continue
            # Post-read moon_masked normalization: keep 0-like values only
            mm = tbl['moon_masked']
            mm_str = pc.cast(mm, pa.utf8(), safe=False)
            keep = pc.equal(mm_str, pa.scalar("00"))
            keep = pc.or_(keep, pc.equal(mm_str, pa.scalar("0")))
            try:
                keep = pc.or_(keep, pc.equal(pc.cast(mm, pa.int64(), safe=False), pa.scalar(0, pa.int64())))
            except Exception:
                pass
            tbl = tbl.filter(keep)
            if tbl.num_rows == 0:
                continue
            df = tbl.to_pandas()
            present = (df['cntr'] == cn).any()
            if present:
                hits += 1
                print(f"  {yr}: present (rows={len(df)})")
        if hits == 0:
            print("  not present in any year/addendum leaf under gates+bbox")
        else:
            print(f"  present in {hits} year leaf(s)")

    # Also show which TAP-only cntrs appear in AWS closest (sanity)
    aws_cntrs = set(aws['cntr'].tolist()) if 'cntr' in aws.columns else set()
    print("\nPresent in AWS closest CSV:", sorted(list(aws_cntrs & set(cntrs))))

if __name__ == '__main__':
    main()