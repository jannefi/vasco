
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
QC summary for a NEOWISE-SE chunk closest CSV.

Inputs:
  - CSV columns: row_id (string), ra, dec, (optional) in_ra, in_dec, sep_arcsec or sep_deg,
                 cntr, mjd, w1snr, w2snr, qual_frame, qi_fact, saa_sep, moon_masked

Outputs (stdout):
  - Totals, matches within 5", median/95% sep, SNR band counts, quality flag distributions.
"""

import sys, math
import pandas as pd

def sep_arcsec(ra1, dec1, ra2, dec2):
    d2r = math.pi/180.0
    ra1, dec1, ra2, dec2 = ra1*d2r, dec1*d2r, ra2*d2r, dec2*d2r
    s = math.acos(max(-1.0, min(1.0,
        math.sin(dec1)*math.sin(dec2) + math.cos(dec1)*math.cos(dec2)*math.cos(ra1-ra2)
    )))
    return s*(180.0/math.pi)*3600.0

def main():
    if len(sys.argv) != 2:
        print("Usage: qc_chunk_summary.py <closest_csv>")
        sys.exit(1)
    infile = sys.argv[1]
    df = pd.read_csv(infile, dtype={'row_id': 'string'})

    # Ensure sep_arcsec present
    if 'sep_arcsec' not in df.columns:
        if 'sep_deg' in df.columns:
            df['sep_arcsec'] = df['sep_deg'] * 3600.0
        elif {'in_ra','in_dec'}.issubset(df.columns):
            df['sep_arcsec'] = [
                sep_arcsec(ra1, dec1, ra2, dec2)
                for ra1, dec1, ra2, dec2 in zip(df['in_ra'], df['in_dec'], df['ra'], df['dec'])
            ]
        else:
            df['sep_arcsec'] = float('nan')

    total = len(df)
    matches = df[df['sep_arcsec'] <= 5.0]
    mcount = len(matches)

    # Robust quantiles with dropna
    med_sep = (matches['sep_arcsec'].dropna()).median() if mcount else float('nan')
    p95_sep = (matches['sep_arcsec'].dropna()).quantile(0.95) if mcount else float('nan')

    # SNR bands
    w1_ok = len(df[df.get('w1snr', 0) >= 5])
    w2_ok = len(df[df.get('w2snr', 0) >= 5])
    any_snr_ok = len(df[(df.get('w1snr', 0) >= 5) | (df.get('w2snr', 0) >= 5)])

    # Quality flags
    qf_ok = len(df[df.get('qual_frame', 0) > 0])
    qif_ok = len(df[df.get('qi_fact', 0) > 0])
    saa_ok = len(df[df.get('saa_sep', 0) > 0])
    moon_ok = len(df[df.get('moon_masked', '').astype(str) == '00'])

    print(f"[QC] file={infile}")
    print(f"[QC] total_rows={total} matches_<=5arcsec={mcount} match_rate={mcount/total:.3f}")
    print(f"[QC] sep_arcsec median={med_sep:.3f} p95={p95_sep:.3f}")
    print(f"[QC] SNR: W1>=5={w1_ok} W2>=5={w2_ok} any>=5={any_snr_ok}")
    print(f"[QC] flags: qual_frame>0={qf_ok} qi_fact>0={qif_ok} saa_sep>0={saa_ok} moon_masked='00'={moon_ok}")

if __name__ == "__main__":
    main()

