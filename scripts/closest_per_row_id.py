
#!/usr/bin/env python3
# Keep the closest NEOWISE-SE row per row_id and add sep_arcsec (if needed).
# Input CSV must include at least: row_id, ra, dec; optionally in_ra, in_dec, sep_deg
import sys, math
import pandas as pd

def sep_arcsec(ra1, dec1, ra2, dec2):
    d2r = math.pi/180.0
    ra1, dec1, ra2, dec2 = ra1*d2r, dec1*d2r, ra2*d2r, dec2*d2r
    s = math.acos(max(-1.0, min(1.0,
        math.sin(dec1)*math.sin(dec2) + math.cos(dec1)*math.cos(dec2)*math.cos(ra1-ra2)
    )))
    return s*(180.0/math.pi)*3600.0

if len(sys.argv) != 3:
    print("Usage: closest_per_row_id.py <in_raw.csv> <out_closest.csv>")
    sys.exit(1)

in_csv, out_csv = sys.argv[1], sys.argv[2]
df = pd.read_csv(in_csv, dtype={'row_id':'string'})

# Ensure we have a separation to sort on
if 'sep_deg' in df.columns:
    df['sep_arcsec'] = df['sep_deg']*3600.0
else:
    # Need in_ra/in_dec to compute; if absent, assume matches are valid and set huge sep
    if {'in_ra','in_dec'}.issubset(df.columns):
        df['sep_arcsec'] = [
            sep_arcsec(ra1, dec1, ra2, dec2)
            for ra1,dec1,ra2,dec2 in zip(df['in_ra'], df['in_dec'], df['ra'], df['dec'])
        ]
    else:
        df['sep_arcsec'] = 1e9  # fallback; still allows grouping but will not rank by proximity

# Sort & keep first per row_id
sort_cols = ['row_id','sep_arcsec']
df.sort_values(sort_cols, ascending=[True, True], inplace=True)
closest = df.groupby('row_id', as_index=False).first()

closest.to_csv(out_csv, index=False)
print(f"Wrote closest-per-row: {out_csv} (rows={len(closest)})")

