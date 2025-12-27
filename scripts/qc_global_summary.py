
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Global QC summary for NEOWISE-SE flags after sidecar write.
Reads neowise_se_flags_ALL.parquet and prints:
- totals, match rate (IR_PRESENT_SE), sep stats
- SNR breakdown (W1SNR/W2SNR)
- quality flag distributions (QUAL_FRAME/QI_FACT/SAA_SEP/MOON_MASKED)

Optionally writes a CSV with per-flag counts for dashboard use.
"""

import sys
import pandas as pd

def main():
    if len(sys.argv) < 2:
        print("Usage: qc_global_summary.py <flags_all.parquet> [out_summary_csv]")
        sys.exit(1)
    flags_path = sys.argv[1]
    out_csv = sys.argv[2] if len(sys.argv) == 3 else None

    df = pd.read_parquet(flags_path)
    total = len(df)
    present = df[df['IR_PRESENT_SE'] == 1]
    n_present = len(present)

    # sep stats
    sep = present['IR_SEP_ARCSEC'].dropna()
    med = sep.median() if len(sep) else float('nan')
    p95 = sep.quantile(0.95) if len(sep) else float('nan')

    # SNR breakdown
    w1_ok = len(df[df['W1SNR'].fillna(-1) >= 5])
    w2_ok = len(df[df['W2SNR'].fillna(-1) >= 5])
    any_ok = len(df[(df['W1SNR'].fillna(-1) >= 5) | (df['W2SNR'].fillna(-1) >= 5)])

    # Quality flags
    qf_ok  = len(df[df['QUAL_FRAME'].fillna(0) > 0])
    qif_ok = len(df[df['QI_FACT'].fillna(0)   > 0])
    saa_ok = len(df[df['SAA_SEP'].fillna(0)   > 0])
    moon_ok= len(df[df['MOON_MASKED'].astype(str).fillna('') == '00'])

    print(f"[GLOBAL] rows={total} present={n_present} match_rate={n_present/total:.3f}")
    print(f"[GLOBAL] sep_arcsec: median={med:.3f} p95={p95:.3f}")
    print(f"[GLOBAL] SNR: W1>=5={w1_ok} W2>=5={w2_ok} any>=5={any_ok}")
    print(f"[GLOBAL] flags: QUAL_FRAME>0={qf_ok} QI_FACT>0={qif_ok} SAA_SEP>0={saa_ok} MOON='00'={moon_ok}")

    if out_csv:
        summary = pd.DataFrame({
            'metric': ['rows','present','match_rate','med_sep_arcsec','p95_sep_arcsec',
                       'w1_snr_ge_5','w2_snr_ge_5','any_snr_ge_5',
                       'qual_frame_gt0','qi_fact_gt0','saa_sep_gt0','moon_mask_00'],
            'value':  [total, n_present, n_present/total, med, p95,
                       w1_ok, w2_ok, any_ok,
                       qf_ok, qif_ok, saa_ok, moon_ok]
        })
        summary.to_csv(out_csv, index=False)
        print(f"[GLOBAL] summary CSV -> {out_csv}")

if __name__ == "__main__":
    main()

