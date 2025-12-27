
#!/usr/bin/env python3
# 1) Concatenate all *_closest.csv into a global flags Parquet mapping by row_id
# 2) Mirror the master Parquet tree into a sidecar tree with flags merged by row_id
#
# Usage:
#   python concat_flags_and_write_sidecar.py \
#       --closest-dir ./data/local-cats/tmp/positions \
#       --master-root ./data/local-cats/_master_optical_parquet \
#       --out-root ./data/local-cats/_master_optical_parquet_irflags \
#       --radius-arcsec 5.0
#
# Notes:
# - Reads master files one by one (row_id only), joins with global flags, writes a sidecar Parquet
# - Leaves master Parquet untouched

import argparse, os
from pathlib import Path
import pandas as pd

def load_all_closest(closest_dir: Path) -> pd.DataFrame:
    files = sorted(closest_dir.glob("*_closest.csv"))
    if not files:
        raise SystemExit(f"No *_closest.csv found under {closest_dir}")
    frames = []
    for f in files:
        try:
            df = pd.read_csv(f, dtype={'row_id':'string'})
            frames.append(df)
        except Exception as e:
            print(f"[WARN] Skipping {f}: {e}")
    if not frames:
        raise SystemExit("No closest CSVs could be read.")
    cat = pd.concat(frames, ignore_index=True)
    # De-dup in case of re-runs: keep smallest sep_arcsec
    if 'sep_arcsec' in cat.columns:
        cat.sort_values(['row_id','sep_arcsec'], ascending=[True, True], inplace=True)
    cat = cat.drop_duplicates(subset=['row_id'], keep='first')
    return cat

def build_flags(cat: pd.DataFrame, radius_arcsec: float) -> pd.DataFrame:
    # minimal flags mapping
    # Ensure required columns exist
    for col in ['sep_arcsec','cntr','mjd','w1snr','w2snr','qual_frame','qi_fact','saa_sep','moon_masked']:
        if col not in cat.columns:
            cat[col] = None

    flags = pd.DataFrame({
        'row_id':     cat['row_id'].astype('string'),
        'IR_SOURCE_ID': cat['cntr'],
        'IR_MJD':       cat['mjd'],
        'IR_SEP_ARCSEC': cat['sep_arcsec'],
        'IR_PRESENT_SE': (cat['sep_arcsec'] <= float(radius_arcsec)).astype('Int8'),
        'W1SNR':      cat['w1snr'],
        'W2SNR':      cat['w2snr'],
        'QUAL_FRAME': cat['qual_frame'],
        'QI_FACT':    cat['qi_fact'],
        'SAA_SEP':    cat['saa_sep'],
        'MOON_MASKED':cat['moon_masked']
    })
    return flags

def write_sidecar_tree(master_root: Path, out_root: Path, flags: pd.DataFrame):
    out_root.mkdir(parents=True, exist_ok=True)
    # Speed: index flags by row_id for quick joins
    flags_idx = flags.set_index('row_id')
    # Walk master tree and write mirrored sidecars
    for p in master_root.rglob("*.parquet"):
        rel = p.relative_to(master_root)
        outp = out_root / rel
        outp.parent.mkdir(parents=True, exist_ok=True)
        try:
            # Read only row_id to keep memory lower
            df_ids = pd.read_parquet(p, columns=['row_id'])
        except Exception as e:
            print(f"[WARN] Skipping {p}: {e}")
            continue
        # Ensure row_id string for join
        df_ids['row_id'] = df_ids['row_id'].astype('string')
        merged = df_ids.join(flags_idx, on='row_id')
        # Drop rows with no flags if you prefer a strictly sparse sidecar:
        # merged = merged[merged['IR_PRESENT_SE'].notna()]
        merged.to_parquet(outp, index=False)
        print(f"[OK] Sidecar -> {outp}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--closest-dir", required=True, help="Directory with *_closest.csv files")
    ap.add_argument("--master-root", required=True, help="Root of the master Parquet dataset")
    ap.add_argument("--out-root", required=True, help="Output root for sidecar Parquet tree")
    ap.add_argument("--radius-arcsec", type=float, default=5.0)
    args = ap.parse_args()

    closest_dir = Path(args.closest_dir)
    master_root = Path(args.master_root)
    out_root    = Path(args.out_root)

    print("[1/3] Loading chunk closest CSVs …")
    cat = load_all_closest(closest_dir)
    print(f"   Loaded {len(cat)} rows from {closest_dir}")

    print("[2/3] Building global flags mapping …")
    flags = build_flags(cat, args.radius_arcsec)

    # Write a single reference Parquet too (optional but handy for QA)
    ref_all = out_root / "neowise_se_flags_ALL.parquet"
    flags.to_parquet(ref_all, index=False)
    print(f"   Wrote global flags Parquet: {ref_all} (rows={len(flags)})")

    print("[3/3] Writing mirror sidecar tree …")
    write_sidecar_tree(master_root, out_root, flags)
    print("[DONE]")

if __name__ == "__main__":
    main()

