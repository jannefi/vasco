#!/usr/bin/env python3
# sidecar_to_closest_chunks.py
"""
Read:
  - sidecar ALL parquet (neowise_se_flags_ALL.parquet)
  - optical delta dataset root (with source_id,opt_ra_deg,opt_dec_deg,chunk_id)
Write:
  - per-chunk positions<chunk_id>_closest.csv
  - small positions<chunk_id>_closest.qc.txt (counts)
Notes:
  - Keeps row_id as string to avoid precision loss (1e19). If you must write numeric,
    add --row-id-float to format as %.16g (risk: precision loss past 2^53).
"""
import argparse, os, pandas as pd, pyarrow.dataset as pds, pyarrow.parquet as pq

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sidecar-all", required=True,
                    help="Path to neowise_se_flags_ALL.parquet")
    ap.add_argument("--optical-root", required=True,
                    help="Optical delta root (s3:// or local path)")
    ap.add_argument("--out-dir", required=True,
                    help="Where to write positionsNNNNN_closest.csv")
    ap.add_argument("--row-id-float", action="store_true",
                    help="Emit row_id as float string (%.16g). Default: keep as string.")
    args = ap.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    # Load sidecar ALL
    neo = pd.read_parquet(args.sidecar_all)

    # Load optical delta (only columns we need)
    ds = pds.dataset(args.optical_root, format="parquet")
    opt = ds.to_table(columns=["source_id","opt_ra_deg","opt_dec_deg","chunk_id"]).to_pandas()

    # Merge to add chunk_id and input coords
    neo = neo.merge(opt, left_on="opt_source_id", right_on="source_id", how="left", validate="m:1")

    # Prepare closest-like frame
    # Sidecar provides: opt_source_id,opt_ra_deg,opt_dec_deg, cntr, ra, dec, mjd, (extra cols via --columns), sep_arcsec
    # TAP header you showed:
    # row_id,in_ra,in_dec,cntr,ra,dec,mjd,w1snr,w2snr,qual_frame,qi_fact,saa_sep,moon_masked,sep_arcsec
    def to_row_id(v):
        if pd.isna(v): return ""
        if not args.row_id_float:
            return str(v)
        try:
            return f"{float(v):.16g}"
        except Exception:
            return str(v)

    cols_out = ["row_id","in_ra","in_dec","cntr","ra","dec","mjd",
                "w1snr","w2snr","qual_frame","qi_fact","saa_sep","moon_masked","sep_arcsec","chunk_id"]
    out = pd.DataFrame({
        "row_id":     neo["opt_source_id"].apply(to_row_id),
        "in_ra":      neo["opt_ra_deg"],
        "in_dec":     neo["opt_dec_deg"],
        "cntr":       neo.get("cntr"),
        "ra":         neo.get("ra"),
        "dec":        neo.get("dec"),
        "mjd":        neo.get("mjd"),
        "w1snr":      neo.get("w1snr"),
        "w2snr":      neo.get("w2snr"),
        "qual_frame": neo.get("qual_frame"),
        "qi_fact":    neo.get("qi_fact"),
        "saa_sep":    neo.get("saa_sep"),
        "moon_masked":neo.get("moon_masked"),
        "sep_arcsec": neo.get("sep_arcsec"),
        "chunk_id":   neo.get("chunk_id")
    })[cols_out]

    # Write per-chunk outputs
    for cid, sub in out.groupby("chunk_id"):
        if pd.isna(cid): cid = "unknown"
        out_csv = os.path.join(args.out_dir, f"positions{cid}_closest.csv")
        qc_txt  = os.path.join(args.out_dir, f"positions{cid}_closest.qc.txt")
        sub.drop(columns=["chunk_id"]).to_csv(out_csv, index=False, float_format="%.10g")
        with open(qc_txt, "w") as f:
            f.write(f"rows={len(sub)}\n")
            f.write("columns=" + ",".join(sub.drop(columns=['chunk_id']).columns) + "\n")
        print(f"[WRITE] {out_csv}  (rows={len(sub)})")

if __name__ == "__main__":
    main()