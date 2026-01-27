#!/usr/bin/env python3
# sidecar_to_closest_chunks.py
"""
Read:
 - sidecar ALL parquet (neowise_se_flags_ALL.parquet)
 - optical delta dataset root (with at least source_id,chunk_id)

Write:
 - per-chunk positions<chunk_id>_closest.csv
 - small positions<chunk_id>_closest.qc.txt (counts)

Notes:
 - The sidecar already carries the optical coordinates it matched:
   opt_source_id, opt_ra_deg, opt_dec_deg, sep_arcsec, ...
 - To avoid Pandas merge collisions ("_x","_y"), we only bring `chunk_id`
   from the optical delta and join on source_id.
"""

import argparse
import os
import pandas as pd
import pyarrow.dataset as pds
import pyarrow.parquet as pq
from pyarrow import fs as pafs


def _mk_s3fs():
    # Uses the instance role on EC2
    return pafs.S3FileSystem(anonymous=False, region="us-west-2")


def _read_optical_chunkmap(opt_root: str) -> pd.DataFrame:
    """
    Read only (source_id, chunk_id) from the optical delta, from local or s3://.
    Creates chunk_id=NA if missing.
    """
    if opt_root.startswith("s3://"):
        ds = pds.dataset(opt_root.replace("s3://", "", 1),
                         format="parquet", filesystem=_mk_s3fs())
    else:
        ds = pds.dataset(opt_root, format="parquet")

    names = set(ds.schema.names)
    want = ["source_id"]
    if "chunk_id" in names:
        want.append("chunk_id")
    tbl = ds.to_table(columns=want)
    df = tbl.to_pandas()
    if "chunk_id" not in df.columns:
        df["chunk_id"] = pd.NA
    return df[["source_id", "chunk_id"]]


def _ensure_coord_columns(neo_df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure opt_ra_deg / opt_dec_deg exist after merge.
    If they got suffixed (_x/_y), normalize back to the expected names.
    """
    if "opt_ra_deg" not in neo_df.columns:
        for cand in ("opt_ra_deg_x", "opt_ra_deg_y", "opt_ra_deg__opt"):
            if cand in neo_df.columns:
                neo_df = neo_df.rename(columns={cand: "opt_ra_deg"})
                break
    if "opt_dec_deg" not in neo_df.columns:
        for cand in ("opt_dec_deg_x", "opt_dec_deg_y", "opt_dec_deg__opt"):
            if cand in neo_df.columns:
                neo_df = neo_df.rename(columns={cand: "opt_dec_deg"})
                break
    missing = [c for c in ("opt_ra_deg", "opt_dec_deg") if c not in neo_df.columns]
    if missing:
        raise KeyError(f"Missing expected coordinate columns after merge: {missing}. "
                       f"Sidecar should include opt_ra_deg/opt_dec_deg.")
    return neo_df


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

    # Load sidecar ALL (contains opt_* coords + NEOWISE hit columns)
    neo = pd.read_parquet(args.sidecar_all)

    # Load optical delta (only chunk_id mapping)
    opt = _read_optical_chunkmap(args.optical_root)

    # Merge to add chunk_id to sidecar rows
    neo = neo.merge(
        opt, left_on="opt_source_id", right_on="source_id",
        how="left", validate="m:1", suffixes=("", "__opt")
    )

    # We don't need the extra copy of source_id from the optical table
    if "source_id" in neo.columns:
        neo.drop(columns=["source_id"], inplace=True)

    # Make sure the optical coords exist after merge (normalize any suffixing)
    neo = _ensure_coord_columns(neo)

    # Prepare closest-like frame
    # TAP-ish header you’ve used before:
    # row_id,in_ra,in_dec,cntr,ra,dec,mjd,w1snr,w2snr,qual_frame,qi_fact,saa_sep,moon_masked,sep_arcsec
    def to_row_id(v):
        if pd.isna(v):
            return ""
        if not args.row_id_float:
            return str(v)
        try:
            return f"{float(v):.16g}"
        except Exception:
            return str(v)

    cols_out = [
        "row_id", "in_ra", "in_dec", "cntr", "ra", "dec", "mjd",
        "w1snr", "w2snr", "qual_frame", "qi_fact", "saa_sep", "moon_masked",
        "sep_arcsec", "chunk_id"
    ]

    out = pd.DataFrame({
        "row_id":     neo["opt_source_id"].apply(to_row_id),
        "in_ra":      neo["opt_ra_deg"],     # from sidecar (normalized)
        "in_dec":     neo["opt_dec_deg"],    # from sidecar (normalized)
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
        "chunk_id":   neo.get("chunk_id"),
    })[cols_out]

    # Write per-chunk outputs
    for cid, sub in out.groupby("chunk_id", dropna=False):
        cid_str = "unknown" if pd.isna(cid) else str(cid)
        out_csv = os.path.join(args.out_dir, f"positions{cid_str}_closest.csv")
        qc_txt  = os.path.join(args.out_dir, f"positions{cid_str}_closest.qc.txt")
        sub.drop(columns=["chunk_id"]).to_csv(out_csv, index=False, float_format="%.10g")
        with open(qc_txt, "w") as f:
            f.write(f"rows={len(sub)}\n")
            f.write("columns=" + ",".join(sub.drop(columns=['chunk_id']).columns) + "\n")
        print(f"[WRITE] {out_csv} (rows={len(sub)})")


if __name__ == "__main__":
    main()
