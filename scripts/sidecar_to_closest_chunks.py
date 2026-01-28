#!/usr/bin/env python3
import argparse, os
import pandas as pd
import pyarrow.dataset as pds
from pyarrow import fs as pafs

def _mk_s3fs():
    return pafs.S3FileSystem(anonymous=False, region="us-west-2")

def _read_optical_chunkmap(opt_root: str) -> pd.DataFrame:
    if opt_root.startswith("s3://"):
        ds = pds.dataset(opt_root.replace("s3://", "", 1), format="parquet", filesystem=_mk_s3fs())
    else:
        ds = pds.dataset(opt_root, format="parquet")
    names = set(ds.schema.names)
    want = ["source_id", "chunk_id"]
    if "row_id" in names: want.append("row_id")
    tbl = ds.to_table(columns=want)
    df = tbl.to_pandas()
    if "chunk_id" not in df.columns: df["chunk_id"] = pd.NA
    return df[[c for c in ["source_id","row_id","chunk_id"] if c in df.columns]]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sidecar-all", required=True)
    ap.add_argument("--optical-root", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--row-id-float", action="store_true")
    a = ap.parse_args()

    os.makedirs(a.out_dir, exist_ok=True)

    neo = pd.read_parquet(a.sidecar_all)
    opt = _read_optical_chunkmap(a.optical_root)
    neo = neo.merge(opt, left_on="opt_source_id", right_on="source_id", how="left", validate="m:1", suffixes=("", "__opt"))

    def to_row_id(v_src, v_opt):
        v = v_opt if pd.notna(v_opt) else v_src
        if pd.isna(v): return ""
        if a.row_id_float:
            try: return f"{float(v):.16g}"
            except Exception: return str(v)
        return str(v)

    out = pd.DataFrame({
        "row_id":      [to_row_id(s, r) for s, r in zip(neo.get("opt_source_id"), neo.get("row_id"))],
        "in_ra":       neo.get("opt_ra_deg"),
        "in_dec":      neo.get("opt_dec_deg"),
        "cntr":        neo.get("cntr"),
        "ra":          neo.get("ra"),
        "dec":         neo.get("dec"),
        "mjd":         neo.get("mjd"),
        "w1snr":       neo.get("w1snr"),
        "w2snr":       neo.get("w2snr"),
        "qual_frame":  neo.get("qual_frame"),
        "qi_fact":     neo.get("qi_fact"),
        "saa_sep":     neo.get("saa_sep"),
        "moon_masked": neo.get("moon_masked"),
        "sep_arcsec":  neo.get("sep_arcsec"),
        "chunk_id":    neo.get("chunk_id"),
    })[["row_id","in_ra","in_dec","cntr","ra","dec","mjd","w1snr","w2snr","qual_frame","qi_fact","saa_sep","moon_masked","sep_arcsec","chunk_id"]]

    for cid, sub in out.groupby("chunk_id", dropna=False):
        cid_str = "unknown" if pd.isna(cid) else str(cid)
        out_csv = os.path.join(a.out_dir, f"positions{cid_str}_closest.csv")
        qc_txt  = os.path.join(a.out_dir, f"positions{cid_str}_closest.qc.txt")
        sub.drop(columns=["chunk_id"]).to_csv(out_csv, index=False, float_format="%.10g")
        with open(qc_txt, "w") as f:
            f.write(f"rows={len(sub)}")
            f.write("columns=" + ",".join(sub.drop(columns=['chunk_id']).columns) + "")
        print(f"[WRITE] {out_csv} (rows={len(sub)})")

if __name__ == "__main__":
    main()
