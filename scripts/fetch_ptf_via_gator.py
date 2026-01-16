
#!/usr/bin/env python3
"""
PTF match via IRSA Gator Upload (fallback when TAP is slow).
Input: positions CSV with NUMBER,ra,dec (ICRS, degrees).
Output: flags_ptf_objects.parquet with has_other_archive_match=True for matched NUMBERs.

Usage:
  python scripts/fetch_ptf_via_gator.py \
    --positions-csv ./work/positions_upload.csv \
    --out ./data/local-cats/_master_optical_parquet_flags/flags_ptf_objects.parquet \
    --catalog ptf_objects \
    --radius-arcsec 5
"""
import argparse, io, sys, time
import requests
import pandas as pd
import pyarrow as pa, pyarrow.parquet as pq

GATOR_URL = "https://irsa.ipac.caltech.edu/cgi-bin/Gator/nph-query"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--positions-csv", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--catalog", default="ptf_objects",
                    choices=["ptf_objects","ptf_sources","ptf_lightcurves","ptfphotcalcat"])
    ap.add_argument("--radius-arcsec", type=float, default=5.0)
    ap.add_argument("--timeout", type=float, default=90.0)
    args = ap.parse_args()

    # Load upload; IRSA expects ra,dec in degrees, NUMBER as an arbitrary ID col
    df = pd.read_csv(args.positions_csv)
    if not {"NUMBER","ra","dec"}.issubset(df.columns):
        raise SystemExit("positions CSV must have NUMBER,ra,dec columns")
    df["NUMBER"] = df["NUMBER"].astype(str)

    # Gator upload: spatial=Upload, radius in arcsec, outfmt=1 (CSV)
    files = {"uploadfile": ("positions.csv", df.to_csv(index=False), "text/csv")}
    data = {
        "outfmt": "1",
        "spatial": "Upload",
        "radius": f"{args.radius_arcsec}",
        "catalog": args.catalog,
        "objstr": "",                # not needed with upload
        "selcols": "ra,dec",         # keep it small
    }
    # NOTE: Gator sees upload columns by header; it will parse ra,dec automatically.

    try:
        r = requests.post(GATOR_URL, data=data, files=files, timeout=args.timeout)
        r.raise_for_status()
    except Exception as e:
        raise SystemExit(f"Gator request failed: {e}")

    # The response is CSV; filter out comments and read matches
    text = "\n".join([ln for ln in r.text.splitlines() if not ln.strip().startswith("#")])
    if not text.strip():
        # no matches at all
        out = pd.DataFrame(columns=["NUMBER","has_other_archive_match"])
        pq.write_table(pa.Table.from_pandas(out, preserve_index=False), args.out)
        print("[OK] PTF (Gator) flags ->", args.out, "rows=0")
        return

    try:
        res = pd.read_csv(io.StringIO(text))
    except Exception as e:
        # Dump a snippet to help diagnose, but still fail cleanly
        print("[ERROR] Failed to parse Gator CSV. First 20 lines:\n", "\n".join(text.splitlines()[:20]))
        raise

    # Expect Gator to echo or include join info. Safest: rejoin by nearest within radius
    # For existence flagging, it's enough to map back by the upload row order if echoed.
    # Many Gator outputs include "rowNum" or "cntr". Here, just inner-join by ra/dec round.
    res["ra"] = res["ra"].round(7); res["dec"] = res["dec"].round(7)
    src = df.copy(); src["ra"] = src["ra"].round(7); src["dec"] = src["dec"].round(7)
    matched = src.merge(res[["ra","dec"]].drop_duplicates(), on=["ra","dec"], how="inner")
    matched = matched[["NUMBER"]].drop_duplicates().assign(has_other_archive_match=True)

    pq.write_table(pa.Table.from_pandas(matched, preserve_index=False), args.out)
    print("[OK] PTF (Gator) flags ->", args.out, "rows=", len(matched))

if __name__ == "__main__":
    main()

