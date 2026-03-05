
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Smoketest: Anonymous S3 access to NEOWISE-R Single-exposure Source Table (Parquet).
- Uses S3 URIs (s3://...) with trailing slash for dataset roots (PyArrow 22 requirement)
- Loads dataset metadata for one or more years
- Reads a small slice via healpix_k5 filter or first fragment (PyArrow 22-friendly)
"""

import argparse
import sys
import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.compute as pc


def build_neowise_dataset(years):
    """
    Create a UnionDataset across years with hive partitioning.
    Use S3 URIs so PyArrow resolves the S3 filesystem automatically.
    """
    root_uris = []
    for yr in years:
        # IMPORTANT: trailing slash because the dataset root is a directory
        root = f"s3://nasa-irsa-wise/wise/neowiser/catalogs/p1bs_psd/healpix_k5/{yr}/neowiser-healpix_k5-{yr}.parquet/"
        root_uris.append(root)

    per_year_ds = []
    for uri in root_uris:
        # Build per-year dataset; if the path is wrong, raise a clear error
        try:
            ds_y = pds.dataset(uri, format="parquet", partitioning="hive")
        except Exception as e:
            raise RuntimeError(f"Failed to open dataset at {uri}\n{e}") from e
        per_year_ds.append(ds_y)

    # UnionDataset over years
    return pds.dataset(per_year_ds)


def read_small_slice(ds, k5=None, rows=100000, columns=None):
    """
    Read a small table slice; if k5 is None, use first fragment (PyArrow 22-compatible).
    """
    if columns is None:
        columns = ["cntr", "source_id", "ra", "dec", "mjd", "w1flux", "w2flux"]

    if k5 is None:
        # Use the first fragment directly (Fragment.from_path is not exposed in v22)
        frags = ds.get_fragments()
        try:
            frag = next(frags)
        except StopIteration:
            raise RuntimeError("Dataset has no fragments; cannot read.")
        tbl = frag.to_table(columns=columns)
    else:
        kfield = pc.field("healpix_k5")
        tbl = ds.to_table(filter=(kfield == pa.scalar(int(k5))), columns=columns)

    if tbl.num_rows > rows:
        tbl = tbl.slice(0, rows)
    return tbl


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", type=str, default="year8",
                    help="Comma-separated NEOWISE years (e.g., 'year8,year9'). Default: year8")
    ap.add_argument("--k5", type=int, default=None,
                    help="Optional HEALPix order-5 pixel index to filter on.")
    ap.add_argument("--rows", type=int, default=100000,
                    help="Max rows for the smoketest. Default: 100000")
    args = ap.parse_args()

    years = [s.strip() for s in args.years.split(",") if s.strip()]
    print(f"[INFO] PyArrow version: {pa.__version__}")
    print(f"[INFO] Building NEOWISE dataset for years: {years}")

    ds = build_neowise_dataset(years)

    # Basic metadata (fast to compute)
    total_files = sum(len(child.files) for child in ds.children)
    print(f"[INFO] Dataset contains ~{total_files} files across {len(ds.children)} year(s).")

    if args.k5 is None:
        print("[INFO] No k5 provided; reading first fragment.")
    else:
        print(f"[INFO] Reading filter: healpix_k5 == {args.k5}")

    tbl = read_small_slice(ds, k5=args.k5, rows=args.rows)

    print(f"[OK] Loaded table: {tbl.num_rows:,} rows x {tbl.num_columns} columns")

    # Lightweight stats
    for c in ("ra", "dec", "mjd"):
        if c in tbl.column_names:
            try:
                cmin = pa.compute.min(tbl[c]).as_py()
                cmax = pa.compute.max(tbl[c]).as_py()
                print(f"[STAT] {c}: min={cmin:.6f} max={cmax:.6f}")
            except Exception:
                pass

    print("[PREVIEW] Head(5):")
    print(tbl.to_pandas().head(5).to_string(index=False))

    print("[SUCCESS] S3 anonymous access + Parquet read smoketest passed.")
    sys.exit(0)


if __name__ == "__main__":
    main()
