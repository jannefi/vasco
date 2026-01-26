
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NEOWISE sidecar via AWS S3 Parquet (anonymous access), with RESUME + PARALLEL.

What this script does
---------------------
• Opens the NEOWISE-R Single Exposure Source Table (Parquet, HEALPix k=5) from IRSA's public S3 (no creds).
• Reads your optical master Parquet (VASCO) and computes healpix_k5 for each source.
• For each k5 bin, finds the closest NEOWISE detection within --radius-arcsec.
• Writes ONE shard per k5 bin under:
      ./data/local-cats/_master_optical_parquet_irflags/tmp/k5=<int>.parquet
  → If a shard already exists, the bin is skipped (RESUME friendly).
• (By default) Finalizes by concatenating all shards into:
      ./data/local-cats/_master_optical_parquet_irflags/neowise_se_flags_ALL.parquet

Key flags
---------
--parallel {none,pixel}    : 'pixel' = parallelize per k5 bin (ThreadPool). Default 'none'.
--workers N                : worker threads for 'pixel' mode. Default: min(8, CPU count).
--columns "<csv list>"     : which NEOWISE columns to read. Default minimal set.
--no-finalize              : do NOT create the final ALL.parquet (leave shards only).
--force                    : recompute shards even if they already exist.
--k5-limit N               : process only first N k5 bins (quick subset).
--k5-include "<list|file>" : restrict run to listed k5 bins (comma list or file with one per line).
"""

import os
import sys
import math
import glob
import argparse
from typing import List, Optional, Tuple, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as pds
import pyarrow.parquet as pq


# -------------------------------
# Configuration (paths & columns)
# -------------------------------
S3_BUCKET = "nasa-irsa-wise"
S3_PREFIX = "wise/neowiser/catalogs/p1bs_psd/healpix_k5"

DEFAULT_OPTICAL_PARQUET_ROOT = "./data/local-cats/_master_optical_parquet"
DEFAULT_OUT_FLAGS_ROOT = "./data/local-cats/_master_optical_parquet_irflags"
DEFAULT_TMP_DIR = os.path.join(DEFAULT_OUT_FLAGS_ROOT, "tmp")
DEFAULT_OUT_FILE = "neowise_se_flags_ALL.parquet"

# Minimal NEOWISE columns needed for association + flags; overridable via --columns
DEFAULT_NEO_COLS = [
    "cntr", "source_id", "ra", "dec", "mjd",
    "w1flux", "w1sigflux", "w2flux", "w2sigflux",
]


# -------------------------------
# HEALPix indexing helpers (k=5)
# -------------------------------
def k5_index_ra_dec(ra_deg_array: np.ndarray, dec_deg_array: np.ndarray) -> np.ndarray:
    nside = 2 ** 5

    # 1) healpy (theta=colat in rad, phi=lon in rad)
    try:
        import healpy as hp
        theta = np.deg2rad(90.0 - np.asarray(dec_deg_array, dtype=float))
        phi = np.deg2rad(np.asarray(ra_deg_array, dtype=float))
        return hp.ang2pix(nside, theta, phi, nest=True)
    except Exception:
        pass

    # 2) astropy-healpix
    try:
        from astropy_healpix import HEALPix
        HPX = HEALPix(nside=nside, order='nested')
        return HPX.lonlat_to_healpix(np.deg2rad(ra_deg_array), np.deg2rad(dec_deg_array))
    except Exception:
        pass

    # 3) hpgeom (theta/phi route)
    try:
        import hpgeom as hpg
        try:
            theta, phi = hpg.lonlat_to_thetaphi(ra_deg_array, dec_deg_array, degrees=True)
        except TypeError:
            theta, phi = hpg.lonlat_to_thetaphi(np.deg2rad(ra_deg_array), np.deg2rad(dec_deg_array))
        if hasattr(hpg, "thetaphi_to_healpix"):
            return hpg.thetaphi_to_healpix(theta, phi, order=5, nest=True)
        try:
            import healpy as hp
            return hp.ang2pix(nside, theta, phi, nest=True)
        except Exception:
            pass
    except Exception:
        pass

    raise RuntimeError(
        "HEALPix indexing failed (healpy/astropy-healpix/hpgeom not usable).\n"
        "Install 'healpy' or 'astropy-healpix'."
    )


# -------------------------------
# Geometry / matching utilities
# -------------------------------
def arcsec2rad(arcsec: float) -> float:
    return arcsec / 206264.806


def rad2arcsec(rad: np.ndarray) -> np.ndarray:
    return rad * 206264.806


def haversine_sep_arcsec(ra0_deg: float, dec0_deg: float,
                         ra_deg: np.ndarray, dec_deg: np.ndarray) -> np.ndarray:
    d2r = np.pi / 180.0
    dra = (ra_deg - ra0_deg) * d2r
    ddec = (dec_deg - dec0_deg) * d2r
    a = np.sin(ddec / 2.0) ** 2 + np.cos(dec0_deg * d2r) * np.cos(dec_deg * d2r) * np.sin(dra / 2.0) ** 2
    return rad2arcsec(2.0 * np.arcsin(np.sqrt(a)))


# -------------------------------
# NEOWISE dataset (S3, anonymous)
# -------------------------------
def build_neowise_dataset(years: List[str]) -> pds.Dataset:
    # URI form; Arrow infers S3 FS and treats the path as a dataset directory
    per_year = []
    for yr in years:
        uri = f"s3://{S3_BUCKET}/{S3_PREFIX}/{yr}/neowiser-healpix_k5-{yr}.parquet"
        fs = pa.fs.S3FileSystem(anonymous=True)
        ds = pds.dataset(uri, format="parquet", partitioning="hive",filesystem=fs)
        per_year.append(ds)
    return pds.dataset(per_year)


# -------------------------------
# Optical loader (robust to schema)
# -------------------------------
_RADEC_PAIRS = [
    ("opt_ra_deg", "opt_dec_deg"),
    ("ra_deg", "dec_deg"),
    ("ALPHA_J2000", "DELTA_J2000"),           # SExtractor (world coords)
    ("ALPHAWIN_J2000", "DELTAWIN_J2000"),     # SExtractor (windowed)
    ("X_WORLD", "Y_WORLD"),                   # SExtractor alt world coords
]

def _choose_radec(schema_names: List[str]) -> Optional[Tuple[str, str]]:
    s = set(schema_names)
    for ra_name, dec_name in _RADEC_PAIRS:
        if ra_name in s and dec_name in s:
            return ra_name, dec_name
    return None


def load_optical_positions(parquet_root: str) -> pd.DataFrame:
    ds = pds.dataset(parquet_root, format="parquet")
    names = ds.schema.names

    radec = _choose_radec(names)
    if not radec:
        raise RuntimeError(
            "Could not detect RA/Dec columns in optical dataset. "
            f"Tried: {_RADEC_PAIRS}. Available: {', '.join(names)}"
        )
    ra_name, dec_name = radec

    need = [ra_name, dec_name]
    # Optional bins if present
    if "ra_bin" in names: need.append("ra_bin")
    if "dec_bin" in names: need.append("dec_bin")

    have_source_id = ("source_id" in names)
    if have_source_id:
        need.append("source_id")
    else:
        # synthesize source_id from NUMBER + (tile_id | image_id)
        if "NUMBER" in names: need.append("NUMBER")
        if "tile_id" in names: need.append("tile_id")
        if "image_id" in names: need.append("image_id")

    tbl = ds.to_table(columns=[c for c in need if c in names])
    df = tbl.to_pandas()

    df = df.rename(columns={ra_name: "opt_ra_deg", dec_name: "opt_dec_deg"})

    if not have_source_id:
        if "NUMBER" in df.columns:
            if "tile_id" in df.columns:
                df["source_id"] = df["tile_id"].astype(str) + "#" + df["NUMBER"].astype(str)
            elif "image_id" in df.columns:
                df["source_id"] = df["image_id"].astype(str) + "#" + df["NUMBER"].astype(str)
            else:
                df["source_id"] = df["NUMBER"].astype(str)
        else:
            df["source_id"] = df.index.astype(str)

    df["healpix_k5"] = k5_index_ra_dec(df["opt_ra_deg"].values, df["opt_dec_deg"].values)

    keep = ["source_id", "opt_ra_deg", "opt_dec_deg", "ra_bin", "dec_bin", "healpix_k5"]
    keep = [c for c in keep if c in df.columns]
    return df[keep]


# -------------------------------
# Output schema + casting helpers
# -------------------------------
def result_schema() -> pa.schema:
    # Compact sidecar types
    return pa.schema([
        ("opt_source_id", pa.string()),
        ("opt_ra_deg",    pa.float64()),
        ("opt_dec_deg",   pa.float64()),
        ("source_id",     pa.string()),
        ("cntr",          pa.int64()),
        ("ra",            pa.float64()),
        ("dec",           pa.float64()),
        ("mjd",           pa.float64()),
        ("w1flux",        pa.float32()),
        ("w1sigflux",     pa.float32()),
        ("w2flux",        pa.float32()),
        ("w2sigflux",     pa.float32()),
        ("sep_arcsec",    pa.float32()),
        ("healpix_k5",    pa.int32()),
    ])

def cast_table_to_schema(tbl: pa.Table, schema: pa.Schema) -> pa.Table:
    arrays, names = [], []
    for field in schema:
        name = field.name
        if name in tbl.column_names:
            col = tbl[name]
            if not col.type.equals(field.type):
                col = pc.cast(col, field.type)
            arrays.append(col)
            names.append(name)
        else:
            arrays.append(pa.nulls(tbl.num_rows, type=field.type))
            names.append(name)
    return pa.Table.from_arrays(arrays, names=names)


# -------------------------------
# Match logic (single partition)
# -------------------------------
def match_partition_to_table(opt_part_df: pd.DataFrame,
                             neowise_ds: pds.Dataset,
                             k5_pixel: int,
                             arcsec_radius: float,
                             neo_cols: List[str]) -> pa.Table:
    if opt_part_df.empty:
        return pa.Table.from_arrays([pa.array([], type=pa.string())], names=["__empty__"]).drop_columns(["__empty__"])

    kfield = pc.field("healpix_k5")
    neo_tbl = neowise_ds.to_table(filter=(kfield == pa.scalar(k5_pixel)), columns=neo_cols)
    neo_df = neo_tbl.to_pandas()
    if neo_df.empty:
        return pa.Table.from_arrays([pa.array([], type=pa.string())], names=["__empty__"]).drop_columns(["__empty__"])

    # BBox prefilter
    delta_deg = math.degrees(arcsec2rad(arcsec_radius))
    results = []

    neo_ra = neo_df["ra"].values
    neo_dec = neo_df["dec"].values

    for _, row in opt_part_df.iterrows():
        ra0, dec0, opt_id = row["opt_ra_deg"], row["opt_dec_deg"], row["source_id"]
        m = (neo_ra >= ra0 - delta_deg) & (neo_ra <= ra0 + delta_deg) & \
            (neo_dec >= dec0 - delta_deg) & (neo_dec <= dec0 + delta_deg)
        if not m.any():
            continue

        sub = neo_df.loc[m, neo_cols]
        # Ensure columns exist
        sub = sub[["ra", "dec", "mjd", "source_id", "cntr",
                   "w1flux", "w1sigflux", "w2flux", "w2sigflux"]]

        d_arcsec = haversine_sep_arcsec(ra0, dec0, sub["ra"].values, sub["dec"].values)
        within = d_arcsec <= arcsec_radius
        if not within.any():
            continue

        j = int(np.argmin(d_arcsec))
        hit = sub.iloc[j].to_dict()
        hit["sep_arcsec"] = float(d_arcsec[j])
        hit["opt_source_id"] = opt_id
        hit["opt_ra_deg"] = float(ra0)
        hit["opt_dec_deg"] = float(dec0)
        hit["healpix_k5"] = int(k5_pixel)
        results.append(hit)

    if not results:
        return pa.Table.from_arrays([pa.array([], type=pa.string())], names=["__empty__"]).drop_columns(["__empty__"])

    out_df = pd.DataFrame(results)
    return pa.Table.from_pandas(out_df, preserve_index=False)


# -------------------------------
# Helpers: bin selection + resume
# -------------------------------
def parse_k5_include(arg: str) -> Optional[set]:
    if not arg:
        return None
    if os.path.exists(arg):
        with open(arg) as f:
            return {int(line.strip()) for line in f if line.strip()}
    return {int(x) for x in arg.replace(",", " ").split() if x.strip()}


def existing_k5_in_tmp(tmp_dir: str) -> set:
    # expect files like tmp/k5=1234.parquet
    out = set()
    for path in glob.glob(os.path.join(tmp_dir, "k5=*.parquet")):
        base = os.path.basename(path)
        try:
            k5 = int(base.split("=")[1].split(".")[0])
            out.add(k5)
        except Exception:
            continue
    return out


def finalize_shards(tmp_dir: str, out_path: str, schema: pa.Schema):
    # Concatenate all shard files in tmp/ into a single ALL.parquet
    shard_paths = sorted(glob.glob(os.path.join(tmp_dir, "k5=*.parquet")))
    if not shard_paths:
        print("[INFO] No shard files found; skipping finalize.")
        return

    writer = pq.ParquetWriter(out_path, schema=schema, compression="snappy")
    try:
        for p in shard_paths:
            tbl = pq.read_table(p)
            # ensure exact schema
            if tbl.schema != schema:
                tbl = cast_table_to_schema(tbl, schema)
            writer.write_table(tbl)
    finally:
        writer.close()
    print(f"[DONE] Finalized {len(shard_paths)} shards → {out_path}")


# -------------------------------
# Main
# -------------------------------
def parse_years_arg(years_arg: str) -> List[str]:
    env = os.environ.get("NEOWISE_YEARS", "").strip()
    if not years_arg and env:
        years_arg = env
    if not years_arg:
        return [f"year{y}" for y in range(1, 12)]  # year1..year11
    return [p.strip() for p in years_arg.replace(",", " ").split() if p.strip()]


def main():
    parser = argparse.ArgumentParser(description="NEOWISE sidecar from S3 parquet (anonymous, resumable)")
    parser.add_argument("--years", type=str, default=os.environ.get("NEOWISE_YEARS", ""),
                        help='Years to process (e.g. "year8" or "year8,year9"). Default: env or year1..year11')
    parser.add_argument("--optical-root", type=str, default=DEFAULT_OPTICAL_PARQUET_ROOT,
                        help="Root folder of optical master Parquet dataset")
    parser.add_argument("--out-root", type=str, default=DEFAULT_OUT_FLAGS_ROOT,
                        help="Output root (sidecar & tmp folder live here)")
    parser.add_argument("--radius-arcsec", type=float,
                        default=float(os.environ.get("NEO_RADIUS_ARCSEC", "5.0")),
                        help="Association radius in arcsec (default 5.0, MNRAS 2022)")
    parser.add_argument("--columns", type=str, default="",
                        help='NEOWISE columns to read (CSV). Default minimal set.')
    parser.add_argument("--parallel", choices=["none", "pixel"], default="none",
                        help="Parallelize by k5 bin with threads ('pixel'). Default: none")
    parser.add_argument("--workers", type=int, default=0,
                        help="Worker threads for --parallel pixel (default: min(8, CPU))")
    parser.add_argument("--no-finalize", action="store_true",
                        help="Do not create ALL.parquet; leave per-k5 shards only")
    parser.add_argument("--force", action="store_true",
                        help="Recompute shards even if tmp/k5=*.parquet exists")
    parser.add_argument("--k5-limit", type=int, default=0,
                        help="Process only first N k5 bins (0=all)")
    parser.add_argument("--k5-include", type=str, default="",
                        help="Restrict to k5 set (comma list) or a file with one k5 per line")

    args = parser.parse_args()

    years = parse_years_arg(args.years)
    optical_root = args.optical_root
    out_root = args.out_root
    tmp_dir = os.path.join(out_root, "tmp")
    os.makedirs(tmp_dir, exist_ok=True)

    arcsec_radius = float(args.radius_arcsec)
    neo_cols = [c.strip() for c in args.columns.split(",")] if args.columns else DEFAULT_NEO_COLS

    print(f"[INFO] PyArrow version: {pa.__version__}")
    try:
        import healpy as _hp  # noqa
        print("[INFO] HEALPix backend: healpy")
    except Exception:
        print("[WARN] healpy not detected (we will try astropy-healpix or hpgeom).")

    print(f"[INFO] NEOWISE years: {years}")
    print(f"[INFO] Optical root: {optical_root}")
    print(f"[INFO] Output root:  {out_root}")
    print(f"[INFO] Temp shards:  {tmp_dir}")
    print(f"[INFO] Match radius: {arcsec_radius:.2f}\" (paper-consistent default)")
    print(f"[INFO] NEOWISE columns: {neo_cols}")

    # Build datasets and load positions
    neowise_ds = build_neowise_dataset(years)
    optical_df = load_optical_positions(optical_root)

    # Bin selection
    unique_bins = list(optical_df["healpix_k5"].unique())
    include_set = parse_k5_include(args.k5_include)
    if include_set:
        unique_bins = [b for b in unique_bins if b in include_set]
        print(f"[INFO] Restricting to {len(unique_bins)} listed k5 bins.")

    if args.k5_limit and args.k5_limit < len(unique_bins):
        unique_bins = unique_bins[:args.k5_limit]
        print(f"[INFO] Limiting to first {len(unique_bins)} k5 bins.")

    print(f"[INFO] Optical unique k5 bins to process: {len(unique_bins)}")

    # Resume: skip bins that already have a shard
    to_skip = set() if args.force else existing_k5_in_tmp(tmp_dir)
    if to_skip and not args.force:
        print(f"[INFO] Resume mode: will skip {len(to_skip)} already completed k5 bins.")

    # Prepare schema for shards
    schema = result_schema()

    # Define per-bin worker
    def process_one(k5: int) -> Tuple[int, int]:
        # returns (k5, rows_written)
        shard_path = os.path.join(tmp_dir, f"k5={k5}.parquet")
        if (not args.force) and os.path.exists(shard_path):
            return (k5, -1)  # skipped

        opt_part = optical_df[optical_df["healpix_k5"] == k5]
        tbl = match_partition_to_table(opt_part, neowise_ds, int(k5), arcsec_radius, neo_cols)

        if tbl.num_rows == 0:
            # write an empty table with correct schema to mark as done (optional)
            empty_tbl = pa.Table.from_arrays([pa.array([], type=f.type) for f in schema], names=schema.names)
            pq.write_table(empty_tbl, shard_path, compression="snappy")
            return (k5, 0)

        tbl = cast_table_to_schema(tbl, schema)
        pq.write_table(tbl, shard_path, compression="snappy")
        return (k5, tbl.num_rows)

    # Execute
    processed = 0
    written = 0
    skipped = 0

    if args.parallel == "pixel":
        import multiprocessing as mp
        max_workers = args.workers if args.workers > 0 else min(8, (mp.cpu_count() or 8))
        print(f"[INFO] Parallel mode: pixel, workers={max_workers}")

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {}
            for k5 in unique_bins:
                if (not args.force) and (k5 in to_skip):
                    skipped += 1
                    continue
                futures[ex.submit(process_one, k5)] = k5

            for i, fut in enumerate(as_completed(futures), 1):
                k5, rows = fut.result()
                processed += 1
                if rows >= 0:
                    written += rows
                else:
                    skipped += 1
                if processed % 50 == 0:
                    print(f"[INFO] Processed {processed} / {len(unique_bins)} bins "
                          f"(skipped={skipped}, rows_written={written})")

    else:
        for k5 in unique_bins:
            if (not args.force) and (k5 in to_skip):
                skipped += 1
                continue
            k5_, rows = process_one(k5)
            processed += 1
            if rows >= 0:
                written += rows
            else:
                skipped += 1
            if processed % 50 == 0:
                print(f"[INFO] Processed {processed} / {len(unique_bins)} bins "
                      f"(skipped={skipped}, rows_written={written})")

    print(f"[INFO] Completed bins: {processed}, skipped: {skipped}, total rows written: {written}")

    # Finalize (unless disabled)
    out_path = os.path.join(out_root, DEFAULT_OUT_FILE)
    if not args.no_finalize:
        finalize_shards(tmp_dir, out_path, schema)
    else:
        print("[INFO] Finalize disabled; shards remain in tmp/.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[WARN] Interrupted by user (Ctrl+C). You can resume safely; existing shards are kept.")
        sys.exit(130)
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)
