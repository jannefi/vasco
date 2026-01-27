#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NEOWISE sidecar via AWS S3 Parquet (anonymous) — Strategy A: leaf-targeted by k5

Version: 2026-01-27 (Strategy-A leaf mode)

WHAT THIS DOES
--------------
Reads NEOWISER Single-exposure Source Table directly from IRSA’s public S3,
but instead of opening an entire per-year dataset (slow due to large _metadata),
it opens ONLY the per-partition (healpix_k5) "leaf" directory needed for each
optical k5 bin. That eliminates the heavy year-root cold-start.

- Anonymous S3 access (region-pinned to us-west-2)
- Resume-friendly per-k5 shards:   <out_root>/tmp/k5=<id>.parquet
- Final merged file (unless --no-finalize): <out_root>/neowise_se_flags_ALL.parquet
- Parallelize by k5 with threads:  --parallel pixel --workers N
- Works with optical parquet on local disk or s3://

IRSA PATH SHAPE
---------------
s3://nasa-irsa-wise/wise/neowiser/catalogs/p1bs_psd/healpix_k5/<YEAR>/
  neowiser-healpix_k5-<YEAR>.parquet/
    healpix_k0=<0..11>/
      healpix_k5=<0..12287>/
        part*.snappy.parquet
      _common_metadata
      _metadata

Leaf path for (year='year9', k5=5318):
  nasa-irsa-wise/wise/neowiser/catalogs/p1bs_psd/healpix_k5/year9/
  neowiser-healpix_k5-year9.parquet/healpix_k0=5/healpix_k5=5318/

USAGE (examples)
----------------
# Single year, limited k5 set from your optical delta; 8 workers
python neowise_s3_sidecar.py \
  --years year9 \
  --optical-root "s3://janne-vasco-usw2/vasco/deltas/abtest-00001/optical_delta" \
  --out-root "./data/local-cats/_master_optical_parquet_irflags" \
  --parallel pixel --workers 8 \
  --k5-include "5318,8277" \
  --columns "cntr,source_id,ra,dec,mjd,w1flux,w1sigflux,w2flux,w2sigflux,w1snr,w2snr,qual_frame,qi_fact,saa_sep,moon_masked"

# Process all k5 bins found in the optical delta (resume-safe, default columns)
python neowise_s3_sidecar.py --years year9 --optical-root ... --out-root ... --parallel pixel --workers 8

Notes:
- You can safely Ctrl+C; reruns will skip existing tmp/k5=<id>.parquet unless --force.
- Keep --years to 1 while iterating; expand later if needed.
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
from pyarrow import fs as pafs


# ------------------------
# Configuration (paths)
# ------------------------
S3_BUCKET = "nasa-irsa-wise"
S3_PREFIX = "wise/neowiser/catalogs/p1bs_psd/healpix_k5"
DEFAULT_OPTICAL_PARQUET_ROOT = "./data/local-cats/_master_optical_parquet"
DEFAULT_OUT_FLAGS_ROOT = "./data/local-cats/_master_optical_parquet_irflags"
DEFAULT_TMP_DIR = os.path.join(DEFAULT_OUT_FLAGS_ROOT, "tmp")
DEFAULT_OUT_FILE = "neowise_se_flags_ALL.parquet"

# Minimal columns needed downstream (overridable with --columns)
DEFAULT_NEO_COLS = [
    "cntr", "source_id", "ra", "dec", "mjd",
    "w1flux", "w1sigflux", "w2flux", "w2sigflux",
    # Often requested for TAP parity:
    "w1snr", "w2snr", "qual_frame", "qi_fact", "saa_sep", "moon_masked",
]

# Candidate RA/Dec names to autodetect in optical parquet
_RADEC_PAIRS = [
    ("opt_ra_deg", "opt_dec_deg"),
    ("ra_deg", "dec_deg"),
    ("ALPHA_J2000", "DELTA_J2000"),
    ("ALPHAWIN_J2000", "DELTAWIN_J2000"),
    ("X_WORLD", "Y_WORLD"),
]


# ------------------------
# HEALPix helpers (k=5)
# ------------------------
def k5_index_ra_dec(ra_deg_array: np.ndarray, dec_deg_array: np.ndarray) -> np.ndarray:
    """Return HEALPix NESTED k=5 index for each (ra,dec)."""
    nside = 2 ** 5
    # Try healpy
    try:
        import healpy as hp  # type: ignore
        theta = np.deg2rad(90.0 - np.asarray(dec_deg_array, dtype=float))
        phi = np.deg2rad(np.asarray(ra_deg_array, dtype=float))
        return hp.ang2pix(nside, theta, phi, nest=True)
    except Exception:
        pass
    # Try astropy-healpix
    try:
        from astropy_healpix import HEALPix  # type: ignore
        HPX = HEALPix(nside=nside, order='nested')
        return HPX.lonlat_to_healpix(np.deg2rad(ra_deg_array), np.deg2rad(dec_deg_array))
    except Exception:
        pass
    # Try hpgeom
    try:
        import hpgeom as hpg  # type: ignore
        try:
            theta, phi = hpg.lonlat_to_thetaphi(ra_deg_array, dec_deg_array, degrees=True)
        except TypeError:
            theta, phi = hpg.lonlat_to_thetaphi(np.deg2rad(ra_deg_array), np.deg2rad(dec_deg_array))
        if hasattr(hpg, "thetaphi_to_healpix"):
            return hpg.thetaphi_to_healpix(theta, phi, order=5, nest=True)
        import healpy as hp  # fallback if present
        return hp.ang2pix(nside, theta, phi, nest=True)
    except Exception:
        pass
    raise RuntimeError("HEALPix indexing failed (healpy/astropy-healpix/hpgeom).")


# ------------------------
# Geometry / matching
# ------------------------
def arcsec2rad(arcsec: float) -> float:
    return arcsec / 206264.806


def rad2arcsec(rad: np.ndarray) -> np.ndarray:
    return rad * 206264.806


def haversine_sep_arcsec(ra0_deg: float, dec0_deg: float,
                         ra_deg: np.ndarray, dec_deg: np.ndarray) -> np.ndarray:
    d2r = np.pi / 180.0
    dra = (ra_deg - ra0_deg) * d2r
    ddec = (dec_deg - dec0_deg) * d2r
    a = (np.sin(ddec / 2.0) ** 2
         + np.cos(dec0_deg * d2r) * np.cos(dec_deg * d2r) * np.sin(dra / 2.0) ** 2)
    return rad2arcsec(2.0 * np.arcsin(np.sqrt(a)))


# ------------------------
# IRSA S3 utilities
# ------------------------
def _mk_s3fs(anon: bool) -> pafs.S3FileSystem:
    # Region pin required for anonymous access with Arrow on many setups
    return pafs.S3FileSystem(anonymous=anon, region="us-west-2")


def _irsa_year_leaf_path(year: str, k5: int) -> str:
    """
    Build the IRSA 'leaf' dataset path for one year and one healpix_k5.

    Example:
      nasa-irsa-wise/wise/neowiser/catalogs/p1bs_psd/healpix_k5/year9/
      neowiser-healpix_k5-year9.parquet/healpix_k0=5/healpix_k5=5318/
    """
    k0 = k5 // 1024  # 4**5 = 1024
    return (f"{S3_BUCKET}/{S3_PREFIX}/{year}/"
            f"neowiser-healpix_k5-{year}.parquet/"
            f"healpix_k0={k0}/healpix_k5={k5}/")


def _leaf_exists(fs: pafs.S3FileSystem, path: str) -> bool:
    try:
        info = fs.get_file_info([path])[0]
        return info.type == pafs.FileType.Directory
    except Exception:
        return False


# ------------------------
# Optical parquet I/O
# ------------------------
def _choose_radec(schema_names: List[str]) -> Optional[Tuple[str, str]]:
    s = set(schema_names)
    for ra_name, dec_name in _RADEC_PAIRS:
        if ra_name in s and dec_name in s:
            return ra_name, dec_name
    return None


def load_optical_positions(parquet_root: str) -> pd.DataFrame:
    """
    Load optical positions (local or s3://). Returns DataFrame with:
      source_id (string), opt_ra_deg, opt_dec_deg, healpix_k5, (optional) ra_bin, dec_bin
    """
    if parquet_root.startswith("s3://"):
        ds = pds.dataset(parquet_root.replace("s3://", "", 1),
                         format="parquet", filesystem=_mk_s3fs(anon=False))
    else:
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
    if "ra_bin" in names: need.append("ra_bin")
    if "dec_bin" in names: need.append("dec_bin")

    have_source_id = ("source_id" in names)
    if have_source_id:
        need.append("source_id")
    else:
        for c in ("NUMBER", "tile_id", "image_id"):
            if c in names:
                need.append(c)

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
    keep = ["source_id", "opt_ra_deg", "opt_dec_deg", "healpix_k5"]
    if "ra_bin" in df.columns: keep.append("ra_bin")
    if "dec_bin" in df.columns: keep.append("dec_bin")
    return df[keep]


# ------------------------
# Output schema & casting
# ------------------------
def result_schema() -> pa.schema:
    return pa.schema([
        ("opt_source_id", pa.string()),
        ("opt_ra_deg", pa.float64()),
        ("opt_dec_deg", pa.float64()),
        ("source_id", pa.string()),
        ("cntr", pa.int64()),
        ("ra", pa.float64()),
        ("dec", pa.float64()),
        ("mjd", pa.float64()),
        ("w1flux", pa.float32()),
        ("w1sigflux", pa.float32()),
        ("w2flux", pa.float32()),
        ("w2sigflux", pa.float32()),
        ("w1snr", pa.float32()),
        ("w2snr", pa.float32()),
        ("qual_frame", pa.int64()),
        ("qi_fact", pa.float32()),
        ("saa_sep", pa.float32()),
        ("moon_masked", pa.string()),
        ("sep_arcsec", pa.float32()),
        ("healpix_k5", pa.int32()),
    ])


def cast_table_to_schema(tbl: pa.Table, schema: pa.Schema) -> pa.Table:
    arrays, names = [], []
    for field in schema:
        name = field.name
        if name in tbl.column_names:
            col = tbl[name]
            if not col.type.equals(field.type):
                try:
                    col = pc.cast(col, field.type)
                except Exception:
                    col = pa.nulls(tbl.num_rows, type=field.type)
            arrays.append(col)
            names.append(name)
        else:
            arrays.append(pa.nulls(tbl.num_rows, type=field.type))
            names.append(name)
    return pa.Table.from_arrays(arrays, names=names)


# ------------------------
# Leaf-window filter (per k5)
# ------------------------
def _make_bbox_filter_for_pixel(opt_part_df: pd.DataFrame, arcsec_radius: float):
    """Build a single RA/Dec predicate covering all optical points (+radius)."""
    delta_deg = math.degrees(arcsec2rad(arcsec_radius))
    ra_vals = opt_part_df["opt_ra_deg"].values % 360.0
    dec_vals = opt_part_df["opt_dec_deg"].values
    ra_min = float(np.min(ra_vals)) - delta_deg
    ra_max = float(np.max(ra_vals)) + delta_deg
    dec_min = float(np.min(dec_vals)) - delta_deg
    dec_max = float(np.max(dec_vals)) + delta_deg

    ra = pc.field("ra")
    dec = pc.field("dec")
    # Handle RA wrap
    if ra_min < 0.0:
        f_ra = ((ra >= 0.0) & (ra <= ra_max)) | (ra >= (ra_min + 360.0))
    elif ra_max >= 360.0:
        f_ra = ((ra >= ra_min) & (ra < 360.0)) | (ra <= (ra_max - 360.0))
    else:
        f_ra = (ra >= ra_min) & (ra <= ra_max)
    f_dec = (dec >= dec_min) & (dec <= dec_max)
    return f_ra & f_dec


# ------------------------
# Per-k5 matching (leaf-only reads)
# ------------------------
def match_k5_with_leaf_reads(opt_part_df: pd.DataFrame,
                             years: Iterable[str],
                             arcsec_radius: float,
                             neo_cols: List[str]) -> pa.Table:
    """
    For one k5 bin:
      - For each year, open ONLY the leaf directory for that k5
      - Read minimal columns with a single RA/Dec bounding-box filter
      - Nearest-neighbour match within arcsec_radius
      - Return Arrow Table (result_schema)
    """
    if opt_part_df.empty:
        # Empty by design
        return pa.Table.from_arrays([pa.array([], type=f.type) for f in result_schema()],
                                    names=result_schema().names)

    fs = _mk_s3fs(anon=True)
    filt = _make_bbox_filter_for_pixel(opt_part_df, arcsec_radius)

    # Columns to request (dedup, preserve order)
    required = ["ra", "dec", "mjd", "source_id", "cntr"]
    optional = ["w1flux", "w1sigflux", "w2flux", "w2sigflux",
                "w1snr", "w2snr", "qual_frame", "qi_fact", "saa_sep", "moon_masked"]
    want = list(dict.fromkeys(required + list(neo_cols) + optional))

    # Pull rows for this k5 across all selected years
    neo_frames: List[pd.DataFrame] = []
    cols_found: Optional[List[str]] = None

    for yr in years:
        leaf = _irsa_year_leaf_path(yr, int(opt_part_df["healpix_k5"].iloc[0]))
        if not _leaf_exists(fs, leaf):
            print(f"[WARN] Missing leaf for {yr}: {leaf}")
            continue

        ds_leaf = pds.dataset(leaf, format="parquet", filesystem=fs, partitioning="hive")
        # Intersect requested columns with what exists in this year
        have = [c for c in want if c in ds_leaf.schema.names]
        if not set(required).issubset(have):
            print(f"[WARN] Leaf lacks required columns in {yr}: {leaf} (have={have})")
            continue

        tbl = ds_leaf.to_table(filter=filt, columns=have)
        if tbl.num_rows == 0:
            continue

        if cols_found is None:
            cols_found = have  # remember first-year column set (for sub.loc below)

        neo_frames.append(tbl.to_pandas())

    if not neo_frames:
        # No NEOWISE candidates in window across the chosen years
        return pa.Table.from_arrays([pa.array([], type=f.type) for f in result_schema()],
                                    names=result_schema().names)

    neo_df = pd.concat(neo_frames, ignore_index=True)
    neo_ra = neo_df["ra"].values
    neo_dec = neo_df["dec"].values
    delta_deg = math.degrees(arcsec2rad(arcsec_radius))

    # Brute-force nearest in a pre-filtered shortlist (fast enough at these sizes)
    out_rows = []
    have_cols = set(neo_df.columns)
    cols_proj = [c for c in (cols_found or want) if c in have_cols]

    for _, row in opt_part_df.iterrows():
        ra0 = float(row["opt_ra_deg"])
        dec0 = float(row["opt_dec_deg"])
        opt_id = row["source_id"]

        m = ((neo_ra >= ra0 - delta_deg) & (neo_ra <= ra0 + delta_deg) &
             (neo_dec >= dec0 - delta_deg) & (neo_dec <= dec0 + delta_deg))
        if not m.any():
            continue

        sub = neo_df.loc[m, cols_proj]
        d_arcsec = haversine_sep_arcsec(ra0, dec0, sub["ra"].values, sub["dec"].values)
        within = d_arcsec <= arcsec_radius
        if not within.any():
            continue

        j = int(np.argmin(d_arcsec))
        hit = sub.iloc[j].to_dict()
        hit["sep_arcsec"] = float(d_arcsec[j])
        hit["opt_source_id"] = str(opt_id)
        hit["opt_ra_deg"] = ra0
        hit["opt_dec_deg"] = dec0
        hit["healpix_k5"] = int(row["healpix_k5"])
        out_rows.append(hit)

    if not out_rows:
        return pa.Table.from_arrays([pa.array([], type=f.type) for f in result_schema()],
                                    names=result_schema().names)

    out_df = pd.DataFrame(out_rows)
    sch = result_schema()
    out = pa.Table.from_pandas(out_df, preserve_index=False)
    return cast_table_to_schema(out, sch)


# ------------------------
# Shards, resume & finalize
# ------------------------
def parse_k5_include(arg: str) -> Optional[set]:
    if not arg:
        return None
    if os.path.exists(arg):
        with open(arg) as f:
            return {int(line.strip()) for line in f if line.strip()}
    return {int(x) for x in arg.replace(",", " ").split() if x.strip()}


def existing_k5_in_tmp(tmp_dir: str) -> set:
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
    shard_paths = sorted(glob.glob(os.path.join(tmp_dir, "k5=*.parquet")))
    if not shard_paths:
        print("[INFO] No shard files found; skipping finalize.")
        return
    writer = pq.ParquetWriter(out_path, schema=schema, compression="snappy")
    try:
        for p in shard_paths:
            tbl = pq.read_table(p)
            if tbl.schema != schema:
                tbl = cast_table_to_schema(tbl, schema)
            writer.write_table(tbl)
    finally:
        writer.close()
    print(f"[DONE] Finalized {len(shard_paths)} shards → {out_path}")


# ------------------------
# CLI / driver
# ------------------------
def parse_years_arg(years_arg: str) -> List[str]:
    env = os.environ.get("NEOWISE_YEARS", "").strip()
    if not years_arg and env:
        years_arg = env
    if not years_arg:
        return [f"year{y}" for y in range(1, 12)]  # year1..year11
    return [p.strip() for p in years_arg.replace(",", " ").split() if p.strip()]


def main():
    ap = argparse.ArgumentParser(description="NEOWISE sidecar (IRSA S3 parquet, Strategy-A leaf-only)")
    ap.add_argument("--years", type=str, default=os.environ.get("NEOWISE_YEARS", ""),
                    help='Years to process (e.g. "year8" or "year8,year9"). Default: env or year1..year11')
    ap.add_argument("--optical-root", type=str, default=DEFAULT_OPTICAL_PARQUET_ROOT,
                    help="Root folder of optical master Parquet dataset (local or s3://)")
    ap.add_argument("--out-root", type=str, default=DEFAULT_OUT_FLAGS_ROOT,
                    help="Output root (sidecar & tmp live here)")
    ap.add_argument("--radius-arcsec", type=float,
                    default=float(os.environ.get("NEO_RADIUS_ARCSEC", "5.0")),
                    help="Association radius in arcsec (default 5.0)")
    ap.add_argument("--columns", type=str, default="",
                    help='NEOWISE columns to read (CSV). Default minimal set.')
    ap.add_argument("--parallel", choices=["none", "pixel"], default="none",
                    help="Parallelize by k5 with threads ('pixel').")
    ap.add_argument("--workers", type=int, default=0,
                    help="Worker threads for --parallel pixel (default: min(8, CPU))")
    ap.add_argument("--no-finalize", action="store_true",
                    help="Do not create ALL.parquet; leave per-k5 shards only")
    ap.add_argument("--force", action="store_true",
                    help="Recompute shards even if tmp/k5=*.parquet exists")
    ap.add_argument("--k5-limit", type=int, default=0,
                    help="Process only first N k5 bins (0=all)")
    ap.add_argument("--k5-include", type=str, default="",
                    help="Restrict to k5 set (comma list) or a file with one k5 per line")
    args = ap.parse_args()

    years = parse_years_arg(args.years)
    optical_root = args.optical_root
    out_root = args.out_root
    tmp_dir = os.path.join(out_root, "tmp")
    os.makedirs(tmp_dir, exist_ok=True)

    arcsec_radius = float(args.radius_arcsec)
    if args.columns:
        neo_cols = [c.strip() for c in args.columns.split(",") if c.strip()]
    else:
        neo_cols = DEFAULT_NEO_COLS

    print(f"[INFO] PyArrow version: {pa.__version__}")
    try:
        import healpy as _hp  # noqa
        print("[INFO] HEALPix backend: healpy")
    except Exception:
        try:
            import astropy_healpix as _ahp  # noqa
            print("[INFO] HEALPix backend: astropy-healpix")
        except Exception:
            print("[WARN] No healpy/astropy-healpix found; will try hpgeom if available.")

    print(f"[INFO] NEOWISE years: {years}")
    print(f"[INFO] Optical root: {optical_root}")
    print(f"[INFO] Output root:  {out_root}")
    print(f"[INFO] Temp shards:  {tmp_dir}")
    print(f"[INFO] Match radius: {arcsec_radius:.2f}\"")
    print(f"[INFO] NEOWISE columns: {neo_cols}")

    # Load optical positions and compute k5 bins
    optical_df = load_optical_positions(optical_root)

    unique_bins = list(map(int, optical_df["healpix_k5"].unique()))
    include_set = parse_k5_include(args.k5_include)
    if include_set:
        unique_bins = [b for b in unique_bins if b in include_set]
        print(f"[INFO] Restricting to {len(unique_bins)} listed k5 bins.")
    if args.k5_limit and args.k5_limit < len(unique_bins):
        unique_bins = unique_bins[:args.k5_limit]
        print(f"[INFO] Limiting to first {len(unique_bins)} k5 bins.")
    print(f"[INFO] Optical unique k5 bins to process: {len(unique_bins)}")

    # Resume support
    to_skip = set() if args.force else existing_k5_in_tmp(tmp_dir)
    if to_skip and not args.force:
        print(f"[INFO] Resume mode: will skip {len(to_skip)} already completed k5 bins.")

    sch = result_schema()

    def process_one(k5: int) -> Tuple[int, int]:
        shard_path = os.path.join(tmp_dir, f"k5={k5}.parquet")
        if (not args.force) and os.path.exists(shard_path):
            return (k5, -1)

        opt_part = optical_df[optical_df["healpix_k5"] == k5]
        if opt_part.empty:
            empty_tbl = pa.Table.from_arrays([pa.array([], type=f.type) for f in sch], names=sch.names)
            pq.write_table(empty_tbl, shard_path, compression="snappy")
            return (k5, 0)

        tbl = match_k5_with_leaf_reads(opt_part, years, arcsec_radius, neo_cols)
        # Always write a valid shard (possibly empty with schema)
        if tbl.num_rows == 0:
            tbl = pa.Table.from_arrays([pa.array([], type=f.type) for f in sch], names=sch.names)
        elif tbl.schema != sch:
            tbl = cast_table_to_schema(tbl, sch)

        pq.write_table(tbl, shard_path, compression="snappy")
        return (k5, tbl.num_rows)

    processed = 0
    written = 0
    skipped = 0

    if args.parallel == "pixel":
        import multiprocessing as mp
        max_workers = args.workers if args.workers > 0 else min(8, (mp.cpu_count() or 8))
        print(f"[INFO] Parallel mode: pixel, workers={max_workers}")
        futs = {}
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            for k5 in unique_bins:
                if (not args.force) and (k5 in to_skip):
                    skipped += 1
                    continue
                futs[ex.submit(process_one, int(k5))] = k5

            for i, fut in enumerate(as_completed(futs), 1):
                k5, rows = fut.result()
                processed += 1
                if rows >= 0:
                    written += rows
                else:
                    skipped += 1
                if processed % 50 == 0 or processed == len(futs):
                    print(f"[INFO] Processed {processed}/{len(futs)} bins "
                          f"(skipped={skipped}, rows_written={written})")
    else:
        for k5 in unique_bins:
            if (not args.force) and (k5 in to_skip):
                skipped += 1
                continue
            _, rows = process_one(int(k5))
            processed += 1
            if rows >= 0:
                written += rows
            else:
                skipped += 1
            if processed % 50 == 0 or processed == len(unique_bins):
                print(f"[INFO] Processed {processed}/{len(unique_bins)} bins "
                      f"(skipped={skipped}, rows_written={written})")

    print(f"[INFO] Completed bins: {processed}, skipped: {skipped}, total rows written: {written}")

    out_path = os.path.join(out_root, DEFAULT_OUT_FILE)
    if not args.no_finalize:
        finalize_shards(tmp_dir, out_path, sch)
    else:
        print("[INFO] Finalize disabled; shards remain in tmp/.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[WARN] Interrupted by user (Ctrl+C). Safe to resume; existing shards are kept.")
        sys.exit(130)
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)