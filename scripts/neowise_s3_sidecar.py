
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NEOWISE sidecar via AWS S3 Parquet (anonymous access), with RESUME + PARALLEL.

What this script does
---------------------
• Opens the NEOWISE‑R Single Exposure Source Table (Parquet, HEALPix k=5) from IRSA's public S3
  via PyArrow's S3 filesystem **with region pinned**; **anonymous** access.
• Reads your optical master Parquet (VASCO) and computes healpix_k5 for each source.
• For each k5 bin, loads **only that pixel's** NEOWISE parquet shards (per selected years),
  avoiding any scanning of entire year directories.
• Finds the closest NEOWISE detection within --radius-arcsec.
• Writes ONE shard per k5 bin under:
    ./data/local-cats/_master_optical_parquet_irflags/tmp/k5=<int>.parquet
  → If a shard already exists, the bin is skipped (RESUME friendly).
• (By default) Finalizes by concatenating all shards into:
    ./data/local-cats/_master_optical_parquet_irflags/neowise_se_flags_ALL.parquet

Key flags
---------
--parallel {none,pixel} : 'pixel' = parallelize per k5 bin (ThreadPool).
--workers N             : worker threads for 'pixel' mode. Default: min(8, CPU count).
--columns "<csv list>"  : which NEOWISE columns to read. Default minimal set.
--no-finalize           : leave only shards (skip ALL.parquet).
--force                 : recompute shards even if they already exist.
--k5-limit N            : process only first N k5 bins (0=all).
--k5-include "<list or file>" : restrict to specified k5 set.
--years "year9,year10"  : limit IRSA years; default is env NEOWISE_YEARS or year1..year11.

Notes
-----
• This version avoids listing large IRSA prefixes and only touches k5 folders you actually need.
• It tolerates missing optional columns (e.g. w1snr), while requiring a small base set.
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


# ----------------------------
# Configuration (paths/columns)
# ----------------------------
S3_BUCKET = "nasa-irsa-wise"
S3_PREFIX = "wise/neowiser/catalogs/p1bs_psd/healpix_k5"

DEFAULT_OPTICAL_PARQUET_ROOT = "./data/local-cats/_master_optical_parquet"
DEFAULT_OUT_FLAGS_ROOT = "./data/local-cats/_master_optical_parquet_irflags"
DEFAULT_TMP_DIR = os.path.join(DEFAULT_OUT_FLAGS_ROOT, "tmp")
DEFAULT_OUT_FILE = "neowise_se_flags_ALL.parquet"

# Minimal NEOWISE columns needed for association + flags; overridable via --columns
DEFAULT_NEO_COLS = [
    "cntr", "source_id", "ra", "dec", "mjd",
    "w1flux", "w1sigflux", "w2flux", "w2sigflux",   # keep fluxes in default; downstream expects them
]

# RA/Dec detection in the optical parquet
_RADEC_PAIRS = [
    ("opt_ra_deg", "opt_dec_deg"),
    ("ra_deg", "dec_deg"),
    ("ALPHA_J2000", "DELTA_J2000"),        # SExtractor (world coords)
    ("ALPHAWIN_J2000", "DELTAWIN_J2000"),  # SExtractor (windowed)
    ("X_WORLD", "Y_WORLD"),                # SExtractor alt world coords
]


# -----------------------
# HEALPix helpers (k = 5)
# -----------------------
def k5_index_ra_dec(ra_deg_array: np.ndarray, dec_deg_array: np.ndarray) -> np.ndarray:
    """Compute nested HEALPix index for k=5 using whatever backend is available."""
    nside = 2 ** 5
    # 1) healpy
    try:
        import healpy as hp
        theta = np.deg2rad(90.0 - np.asarray(dec_deg_array, dtype=float))
        phi   = np.deg2rad(np.asarray(ra_deg_array, dtype=float))
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
    # 3) hpgeom
    try:
        import hpgeom as hpg
        try:
            theta, phi = hpg.lonlat_to_thetaphi(ra_deg_array, dec_deg_array, degrees=True)
        except TypeError:
            theta, phi = hpg.lonlat_to_thetaphi(np.deg2rad(ra_deg_array), np.deg2rad(dec_deg_array))
        if hasattr(hpg, "thetaphi_to_healpix"):
            return hpg.thetaphi_to_healpix(theta, phi, order=5, nest=True)
        # fallback via healpy
        import healpy as hp
        return hp.ang2pix(nside, theta, phi, nest=True)
    except Exception:
        pass
    raise RuntimeError(
        "HEALPix indexing failed (healpy/astropy-healpix/hpgeom not usable). "
        "Install 'healpy' or 'astropy-healpix'."
    )


# ------------------------------
# Geometry / matching utilities
# ------------------------------
def arcsec2rad(arcsec: float) -> float:
    return arcsec / 206264.806

def rad2arcsec(rad: np.ndarray) -> np.ndarray:
    return rad * 206264.806

def haversine_sep_arcsec(ra0_deg: float, dec0_deg: float,
                         ra_deg: np.ndarray, dec_deg: np.ndarray) -> np.ndarray:
    d2r = np.pi / 180.0
    dra  = (ra_deg - ra0_deg) * d2r
    ddec = (dec_deg - dec0_deg) * d2r
    a = np.sin(ddec / 2.0) ** 2 + np.cos(dec0_deg * d2r) * np.cos(dec_deg * d2r) * np.sin(dra / 2.0) ** 2
    return rad2arcsec(2.0 * np.arcsin(np.sqrt(a)))


# ------------------------
# IRSA S3 access utilities
# ------------------------
def _mk_s3fs(anon: bool) -> pafs.S3FileSystem:
    """Region‑pinned S3 filesystem (Arrow 21 needs explicit region when anonymous)."""
    return pafs.S3FileSystem(anonymous=anon, region="us-west-2")

def _neowise_pixel_paths(year: str, k5: int) -> List[str]:
    """
    IRSA layout under .../healpix_k5/yearX/... may be hive-like.
    Try a few plausible directory patterns for a given pixel.
    """
    base = f"{S3_BUCKET}/{S3_PREFIX}/{year}"
    return [
        f"{base}/healpix_k5={k5}",  # typical hive partition
        f"{base}/k5={k5}",          # alt
        f"{base}/h={k5}",           # alt
    ]

def _build_neowise_dataset_for_k5(years: List[str], k5: int) -> Optional[pds.Dataset]:
    """
    Build a tiny dataset for a single healpix_k5 pixel across the selected years,
    without scanning entire year directories. Returns None if nothing is found.
    """
    fs = _mk_s3fs(anon=True)
    dsets = []

    for yr in years:
        found_paths: List[str] = []
        chosen_root: Optional[str] = None

        # Try several likely pixel directory names; stop at first that exists
        for root in _neowise_pixel_paths(yr, k5):
            try:
                info = fs.get_file_info([root])[0]
            except Exception:
                continue
            if info.type == pafs.FileType.NotFound:
                continue

            # Collect parquet files under this pixel directory (recursive -> sharded files)
            selector = pafs.FileSelector(root, recursive=True)
            try:
                infos = fs.get_file_info(selector)
            except Exception:
                continue

            found_paths = [fi.path for fi in infos if fi.is_file and fi.path.endswith(".parquet")]
            if found_paths:
                chosen_root = root
                break  # found this year's pixel directory

        if found_paths and chosen_root:
            ds = pds.dataset(
                found_paths,
                format="parquet",
                filesystem=fs,
                partitioning="hive",
                partition_base_dir=chosen_root,     # expose healpix_k5 if present
                exclude_invalid_files=True
            )
            dsets.append(ds)

    return pds.dataset(dsets) if dsets else None


# ---------------------------
# Optical parquet (VASCO) I/O
# ---------------------------
def _choose_radec(schema_names: List[str]) -> Optional[Tuple[str, str]]:
    s = set(schema_names)
    for ra_name, dec_name in _RADEC_PAIRS:
        if ra_name in s and dec_name in s:
            return ra_name, dec_name
    return None

def load_optical_positions(parquet_root: str) -> pd.DataFrame:
    """
    Load optical positions from parquet (local or s3://). Returns a DataFrame:
      columns: source_id (string), opt_ra_deg, opt_dec_deg, (optional) ra_bin, dec_bin, healpix_k5
    """
    if parquet_root.startswith("s3://"):
        path_wo_scheme = parquet_root.replace("s3://", "", 1)
        ds = pds.dataset(path_wo_scheme, format="parquet", filesystem=_mk_s3fs(anon=False))
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
    if "ra_bin" in names:  need.append("ra_bin")
    if "dec_bin" in names: need.append("dec_bin")
    have_source_id = ("source_id" in names)
    if have_source_id:
        need.append("source_id")
    else:
        # synthesize later from NUMBER + (tile_id|image_id)
        for c in ("NUMBER", "tile_id", "image_id"):
            if c in names:
                need.append(c)

    tbl = ds.to_table(columns=[c for c in need if c in names])
    df  = tbl.to_pandas()

    # Normalize columns
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

    # Compute k5 index
    df["healpix_k5"] = k5_index_ra_dec(df["opt_ra_deg"].values, df["opt_dec_deg"].values)

    keep = ["source_id", "opt_ra_deg", "opt_dec_deg", "healpix_k5"]
    if "ra_bin"  in df.columns: keep.append("ra_bin")
    if "dec_bin" in df.columns: keep.append("dec_bin")
    return df[keep]


# ---------------------------------------
# Output schema + casting helpers (Arrow)
# ---------------------------------------
def result_schema() -> pa.schema:
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
        ("w1snr",         pa.float32()),
        ("w2snr",         pa.float32()),
        ("qual_frame",    pa.int64()),
        ("qi_fact",       pa.float32()),
        ("saa_sep",       pa.float32()),
        ("moon_masked",   pa.string()),
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
                try:
                    col = pc.cast(col, field.type)
                except Exception:
                    # create nulls if cast is impossible
                    col = pa.nulls(tbl.num_rows, type=field.type)
            arrays.append(col)
            names.append(name)
        else:
            arrays.append(pa.nulls(tbl.num_rows, type=field.type))
            names.append(name)
    return pa.Table.from_arrays(arrays, names=names)


# -------------------------------------
# Per-pixel (k5) matching & extraction
# -------------------------------------
def match_partition_to_table(opt_part_df: pd.DataFrame,
                             years: List[str],
                             k5_pixel: int,
                             arcsec_radius: float,
                             neo_cols: List[str]) -> pa.Table:
    """
    For one k5 bin:
      - build a tiny NEOWISE dataset for just this pixel (across years),
      - materialize requested columns,
      - nearest-neighbour match inside arcsec_radius,
      - return Arrow Table conforming to result_schema().
    """
    # Build dataset for this pixel only
    neowise_ds = _build_neowise_dataset_for_k5(years, int(k5_pixel))
    if neowise_ds is None:
        # Return an empty, but correctly typed, table for this pixel
        empty = pa.Table.from_arrays([pa.array([], type=pa.string())], names=["__empty__"])
        return empty.drop_columns(["__empty__"])

    # Ensure we include partition column if Arrow exposes it
    cols = list(dict.fromkeys(list(neo_cols) + ["healpix_k5"]))
    cols = [c for c in cols if c in neowise_ds.schema.names]

    neo_tbl = neowise_ds.to_table(columns=cols)
    neo_df  = neo_tbl.to_pandas()
    if neo_df.empty:
        empty = pa.Table.from_arrays([pa.array([], type=pa.string())], names=["__empty__"])
        return empty.drop_columns(["__empty__"])

    # Minimal required + optional exposure/quality columns (tolerant to absences)
    required = ["ra", "dec", "mjd", "source_id", "cntr"]
    optional = ["w1flux", "w1sigflux", "w2flux", "w2sigflux",
                "w1snr",  "w2snr",    "qual_frame", "qi_fact", "saa_sep", "moon_masked"]
    want = required + optional
    have = [c for c in want if c in neo_df.columns]
    # If any of required columns missing, abort for this pixel
    if not set(required).issubset(set(have)):
        empty = pa.Table.from_arrays([pa.array([], type=pa.string())], names=["__empty__"])
        return empty.drop_columns(["__empty__"])

    # Pre-extract arrays for vectorized candidate filtering
    neo_ra   = neo_df["ra"].values
    neo_dec  = neo_df["dec"].values

    # Bounding‑box prefilter (fast shortlist)
    delta_deg = math.degrees(arcsec2rad(arcsec_radius))
    results = []

    for _, row in opt_part_df.iterrows():
        ra0  = float(row["opt_ra_deg"])
        dec0 = float(row["opt_dec_deg"])
        opt_id = row["source_id"]

        mask = (
            (neo_ra  >= ra0 - delta_deg) & (neo_ra <= ra0 + delta_deg) &
            (neo_dec >= dec0 - delta_deg) & (neo_dec <= dec0 + delta_deg)
        )
        if not mask.any():
            continue

        sub = neo_df.loc[mask, have]  # only columns that exist
        d_arcsec = haversine_sep_arcsec(ra0, dec0, sub["ra"].values, sub["dec"].values)
        within = d_arcsec <= arcsec_radius
        if not within.any():
            continue

        j = int(np.argmin(d_arcsec))
        hit = sub.iloc[j].to_dict()
        hit["sep_arcsec"]   = float(d_arcsec[j])
        hit["opt_source_id"] = str(opt_id)
        hit["opt_ra_deg"]    = ra0
        hit["opt_dec_deg"]   = dec0
        hit["healpix_k5"]    = int(k5_pixel)
        results.append(hit)

    if not results:
        empty = pa.Table.from_arrays([pa.array([], type=pa.string())], names=["__empty__"])
        return empty.drop_columns(["__empty__"])

    out_df = pd.DataFrame(results)
    # Ensure all schema fields exist (fill missing optionals with NaNs / proper dtypes)
    sch = result_schema()
    tbl = pa.Table.from_pandas(out_df, preserve_index=False)
    tbl = cast_table_to_schema(tbl, sch)
    return tbl


# -----------------------------------------
# Shard listing, resume & finalization I/O
# -----------------------------------------
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


# -------------
# CLI & driver
# -------------
def parse_years_arg(years_arg: str) -> List[str]:
    env = os.environ.get("NEOWISE_YEARS", "").strip()
    if not years_arg and env:
        years_arg = env
    if not years_arg:
        return [f"year{y}" for y in range(1, 12)]  # year1..year11
    return [p.strip() for p in years_arg.replace(",", " ").split() if p.strip()]

def main():
    ap = argparse.ArgumentParser(description="NEOWISE sidecar from S3 parquet (anonymous, resumable)")
    ap.add_argument("--years", type=str, default=os.environ.get("NEOWISE_YEARS", ""),
                    help='Years to process (e.g. "year8" or "year8,year9"). Default: env or year1..year11')
    ap.add_argument("--optical-root", type=str, default=DEFAULT_OPTICAL_PARQUET_ROOT,
                    help="Root folder of optical master Parquet dataset")
    ap.add_argument("--out-root", type=str, default=DEFAULT_OUT_FLAGS_ROOT,
                    help="Output root (sidecar & tmp folder live here)")
    ap.add_argument("--radius-arcsec", type=float,
                    default=float(os.environ.get("NEO_RADIUS_ARCSEC", "5.0")),
                    help="Association radius in arcsec (default 5.0, paper-consistent)")
    ap.add_argument("--columns", type=str, default="",
                    help='NEOWISE columns to read (CSV). Default minimal set.')
    ap.add_argument("--parallel", choices=["none", "pixel"], default="none",
                    help="Parallelize by k5 bin with threads ('pixel'). Default: none")
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

    years         = parse_years_arg(args.years)
    optical_root  = args.optical_root
    out_root      = args.out_root
    tmp_dir       = os.path.join(out_root, "tmp")
    os.makedirs(tmp_dir, exist_ok=True)

    arcsec_radius = float(args.radius_arcsec)
    if args.columns:
        neo_cols = [c.strip() for c in args.columns.split(",") if c.strip()]
    else:
        neo_cols = DEFAULT_NEO_COLS

    print(f"[INFO] PyArrow version: {pa.__version__}")
    # Try to identify backend for HEALPix informationally
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
    print(f"[INFO] Match radius: {arcsec_radius:.2f}\" (paper-consistent default)")
    print(f"[INFO] NEOWISE columns: {neo_cols}")

    # Load optical dataset and compute unique k5 bins
    optical_df = load_optical_positions(optical_root)
    unique_bins = list(optical_df["healpix_k5"].unique())

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

    # Worker function
    def process_one(k5: int) -> Tuple[int, int]:
        shard_path = os.path.join(tmp_dir, f"k5={k5}.parquet")
        if (not args.force) and os.path.exists(shard_path):
            return (k5, -1)  # skipped

        opt_part = optical_df[optical_df["healpix_k5"] == k5]
        if opt_part.empty:
            # write empty shard to mark as done (optional but helpful for resume)
            empty_tbl = pa.Table.from_arrays([pa.array([], type=f.type) for f in sch], names=sch.names)
            pq.write_table(empty_tbl, shard_path, compression="snappy")
            return (k5, 0)

        tbl = match_partition_to_table(opt_part, years, int(k5), arcsec_radius, neo_cols)
        if tbl.num_rows == 0:
            # write empty shard
            empty_tbl = pa.Table.from_arrays([pa.array([], type=f.type) for f in sch], names=sch.names)
            pq.write_table(empty_tbl, shard_path, compression="snappy")
            return (k5, 0)

        # Ensure exact schema then write
        tbl = cast_table_to_schema(tbl, sch)
        pq.write_table(tbl, shard_path, compression="snappy")
        return (k5, tbl.num_rows)

    processed = 0
    written   = 0
    skipped   = 0

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
                futures[ex.submit(process_one, int(k5))] = k5

            for i, fut in enumerate(as_completed(futures), 1):
                k5, rows = fut.result()
                processed += 1
                if rows >= 0:
                    written  += rows
                else:
                    skipped  += 1
                if processed % 50 == 0 or processed == len(futures):
                    print(f"[INFO] Processed {processed}/{len(futures)} bins "
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

    # Finalize unless disabled
    out_path = os.path.join(out_root, DEFAULT_OUT_FILE)
    if not args.no_finalize:
        finalize_shards(tmp_dir, out_path, sch)
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
