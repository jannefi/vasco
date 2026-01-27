#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NEOWISE sidecar via AWS S3 Parquet (anonymous) — Strategy A: leaf-targeted by k5
Version: 2026-01-27a (leaf-mode + diagnostics)

See header in previous version for background. This release adds:
- Robust parse_k5_include()
- --debug-list-bins <path>: writes ALL optical k5 bins (one per line) before filtering
- Clear logs when include filter yields 0 bins, showing a sample of available bins
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

S3_BUCKET = "nasa-irsa-wise"
S3_PREFIX = "wise/neowiser/catalogs/p1bs_psd/healpix_k5"
DEFAULT_OPTICAL_PARQUET_ROOT = "./data/local-cats/_master_optical_parquet"
DEFAULT_OUT_FLAGS_ROOT = "./data/local-cats/_master_optical_parquet_irflags"
DEFAULT_OUT_FILE = "neowise_se_flags_ALL.parquet"

DEFAULT_NEO_COLS = [
    "cntr", "source_id", "ra", "dec", "mjd",
    "w1flux", "w1sigflux", "w2flux", "w2sigflux",
    "w1snr", "w2snr", "qual_frame", "qi_fact", "saa_sep", "moon_masked",
]

_RADEC_PAIRS = [
    ("opt_ra_deg", "opt_dec_deg"),
    ("ra_deg", "dec_deg"),
    ("ALPHA_J2000", "DELTA_J2000"),
    ("ALPHAWIN_J2000", "DELTAWIN_J2000"),
    ("X_WORLD", "Y_WORLD"),
]

def k5_index_ra_dec(ra_deg_array: np.ndarray, dec_deg_array: np.ndarray) -> np.ndarray:
    nside = 2 ** 5
    try:
        import healpy as hp
        theta = np.deg2rad(90.0 - np.asarray(dec_deg_array, dtype=float))
        phi = np.deg2rad(np.asarray(ra_deg_array, dtype=float))
        return hp.ang2pix(nside, theta, phi, nest=True)
    except Exception:
        pass
    try:
        from astropy_healpix import HEALPix
        HPX = HEALPix(nside=nside, order='nested')
        return HPX.lonlat_to_healpix(np.deg2rad(ra_deg_array), np.deg2rad(dec_deg_array))
    except Exception:
        pass
    try:
        import hpgeom as hpg
        try:
            theta, phi = hpg.lonlat_to_thetaphi(ra_deg_array, dec_deg_array, degrees=True)
        except TypeError:
            theta, phi = hpg.lonlat_to_thetaphi(np.deg2rad(ra_deg_array), np.deg2rad(dec_deg_array))
        if hasattr(hpg, "thetaphi_to_healpix"):
            return hpg.thetaphi_to_healpix(theta, phi, order=5, nest=True)
        import healpy as hp
        return hp.ang2pix(nside, theta, phi, nest=True)
    except Exception:
        pass
    raise RuntimeError("HEALPix indexing failed (healpy/astropy-healpix/hpgeom).")

def arcsec2rad(arcsec: float) -> float: return arcsec / 206264.806
def rad2arcsec(rad: np.ndarray) -> np.ndarray: return rad * 206264.806

def haversine_sep_arcsec(ra0_deg: float, dec0_deg: float,
                         ra_deg: np.ndarray, dec_deg: np.ndarray) -> np.ndarray:
    d2r = np.pi / 180.0
    dra = (ra_deg - ra0_deg) * d2r
    ddec = (dec_deg - dec0_deg) * d2r
    a = (np.sin(ddec / 2.0) ** 2
         + np.cos(dec0_deg * d2r) * np.cos(dec_deg * d2r) * np.sin(dra / 2.0) ** 2)
    return rad2arcsec(2.0 * np.arcsin(np.sqrt(a)))

def _mk_s3fs(anon: bool) -> pafs.S3FileSystem:
    return pafs.S3FileSystem(anonymous=anon, region="us-west-2")

def _irsa_year_leaf_path(year: str, k5: int) -> str:
    k0 = k5 // 1024
    return (f"{S3_BUCKET}/{S3_PREFIX}/{year}/"
            f"neowiser-healpix_k5-{year}.parquet/healpix_k0={k0}/healpix_k5={k5}/")

def _leaf_exists(fs: pafs.S3FileSystem, path: str) -> bool:
    try:
        return fs.get_file_info([path])[0].type == pafs.FileType.Directory
    except Exception:
        return False

def _choose_radec(schema_names: List[str]) -> Optional[Tuple[str, str]]:
    s = set(schema_names)
    for ra_name, dec_name in _RADEC_PAIRS:
        if ra_name in s and dec_name in s:
            return ra_name, dec_name
    return None

def load_optical_positions(parquet_root: str) -> pd.DataFrame:
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
    if have_source_id: need.append("source_id")
    else:
        for c in ("NUMBER", "tile_id", "image_id"):
            if c in names: need.append(c)

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

    if df.empty:
        return df.assign(healpix_k5=pd.Series(dtype=np.int32))

    df["healpix_k5"] = k5_index_ra_dec(df["opt_ra_deg"].values, df["opt_dec_deg"].values)
    keep = ["source_id", "opt_ra_deg", "opt_dec_deg", "healpix_k5"]
    if "ra_bin" in df.columns: keep.append("ra_bin")
    if "dec_bin" in df.columns: keep.append("dec_bin")
    return df[keep]

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
            arrays.append(col); names.append(name)
        else:
            arrays.append(pa.nulls(tbl.num_rows, type=field.type)); names.append(name)
    return pa.Table.from_arrays(arrays, names=names)

def _make_bbox_filter_for_pixel(opt_part_df: pd.DataFrame, arcsec_radius: float):
    delta_deg = math.degrees(arcsec2rad(arcsec_radius))
    ra_vals = opt_part_df["opt_ra_deg"].values % 360.0
    dec_vals = opt_part_df["opt_dec_deg"].values
    ra_min = float(np.min(ra_vals)) - delta_deg
    ra_max = float(np.max(ra_vals)) + delta_deg
    dec_min = float(np.min(dec_vals)) - delta_deg
    dec_max = float(np.max(dec_vals)) + delta_deg
    ra = pc.field("ra"); dec = pc.field("dec")
    if ra_min < 0.0:
        f_ra = ((ra >= 0.0) & (ra <= ra_max)) | (ra >= (ra_min + 360.0))
    elif ra_max >= 360.0:
        f_ra = ((ra >= ra_min) & (ra < 360.0)) | (ra <= (ra_max - 360.0))
    else:
        f_ra = (ra >= ra_min) & (ra <= ra_max)
    f_dec = (dec >= dec_min) & (dec <= dec_max)
    return f_ra & f_dec

def match_k5_with_leaf_reads(opt_part_df: pd.DataFrame,
                             years: Iterable[str],
                             arcsec_radius: float,
                             neo_cols: List[str]) -> pa.Table:
    if opt_part_df.empty:
        sch = result_schema()
        return pa.Table.from_arrays([pa.array([], type=f.type) for f in sch], names=sch.names)

    fs = _mk_s3fs(anon=True)
    filt = _make_bbox_filter_for_pixel(opt_part_df, arcsec_radius)

    required = ["ra", "dec", "mjd", "source_id", "cntr"]
    optional = ["w1flux", "w1sigflux", "w2flux", "w2sigflux",
                "w1snr", "w2snr", "qual_frame", "qi_fact", "saa_sep", "moon_masked"]
    want = list(dict.fromkeys(required + list(neo_cols) + optional))

    neo_frames: List[pd.DataFrame] = []
    cols_found: Optional[List[str]] = None

    for yr in years:
        leaf = _irsa_year_leaf_path(yr, int(opt_part_df["healpix_k5"].iloc[0]))
        if not _leaf_exists(fs, leaf):
            print(f"[WARN] Missing leaf for {yr}: {leaf}")
            continue

        ds_leaf = pds.dataset(leaf, format="parquet", filesystem=fs, partitioning="hive")
        have = [c for c in want if c in ds_leaf.schema.names]
        if not set(required).issubset(have):
            print(f"[WARN] Leaf lacks required columns in {yr}: {leaf} (have={have})")
            continue

        tbl = ds_leaf.to_table(filter=filt, columns=have)
        if tbl.num_rows == 0:
            continue

        if cols_found is None:
            cols_found = have
        neo_frames.append(tbl.to_pandas())

    sch = result_schema()
    if not neo_frames:
        return pa.Table.from_arrays([pa.array([], type=f.type) for f in sch], names=sch.names)

    neo_df = pd.concat(neo_frames, ignore_index=True)
    neo_ra = neo_df["ra"].values; neo_dec = neo_df["dec"].values
    delta_deg = math.degrees(arcsec2rad(arcsec_radius))
    out_rows = []
    have_cols = set(neo_df.columns)
    cols_proj = [c for c in (cols_found or want) if c in have_cols]

    for _, row in opt_part_df.iterrows():
        ra0 = float(row["opt_ra_deg"]); dec0 = float(row["opt_dec_deg"])
        opt_id = row["source_id"]
        m = ((neo_ra >= ra0 - delta_deg) & (neo_ra <= ra0 + delta_deg) &
             (neo_dec >= dec0 - delta_deg) & (neo_dec <= dec0 + delta_deg))
        if not m.any(): continue
        sub = neo_df.loc[m, cols_proj]
        d_arcsec = haversine_sep_arcsec(ra0, dec0, sub["ra"].values, sub["dec"].values)
        within = d_arcsec <= arcsec_radius
        if not within.any(): continue
        j = int(np.argmin(d_arcsec))
        hit = sub.iloc[j].to_dict()
        hit.update({
            "sep_arcsec": float(d_arcsec[j]),
            "opt_source_id": str(opt_id),
            "opt_ra_deg": ra0,
            "opt_dec_deg": dec0,
            "healpix_k5": int(row["healpix_k5"]),
        })
        out_rows.append(hit)

    if not out_rows:
        return pa.Table.from_arrays([pa.array([], type=f.type) for f in sch], names=sch.names)

    out_df = pd.DataFrame(out_rows)
    out = pa.Table.from_pandas(out_df, preserve_index=False)
    return cast_table_to_schema(out, sch)

def parse_k5_include(arg: str) -> Optional[set]:
    """Return a set of k5 ints. Accepts '5318,8277' or a file path with one k5 per line."""
    if not arg: return None
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
            if tbl.schema != schema: tbl = cast_table_to_schema(tbl, schema)
            writer.write_table(tbl)
    finally:
        writer.close()
    print(f"[DONE] Finalized {len(shard_paths)} shards → {out_path}")

def parse_years_arg(years_arg: str) -> List[str]:
    env = os.environ.get("NEOWISE_YEARS", "").strip()
    if not years_arg and env: years_arg = env
    if not years_arg: return [f"year{y}" for y in range(1, 12)]
    return [p.strip() for p in years_arg.replace(",", " ").split() if p.strip()]

def main():
    ap = argparse.ArgumentParser(description="NEOWISE sidecar (IRSA S3 parquet, Strategy-A leaf-only)")
    ap.add_argument("--years", type=str, default=os.environ.get("NEOWISE_YEARS", ""))
    ap.add_argument("--optical-root", type=str, default=DEFAULT_OPTICAL_PARQUET_ROOT)
    ap.add_argument("--out-root", type=str, default=DEFAULT_OUT_FLAGS_ROOT)
    ap.add_argument("--radius-arcsec", type=float, default=float(os.environ.get("NEO_RADIUS_ARCSEC", "5.0")))
    ap.add_argument("--columns", type=str, default="")
    ap.add_argument("--parallel", choices=["none", "pixel"], default="none")
    ap.add_argument("--workers", type=int, default=0)
    ap.add_argument("--no-finalize", action="store_true")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--k5-limit", type=int, default=0)
    ap.add_argument("--k5-include", type=str, default="")
    ap.add_argument("--debug-list-bins", type=str, default="",
                    help="Write optical k5 bins to this file, one per line, before filtering")
    args = ap.parse_args()

    years = parse_years_arg(args.years)
    optical_root = args.optical_root
    out_root = args.out_root
    tmp_dir = os.path.join(out_root, "tmp")
    os.makedirs(tmp_dir, exist_ok=True)

    arcsec_radius = float(args.radius_arcsec)
    neo_cols = [c.strip() for c in args.columns.split(",") if c.strip()] if args.columns else DEFAULT_NEO_COLS

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

    # Load optical and compute k5
    optical_df = load_optical_positions(optical_root)
    if optical_df.empty:
        print("[WARN] Optical parquet loaded but contains 0 rows. Check --optical-root.")
        print("[INFO] Exiting without work.")
        sys.exit(0)

    optical_bins = sorted(map(int, pd.unique(optical_df["healpix_k5"])))
    if args.debug_list_bins:
        with open(args.debug_list_bins, "w") as f:
            for b in optical_bins: f.write(f"{b}\n")
        print(f"[INFO] Wrote {len(optical_bins)} optical k5 bins to {args.debug_list_bins}")

    unique_bins = optical_bins[:]  # start from all optical bins
    include_set = parse_k5_include(args.k5_include)
    if include_set is not None:
        before = len(unique_bins)
        unique_bins = [b for b in unique_bins if b in include_set]
        print(f"[INFO] --k5-include provided {len(include_set)} bins; "
              f"optical intersection = {len(unique_bins)}")
        if len(unique_bins) == 0:
            # Help the user by showing first few optical bins
            preview = ", ".join(map(str, optical_bins[:20]))
            print("[WARN] After applying --k5-include, there are 0 bins to process.")
            print(f"[HINT] Your optical delta currently spans {len(optical_bins)} bins; "
                  f"first 20 bins: {preview}")
    if args.k5_limit and args.k5_limit < len(unique_bins):
        unique_bins = unique_bins[:args.k5_limit]
        print(f"[INFO] Limiting to first {len(unique_bins)} k5 bins.")
    print(f"[INFO] Optical unique k5 bins to process: {len(unique_bins)}")

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
        if tbl.num_rows == 0:
            tbl = pa.Table.from_arrays([pa.array([], type=f.type) for f in sch], names=sch.names)
        elif tbl.schema != sch:
            tbl = cast_table_to_schema(tbl, sch)
        pq.write_table(tbl, shard_path, compression="snappy")
        return (k5, tbl.num_rows)

    processed = written = skipped = 0

    if args.parallel == "pixel":
        import multiprocessing as mp
        max_workers = args.workers if args.workers > 0 else min(8, (mp.cpu_count() or 8))
        print(f"[INFO] Parallel mode: pixel, workers={max_workers}")
        futs = {}
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            for k5 in unique_bins:
                if (not args.force) and (k5 in to_skip):
                    skipped += 1; continue
                futs[ex.submit(process_one, int(k5))] = k5
            for i, fut in enumerate(as_completed(futs), 1):
                k5, rows = fut.result()
                processed += 1
                if rows >= 0: written += rows
                else: skipped += 1
                if processed % 50 == 0 or processed == len(futs):
                    print(f"[INFO] Processed {processed}/{len(futs)} bins "
                          f"(skipped={skipped}, rows_written={written})")
    else:
        for k5 in unique_bins:
            if (not args.force) and (k5 in to_skip):
                skipped += 1; continue
            _, rows = process_one(int(k5))
            processed += 1
            if rows >= 0: written += rows
            else: skipped += 1
            if processed % 50 == 0 or processed == len(unique_bins):
                print(f"[INFO] Processed {processed}/{len(unique_bins)} bins "
                      f"(skipped={skipped}, rows_written={written})")

    print(f"[INFO] Completed bins: {processed}, skipped: {skipped}, total rows written: {written}")

    out_path = os.path.join(out_root, DEFAULT_OUT_FILE)
    if not args.no_finalize:
        finalize_shards(os.path.join(out_root, "tmp"), out_path, sch)
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