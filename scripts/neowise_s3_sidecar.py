#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NEOWISER sidecar via AWS S3 Parquet (anonymous) — leaf-targeted by k5
Version: 2026-01-27e (RA wrap for k5 planning, TAP-equivalent defaults, addendum included)

TAP-equivalent defaults:
 - all years (year1..year11) + addendum
 - raw ra/dec
 - radius = 5.0 arcsec
 - qual_frame > 0, qi_fact > 0, saa_sep > 0, w1snr >= 5, mjd <= 59198
 - moon_masked == '00' enforced AFTER read (push-down uses only numeric gates)
"""

import os, sys, math, glob, argparse
from typing import List, Optional, Tuple, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as pds
import pyarrow.parquet as pq
from pyarrow import fs as pafs

S3_BUCKET  = "nasa-irsa-wise"
S3_PREFIX  = "wise/neowiser/catalogs/p1bs_psd/healpix_k5"

DEFAULT_OPTICAL_PARQUET_ROOT = "./data/local-cats/_master_optical_parquet"
DEFAULT_OUT_FLAGS_ROOT       = "./data/local-cats/_master_optical_parquet_irflags"
DEFAULT_OUT_FILE             = "neowise_se_flags_ALL.parquet"

DEFAULT_NEO_COLS = [
    "cntr","source_id","ra","dec","mjd",
    "w1snr","w2snr","qual_frame","qi_fact","saa_sep","moon_masked"
]

_RADEC_PAIRS_OPT = [
    ("opt_ra_deg", "opt_dec_deg"),
    ("ra_deg", "dec_deg"),
    ("ALPHA_J2000", "DELTA_J2000"),
    ("ALPHAWIN_J2000", "DELTAWIN_J2000"),
    ("X_WORLD", "Y_WORLD"),
]

def k5_index_ra_dec(ra_deg_array: np.ndarray, dec_deg_array: np.ndarray) -> np.ndarray:
    """HEALPix NESTED order-5 index for arrays of RA/Dec in degrees (RA expected in [0,360))."""
    nside = 2 ** 5
    try:
        import healpy as hp
        theta = np.deg2rad(90.0 - np.asarray(dec_deg_array, dtype=float))
        phi   = np.deg2rad(np.asarray(ra_deg_array, dtype=float))
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
        theta, phi = hpg.lonlat_to_thetaphi(ra_deg_array, dec_deg_array, degrees=True)
        if hasattr(hpg, "thetaphi_to_healpix"):
            return hpg.thetaphi_to_healpix(theta, phi, order=5, nest=True)
        import healpy as hp
        return hp.ang2pix(nside, theta, phi, nest=True)
    except Exception:
        pass
    raise RuntimeError("HEALPix indexing failed.")

def arcsec2rad(arcsec: float) -> float: return arcsec / 206264.806

def haversine_sep_arcsec(ra0_deg: float, dec0_deg: float,
                         ra_deg: np.ndarray, dec_deg: np.ndarray) -> np.ndarray:
    d2r = np.pi / 180.0
    dra  = (ra_deg  - ra0_deg) * d2r
    ddec = (dec_deg - dec0_deg) * d2r
    a = (np.sin(ddec/2.0)**2 +
         np.cos(dec0_deg*d2r)*np.cos(dec_deg*d2r)*np.sin(dra/2.0)**2)
    return 206264.806 * 2*np.arcsin(np.sqrt(a))

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

def _choose_optical_radec(schema_names: List[str]) -> Optional[Tuple[str, str]]:
    s = set(schema_names)
    for ra_name, dec_name in _RADEC_PAIRS_OPT:
        if ra_name in s and dec_name in s:
            return ra_name, dec_name
    return None

def load_optical_positions(parquet_root: str) -> pd.DataFrame:
    """Load optical positions and compute k5 index (with RA wrapped to [0,360) for planning)."""
    if parquet_root.startswith("s3://"):
        ds = pds.dataset(
            parquet_root.replace("s3://", "", 1),
            format="parquet",
            filesystem=_mk_s3fs(anon=False),
        )
    else:
        ds = pds.dataset(parquet_root, format="parquet")

    names = ds.schema.names
    radec = _choose_optical_radec(names)
    if not radec:
        raise RuntimeError(f"Could not detect RA/Dec columns. Available: {', '.join(names)}")

    ra_name, dec_name = radec
    need = [ra_name, dec_name, "source_id"] if "source_id" in names else [ra_name, dec_name]
    if "NUMBER"   in names: need.append("NUMBER")
    if "tile_id"  in names: need.append("tile_id")
    if "image_id" in names: need.append("image_id")

    tbl = ds.to_table(columns=[c for c in need if c in names])
    df  = tbl.to_pandas()
    df  = df.rename(columns={ra_name: "opt_ra_deg", dec_name: "opt_dec_deg"})

    if "source_id" not in df.columns:
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

    # *** CRITICAL FIX: wrap RA to [0,360) **only** for k5 planning ***
    ra_mod = (df["opt_ra_deg"].astype(float) % 360.0).to_numpy()
    dec    = df["opt_dec_deg"].astype(float).to_numpy()
    df["healpix_k5"] = k5_index_ra_dec(ra_mod, dec)
    return df[["source_id","opt_ra_deg","opt_dec_deg","healpix_k5"]]

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
    for f in schema:
        if f.name in tbl.column_names:
            col = tbl[f.name]
            if not col.type.equals(f.type):
                try: col = pc.cast(col, f.type)
                except Exception: col = pa.nulls(tbl.num_rows, type=f.type)
            arrays.append(col); names.append(f.name)
        else:
            arrays.append(pa.nulls(tbl.num_rows, type=f.type)); names.append(f.name)
    return pa.Table.from_arrays(arrays, names=names)

def _bbox_filter_for_ra_dec(opt_part_df: pd.DataFrame, arcsec_radius: float):
    delta_deg = math.degrees(arcsec2rad(arcsec_radius))
    ra_vals   = opt_part_df["opt_ra_deg"].values % 360.0
    dec_vals  = opt_part_df["opt_dec_deg"].values
    ra_min = float(np.min(ra_vals))  - delta_deg
    ra_max = float(np.max(ra_vals))  + delta_deg
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

def _tap_pushdown_filter():
    # Only numeric gates pushed down; moon_masked handled post-read
    return (
        (pc.field("qual_frame") > pc.scalar(0)) &
        (pc.field("qi_fact")    > pc.scalar(0.0)) &
        (pc.field("saa_sep")    > pc.scalar(0.0)) &
        (pc.field("w1snr")      >= pc.scalar(5.0)) &
        (pc.field("mjd")        <= pc.scalar(59198.0))
    )

def parse_years_arg(years_arg: str) -> List[str]:
    env = os.environ.get("NEOWISE_YEARS", "").strip()
    if not years_arg and env:
        years_arg = env
    if not years_arg:
        return [f"year{y}" for y in range(1, 12)] + ["addendum"]
    return [p.strip() for p in years_arg.replace(",", " ").split() if p.strip()]

def match_k5(opt_part_df: pd.DataFrame,
             years: Iterable[str],
             arcsec_radius: float,
             neo_cols: List[str]) -> pa.Table:
    sch = result_schema()
    if opt_part_df.empty:
        return pa.Table.from_arrays([pa.array([], type=f.type) for f in sch], names=sch.names)

    fs     = _mk_s3fs(anon=True)
    bbox_f = _bbox_filter_for_ra_dec(opt_part_df, arcsec_radius)
    tap_f  = _tap_pushdown_filter()

    neo_frames: List[pd.DataFrame] = []
    for yr in years:
        leaf = _irsa_year_leaf_path(yr, int(opt_part_df["healpix_k5"].iloc[0]))
        if not _leaf_exists(fs, leaf):
            print(f"[WARN] Missing leaf for {yr}: {leaf}"); continue

        ds_leaf = pds.dataset(leaf, format="parquet", filesystem=fs, partitioning="hive",
                              exclude_invalid_files=True)
        fields   = set(ds_leaf.schema.names)
        required = ["ra","dec","mjd","source_id","cntr"]
        want     = list(dict.fromkeys(required + [c for c in neo_cols if c in fields]))
        have     = [c for c in want if c in fields]
        if not set(required).issubset(have):
            print(f"[WARN] Missing required columns in {yr}: {leaf}"); continue

        tbl  = ds_leaf.to_table(filter=bbox_f & tap_f, columns=have)

        # Post-read normalization for moon_masked ('00' logical)
        if "moon_masked" in tbl.column_names and tbl.num_rows > 0:
            mm_col = tbl["moon_masked"]
            mm_num0 = None
            try:
                mm_num0 = pc.equal(pc.cast(mm_col, pa.int64(), safe=False), pa.scalar(0, pa.int64()))
            except Exception:
                mm_num0 = None
            mm_str  = pc.cast(mm_col, pa.utf8(), safe=False)
            mm_eq00 = pc.equal(mm_str, pa.scalar("00"))
            mm_eq0  = pc.equal(mm_str, pa.scalar("0"))
            keep    = mm_eq00 if mm_num0 is None else pc.or_(mm_eq00, mm_num0)
            keep    = pc.or_(keep, mm_eq0)
            tbl     = tbl.filter(keep)

        if tbl.num_rows == 0: continue
        neo_frames.append(tbl.to_pandas())

    if not neo_frames:
        return pa.Table.from_arrays([pa.array([], type=f.type) for f in sch], names=sch.names)

    neo_df  = pd.concat(neo_frames, ignore_index=True)
    neo_ra  = neo_df["ra"].values
    neo_dec = neo_df["dec"].values
    delta_deg = math.degrees(arcsec2rad(arcsec_radius))

    out_rows = []
    for _, row in opt_part_df.iterrows():
        ra0  = float(row["opt_ra_deg"]); dec0 = float(row["opt_dec_deg"])
        opt_id = row["source_id"]
        m = ((neo_ra  >= ra0 - delta_deg) & (neo_ra  <= ra0 + delta_deg) &
             (neo_dec >= dec0 - delta_deg) & (neo_dec <= dec0 + delta_deg))
        if not m.any(): continue
        sub = neo_df.loc[m, :]
        d_arcsec = haversine_sep_arcsec(ra0, dec0, sub["ra"].values, sub["dec"].values)
        within   = d_arcsec <= arcsec_radius
        if not within.any(): continue
        j   = int(np.argmin(d_arcsec))
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
    out    = pa.Table.from_pandas(out_df, preserve_index=False)
    return cast_table_to_schema(out, sch)

def existing_k5_in_tmp(tmp_dir: str) -> set:
    out = set()
    for path in glob.glob(os.path.join(tmp_dir, "k5=*.parquet")):
        base = os.path.basename(path)
        try: out.add(int(base.split("=")[1].split(".")[0]))
        except Exception: continue
    return out

def finalize_shards(tmp_dir: str, out_path: str, schema: pa.Schema):
    shard_paths = sorted(glob.glob(os.path.join(tmp_dir, "k5=*.parquet")))
    if not shard_paths:
        print("[INFO] No shard files; skipping finalize."); return
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

def main():
    ap = argparse.ArgumentParser(description="NEOWISER S3 sidecar (leaf-only, TAP-equivalent defaults)")
    ap.add_argument("--years", type=str, default=os.environ.get("NEOWISE_YEARS", ""))
    ap.add_argument("--optical-root", type=str, default=DEFAULT_OPTICAL_PARQUET_ROOT)
    ap.add_argument("--out-root",     type=str, default=DEFAULT_OUT_FLAGS_ROOT)
    ap.add_argument("--radius-arcsec", type=float, default=5.0)
    ap.add_argument("--parallel", choices=["none","pixel"], default="none")
    ap.add_argument("--workers",  type=int, default=0)
    ap.add_argument("--no-finalize", action="store_true")
    ap.add_argument("--force",        action="store_true")
    ap.add_argument("--k5-limit",     type=int, default=0)
    ap.add_argument("--k5-include",   type=str, default="")
    args = ap.parse_args()

    years       = parse_years_arg(args.years)
    optical_root= args.optical_root
    out_root    = args.out_root
    tmp_dir     = os.path.join(out_root, "tmp")
    os.makedirs(tmp_dir, exist_ok=True)

    print(f"[INFO] PyArrow: {pa.__version__}")
    print(f"[INFO] NEOWISER years: {years}")
    print(f"[INFO] Radius: {args.radius_arcsec:.2f}\"")
    print(f"[INFO] Optical root: {optical_root}")
    print(f"[INFO] Output root:  {out_root}")

    optical_df = load_optical_positions(optical_root)
    if optical_df.empty:
        print("[WARN] Optical dataset has 0 rows."); sys.exit(0)

    bins = sorted(map(int, pd.unique(optical_df["healpix_k5"])))
    if args.k5_include:
        if os.path.exists(args.k5_include):
            with open(args.k5_include) as f:
                include = {int(x.strip()) for x in f if x.strip()}
        else:
            include = {int(x) for x in args.k5_include.replace(",", " ").split() if x.strip()}
        bins = [b for b in bins if b in include]
        print(f"[INFO] --k5-include: intersection={len(bins)}")

    if not bins:
        print("[WARN] 0 bins remain after include filter."); sys.exit(0)

    if args.k5_limit and args.k5_limit < len(bins):
        bins = bins[:args.k5_limit]
        print(f"[INFO] Limiting to first {len(bins)} bins.")

    print(f"[INFO] k5 bins to process: {len(bins)}")

    to_skip = set() if args.force else existing_k5_in_tmp(tmp_dir)
    if to_skip and not args.force:
        print(f"[INFO] Resume: skipping {len(to_skip)} existing shards.")

    sch = result_schema()

    def process_one(k5: int) -> Tuple[int,int]:
        shard = os.path.join(tmp_dir, f"k5={k5}.parquet")
        if (not args.force) and os.path.exists(shard):
            return (k5, -1)
        part = optical_df[optical_df["healpix_k5"] == k5]
        if part.empty:
            empty = pa.Table.from_arrays([pa.array([], type=f.type) for f in sch], names=sch.names)
            pq.write_table(empty, shard, compression="snappy"); return (k5, 0)
        tbl = match_k5(part, years, args.radius_arcsec, DEFAULT_NEO_COLS)
        if tbl.num_rows == 0:
            tbl = pa.Table.from_arrays([pa.array([], type=f.type) for f in sch], names=sch.names)
        elif tbl.schema != sch:
            tbl = cast_table_to_schema(tbl, sch)
        pq.write_table(tbl, shard, compression="snappy")
        return (k5, tbl.num_rows)

    processed = written = skipped = 0
    if args.parallel == "pixel":
        import multiprocessing as mp
        W = args.workers if args.workers > 0 else min(8, (mp.cpu_count() or 8))
        print(f"[INFO] Parallel=pixel, workers={W}")
        futs = {}
        with ThreadPoolExecutor(max_workers=W) as ex:
            for k5 in bins:
                if (not args.force) and (k5 in to_skip):
                    skipped += 1; continue
                futs[ex.submit(process_one, int(k5))] = k5
            for i, fut in enumerate(as_completed(futs), 1):
                k5, rows = fut.result()
                processed += 1
                if rows >= 0: written += rows
                else:         skipped += 1
                if processed % 50 == 0 or processed == len(futs):
                    print(f"[INFO] {processed}/{len(futs)} (skipped={skipped}, rows={written})")
    else:
        for k5 in bins:
            if (not args.force) and (k5 in to_skip):
                skipped += 1; continue
            _, rows = process_one(int(k5))
            processed += 1
            if rows >= 0: written += rows
            else:         skipped += 1
            if processed % 50 == 0 or processed == len(bins):
                print(f"[INFO] {processed}/{len(bins)} (skipped={skipped}, rows={written})")

    print(f"[INFO] Completed bins: {processed}, skipped: {skipped}, total rows: {written}")
    out_path = os.path.join(out_root, DEFAULT_OUT_FILE)
    if not args.no_finalize:
        finalize_shards(tmp_dir, out_path, sch)
    else:
        print("[INFO] Finalize disabled.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[WARN] Interrupted; shards safe to resume."); sys.exit(130)
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr); sys.exit(1)
