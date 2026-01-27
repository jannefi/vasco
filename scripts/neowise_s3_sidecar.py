#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NEOWISER sidecar via AWS S3 Parquet (anonymous) — leaf-targeted by k5
Version: 2026-01-27f (RA-wrap fix in per-row filter; addendum default; post-read moon_masked)

TAP-equivalent defaults:
 - years = year1..year11 + addendum
 - raw ra/dec, radius = 5.0 arcsec
 - gates: qual_frame>0, qi_fact>0, saa_sep>0, w1snr>=5, mjd<=59198
 - moon_masked == '00' enforced AFTER read (push down uses numeric-only gates)
"""
import os, sys, math, glob, argparse
from typing import List, Optional, Tuple, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np, pandas as pd
import pyarrow as pa, pyarrow.compute as pc, pyarrow.dataset as pds, pyarrow.parquet as pq
from pyarrow import fs as pafs

S3_BUCKET = "nasa-irsa-wise"
S3_PREFIX = "wise/neowiser/catalogs/p1bs_psd/healpix_k5"

DEFAULT_OPTICAL_PARQUET_ROOT = "./data/local-cats/_master_optical_parquet"
DEFAULT_OUT_FLAGS_ROOT       = "./data/local-cats/_master_optical_parquet_irflags"
DEFAULT_OUT_FILE             = "neowise_se_flags_ALL.parquet"

DEFAULT_NEO_COLS = ["cntr","source_id","ra","dec","mjd","w1snr","w2snr","qual_frame","qi_fact","saa_sep","moon_masked"]

_RADEC_PAIRS_OPT = [
    ("opt_ra_deg", "opt_dec_deg"),
    ("ra_deg", "dec_deg"),
    ("ALPHA_J2000", "DELTA_J2000"),
    ("ALPHAWIN_J2000", "DELTAWIN_J2000"),
    ("X_WORLD", "Y_WORLD"),
]

def k5_index_ra_dec(ra_deg_array: np.ndarray, dec_deg_array: np.ndarray) -> np.ndarray:
    nside = 2**5
    try:
        import healpy as hp
        theta = np.deg2rad(90.0 - np.asarray(dec_deg_array, float))
        phi   = np.deg2rad(np.asarray(ra_deg_array, float))
        return hp.ang2pix(nside, theta, phi, nest=True)
    except Exception:
        pass
    try:
        from astropy_healpix import HEALPix
        return HEALPix(nside=nside, order='nested').lonlat_to_healpix(
            np.deg2rad(ra_deg_array), np.deg2rad(dec_deg_array))
    except Exception:
        pass
    try:
        import hpgeom as hpg, healpy as hp
        th, ph = hpg.lonlat_to_thetaphi(ra_deg_array, dec_deg_array, degrees=True)
        return hpg.thetaphi_to_healpix(th, ph, order=5, nest=True) if hasattr(hpg,"thetaphi_to_healpix") else hp.ang2pix(nside, th, ph, nest=True)
    except Exception:
        pass
    raise RuntimeError("HEALPix indexing failed.")

def arcsec2rad(arcsec: float) -> float: return arcsec / 206264.806

def haversine_sep_arcsec(ra0: float, dec0: float, ra: np.ndarray, dec: np.ndarray) -> np.ndarray:
    d2r = np.pi/180.0
    dra = (ra - ra0)*d2r; ddc = (dec - dec0)*d2r
    a = (np.sin(ddc/2)**2 + np.cos(dec0*d2r)*np.cos(dec*d2r)*np.sin(dra/2)**2)
    return 206264.806 * 2*np.arcsin(np.sqrt(a))

def _mk_s3fs(anon: bool) -> pafs.S3FileSystem: return pafs.S3FileSystem(anonymous=anon, region="us-west-2")

def _irsa_year_leaf_path(year: str, k5: int) -> str:
    k0 = k5 // 1024
    return f"{S3_BUCKET}/{S3_PREFIX}/{year}/neowiser-healpix_k5-{year}.parquet/healpix_k0={k0}/healpix_k5={k5}/"

def _leaf_exists(fs: pafs.S3FileSystem, path: str) -> bool:
    try: return fs.get_file_info([path])[0].type == pafs.FileType.Directory
    except Exception: return False

def _choose_optical_radec(names: List[str]) -> Optional[Tuple[str,str]]:
    s = set(names)
    for ra, dec in _RADEC_PAIRS_OPT:
        if ra in s and dec in s: return ra, dec
    return None

def load_optical_positions(root: str) -> pd.DataFrame:
    if root.startswith("s3://"):
        ds = pds.dataset(root.replace("s3://","",1), format="parquet", filesystem=_mk_s3fs(False))
    else:
        ds = pds.dataset(root, format="parquet")
    names = ds.schema.names
    pair = _choose_optical_radec(names)
    if not pair: raise RuntimeError(f"Could not detect RA/Dec columns. Available: {', '.join(names)}")
    ra_name, dec_name = pair
    need = [ra_name, dec_name, "source_id"] if "source_id" in names else [ra_name, dec_name]
    for extra in ("NUMBER","tile_id","image_id"):
        if extra in names: need.append(extra)
    df = ds.to_table(columns=[c for c in need if c in names]).to_pandas()
    df = df.rename(columns={ra_name:"opt_ra_deg", dec_name:"opt_dec_deg"})
    if "source_id" not in df.columns:
        if "NUMBER" in df.columns:
            if "tile_id" in df.columns:  df["source_id"] = df["tile_id"].astype(str) + "#" + df["NUMBER"].astype(str)
            elif "image_id" in df.columns: df["source_id"] = df["image_id"].astype(str) + "#" + df["NUMBER"].astype(str)
            else: df["source_id"] = df["NUMBER"].astype(str)
        else:
            df["source_id"] = df.index.astype(str)
    if df.empty: return df.assign(healpix_k5=pd.Series(dtype=np.int32))
    ra_mod = (df["opt_ra_deg"].astype(float) % 360.0).to_numpy()   # RA wrap for planning
    dec    = df["opt_dec_deg"].astype(float).to_numpy()
    df["healpix_k5"] = k5_index_ra_dec(ra_mod, dec)
    return df[["source_id","opt_ra_deg","opt_dec_deg","healpix_k5"]]

def result_schema() -> pa.Schema:
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

def cast_table_to_schema(tbl: pa.Table, sch: pa.Schema) -> pa.Table:
    arrays, names = [], []
    for f in sch:
        if f.name in tbl.column_names:
            col = tbl[f.name]
            if not col.type.equals(f.type):
                try: col = pc.cast(col, f.type)
                except Exception: col = pa.nulls(tbl.num_rows, type=f.type)
            arrays.append(col); names.append(f.name)
        else:
            arrays.append(pa.nulls(tbl.num_rows, type=f.type)); names.append(f.name)
    return pa.Table.from_arrays(arrays, names=names)

def _bbox_filter_for_ra_dec(opt_df: pd.DataFrame, arcsec_radius: float):
    ddeg = math.degrees(arcsec2rad(arcsec_radius))
    ra_vals  = opt_df["opt_ra_deg"].values % 360.0
    dec_vals = opt_df["opt_dec_deg"].values
    ra_min, ra_max = float(np.min(ra_vals)) - ddeg, float(np.max(ra_vals)) + ddeg
    dec_min, dec_max = float(np.min(dec_vals)) - ddeg, float(np.max(dec_vals)) + ddeg
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
    return ((pc.field("qual_frame") > pc.scalar(0)) &
            (pc.field("qi_fact")    > pc.scalar(0.0)) &
            (pc.field("saa_sep")    > pc.scalar(0.0)) &
            (pc.field("w1snr")      >= pc.scalar(5.0)) &
            (pc.field("mjd")        <= pc.scalar(59198.0)))

def parse_years_arg(years_arg: str) -> List[str]:
    env = os.environ.get("NEOWISE_YEARS","").strip()
    if not years_arg and env: years_arg = env
    if not years_arg: return [f"year{y}" for y in range(1,12)] + ["addendum"]
    return [p.strip() for p in years_arg.replace(","," ").split() if p.strip()]

def match_k5(opt_part_df: pd.DataFrame, years: Iterable[str], arcsec_radius: float, neo_cols: List[str]) -> pa.Table:
    sch = result_schema()
    if opt_part_df.empty:
        return pa.Table.from_arrays([pa.array([], type=f.type) for f in sch], names=sch.names)

    fs     = _mk_s3fs(True)
    bbox_f = _bbox_filter_for_ra_dec(opt_part_df, arcsec_radius)
    tap_f  = _tap_pushdown_filter()
    neo_frames = []

    for yr in years:
        leaf = _irsa_year_leaf_path(yr, int(opt_part_df["healpix_k5"].iloc[0]))
        if not _leaf_exists(fs, leaf):
            print(f"[WARN] Missing leaf for {yr}: {leaf}"); continue
        ds_leaf = pds.dataset(leaf, format="parquet", filesystem=fs, partitioning="hive", exclude_invalid_files=True)
        fields   = set(ds_leaf.schema.names)
        required = ["ra","dec","mjd","source_id","cntr"]
        want     = list(dict.fromkeys(required + [c for c in neo_cols if c in fields]))
        have     = [c for c in want if c in fields]
        if not set(required).issubset(have):
            print(f"[WARN] Missing required columns in {yr}: {leaf}"); continue

        tbl = ds_leaf.to_table(filter=bbox_f & tap_f, columns=have)

        # Post-read normalization: moon_masked logically '00'
        if "moon_masked" in tbl.column_names and tbl.num_rows > 0:
            mm = tbl["moon_masked"]
            mm_num0 = None
            try: mm_num0 = pc.equal(pc.cast(mm, pa.int64(), safe=False), pa.scalar(0, pa.int64()))
            except Exception: mm_num0 = None
            mm_str  = pc.cast(mm, pa.utf8(), safe=False)
            keep = pc.equal(mm_str, pa.scalar("00"))
            keep = pc.or_(keep, pc.equal(mm_str, pa.scalar("0")))
            if mm_num0 is not None: keep = pc.or_(keep, mm_num0)
            tbl = tbl.filter(keep)

        if tbl.num_rows == 0: continue
        neo_frames.append(tbl.to_pandas())

    if not neo_frames:
        return pa.Table.from_arrays([pa.array([], type=f.type) for f in sch], names=sch.names)

    neo_df  = pd.concat(neo_frames, ignore_index=True)
    neo_ra  = neo_df["ra"].values
    neo_dec = neo_df["dec"].values
    ddeg    = math.degrees(arcsec2rad(arcsec_radius))

    out_rows = []
    for _, row in opt_part_df.iterrows():
        ra0_raw = float(row["opt_ra_deg"])
        dec0    = float(row["opt_dec_deg"])
        ra0     = ra0_raw % 360.0  # --- RA wrap for per-row prefilter ---
        # wrap-aware rectangular prefilter
        ra_lo = (ra0 - ddeg) % 360.0
        ra_hi = (ra0 + ddeg) % 360.0
        if ra_lo <= ra_hi:
            m_ra = (neo_ra >= ra_lo) & (neo_ra <= ra_hi)
        else:
            m_ra = (neo_ra >= ra_lo) | (neo_ra <= ra_hi)
        m_dec = (neo_dec >= (dec0 - ddeg)) & (neo_dec <= (dec0 + ddeg))
        m = m_ra & m_dec
        if not m.any(): continue
        sub = neo_df.loc[m, :]
        d_arcsec = haversine_sep_arcsec(ra0, dec0, sub["ra"].values, sub["dec"].values)
        within   = d_arcsec <= arcsec_radius
        if not within.any(): continue
        j   = int(np.argmin(d_arcsec))
        hit = sub.iloc[j].to_dict()
        hit.update({
            "sep_arcsec": float(d_arcsec[j]),
            "opt_source_id": str(row["source_id"]),
            "opt_ra_deg": ra0_raw,     # preserve original reported RA
            "opt_dec_deg": dec0,
            "healpix_k5": int(row["healpix_k5"]),
        })
        out_rows.append(hit)

    if not out_rows:
        return pa.Table.from_arrays([pa.array([], type=f.type) for f in sch], names=sch.names)
    out_df = pd.DataFrame(out_rows)
    return cast_table_to_schema(pa.Table.from_pandas(out_df, preserve_index=False), sch)

def existing_k5_in_tmp(tmp_dir: str) -> set:
    out=set()
    for p in glob.glob(os.path.join(tmp_dir, "k5=*.parquet")):
        try: out.add(int(os.path.basename(p).split("=")[1].split(".")[0]))
        except Exception: pass
    return out

def finalize_shards(tmp_dir: str, out_path: str, sch: pa.Schema):
    parts = sorted(glob.glob(os.path.join(tmp_dir,"k5=*.parquet")))
    if not parts: print("[INFO] No shard files; skipping finalize."); return
    w = pq.ParquetWriter(out_path, schema=sch, compression="snappy")
    try:
        for p in parts:
            t = pq.read_table(p)
            if t.schema != sch: t = cast_table_to_schema(t, sch)
            w.write_table(t)
    finally:
        w.close()
    print(f"[DONE] Finalized {len(parts)} shards → {out_path}")

def main():
    ap = argparse.ArgumentParser(description="NEOWISER S3 sidecar (leaf-only, TAP-equivalent)")
    ap.add_argument("--years", type=str, default=os.environ.get("NEOWISE_YEARS",""))
    ap.add_argument("--optical-root", type=str, default=DEFAULT_OPTICAL_PARQUET_ROOT)
    ap.add_argument("--out-root",     type=str, default=DEFAULT_OUT_FLAGS_ROOT)
    ap.add_argument("--radius-arcsec", type=float, default=5.0)
    ap.add_argument("--parallel", choices=["none","pixel"], default="none")
    ap.add_argument("--workers", type=int, default=0)
    ap.add_argument("--no-finalize", action="store_true")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--k5-limit", type=int, default=0)
    ap.add_argument("--k5-include", type=str, default="")
    a = ap.parse_args()

    years = parse_years_arg(a.years)
    tmp   = os.path.join(a.out_root,"tmp"); os.makedirs(tmp, exist_ok=True)

    print(f"[INFO] PyArrow: {pa.__version__}")
    print(f"[INFO] NEOWISER years: {years}")
    print(f"[INFO] Radius: {a.radius_arcsec:.2f}\"")
    print(f"[INFO] Optical root: {a.optical_root}")
    print(f"[INFO] Output root:  {a.out_root}")

    opt = load_optical_positions(a.optical_root)
    if opt.empty: print("[WARN] Optical dataset has 0 rows."); sys.exit(0)

    bins = sorted(map(int, pd.unique(opt["healpix_k5"])))
    if a.k5_include:
        include = (set(int(x) for x in open(a.k5_include) if x.strip())
                   if os.path.exists(a.k5_include)
                   else {int(x) for x in a.k5_include.replace(","," ").split() if x.strip()})
        bins = [b for b in bins if b in include]
        print(f"[INFO] --k5-include intersection={len(bins)}")
    if not bins: print("[WARN] 0 bins remain after include filter."); sys.exit(0)
    if a.k5_limit and a.k5_limit < len(bins):
        bins = bins[:a.k5_limit]; print(f"[INFO] Limiting to first {len(bins)} bins.")
    print(f"[INFO] k5 bins to process: {len(bins)}")

    to_skip = set() if a.force else existing_k5_in_tmp(tmp)
    if to_skip and not a.force: print(f"[INFO] Resume: skipping {len(to_skip)} existing shards.")

    sch = result_schema()
    def process_one(k5: int) -> Tuple[int,int]:
        shard = os.path.join(tmp, f"k5={k5}.parquet")
        if (not a.force) and os.path.exists(shard): return (k5, -1)
        part = opt[opt["healpix_k5"] == k5]
        if part.empty:
            pq.write_table(pa.Table.from_arrays([pa.array([], type=f.type) for f in sch], names=sch.names),
                           shard, compression="snappy")
            return (k5, 0)
        t = match_k5(part, years, a.radius_arcsec, DEFAULT_NEO_COLS)
        if t.num_rows == 0: t = pa.Table.from_arrays([pa.array([], type=f.type) for f in sch], names=sch.names)
        elif t.schema != sch: t = cast_table_to_schema(t, sch)
        pq.write_table(t, shard, compression="snappy"); return (k5, t.num_rows)

    processed = written = skipped = 0
    if a.parallel == "pixel":
        import multiprocessing as mp
        W = a.workers if a.workers>0 else min(8, (mp.cpu_count() or 8))
        print(f"[INFO] Parallel=pixel, workers={W}")
        futs={}
        with ThreadPoolExecutor(max_workers=W) as ex:
            for k5 in bins:
                if (not a.force) and (k5 in to_skip): skipped += 1; continue
                futs[ex.submit(process_one, int(k5))] = k5
            for i, fut in enumerate(as_completed(futs), 1):
                k5, rows = fut.result(); processed += 1
                if rows >= 0: written += rows
                else: skipped += 1
                if processed % 50 == 0 or processed == len(futs):
                    print(f"[INFO] {processed}/{len(futs)} (skipped={skipped}, rows={written})")
    else:
        for k5 in bins:
            if (not a.force) and (k5 in to_skip): skipped += 1; continue
            _, rows = process_one(int(k5)); processed += 1
            if rows >= 0: written += rows
            else: skipped += 1
            if processed % 50 == 0 or processed == len(bins):
                print(f"[INFO] {processed}/{len(bins)} (skipped={skipped}, rows={written})")

    print(f"[INFO] Completed bins: {processed}, skipped: {skipped}, total rows: {written}")
    out_path = os.path.join(a.out_root, DEFAULT_OUT_FILE)
    if not a.no_finalize: finalize_shards(tmp, out_path, sch)
    else: print("[INFO] Finalize disabled.")

if __name__ == "__main__":
    try: main()
    except KeyboardInterrupt:
        print("\n[WARN] Interrupted; shards safe to resume."); sys.exit(130)
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr); sys.exit(1)