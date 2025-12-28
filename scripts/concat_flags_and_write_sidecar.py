
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build NEOWISE-SE IR sidecar and global flags from per-chunk 'closest' CSVs.

Inputs:
  --closest-dir   Directory containing positions*_closest.csv files
  --master-root   (kept for interface stability; not required here)
  --out-root      Output root for ALL parquet and sidecar tree
  --radius-arcsec Strict match radius in arcsec (default: 5.0)
  --bin-deg       Bin size in degrees for ra_bin/dec_bin (default: 5)
  --dataset-name  Base name for outputs (default: neowise_se)

Outputs:
  <out-root>/<dataset-name>_flags_ALL.parquet
  <out-root>/sidecar/ra_bin=XX/dec_bin=YY/part-*.parquet
  <out-root>/_SUCCESS

Notes:
  * 'Strict match' = ADQL quality gates + sep_arcsec <= radius_arcsec.
  * Important dtype fix: 'cntr' is stored as nullable Int64 (not Int32).
"""

import argparse
import glob
import math
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd


# -------------------------- dtype helpers --------------------------

def to_float32(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").astype("float32")


def to_int16(s: pd.Series) -> pd.Series:
    # nullable Int16; safe for small counters like qual_frame
    return pd.to_numeric(s, errors="coerce").astype("Int16")


def to_int32(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").astype("Int32")


def to_int64_nullable(s: pd.Series) -> pd.Series:
    # IMPORTANT: NEOWISE cntr can exceed 32-bit; use nullable Int64
    return pd.to_numeric(s, errors="coerce").astype("Int64")


# -------------------------- binning --------------------------

def compute_bins(in_ra: pd.Series, in_dec: pd.Series, bin_deg: int) -> Tuple[pd.Series, pd.Series]:
    """
    Compute integer-aligned bins like your parquet layout.
    ra_bin in {0,5,10,...,355}, dec_bin in {-90,-85,...,85}
    """
    ra = pd.to_numeric(in_ra, errors="coerce")
    dec = pd.to_numeric(in_dec, errors="coerce")
    valid = ra.notna() & dec.notna()

    ra_bin = pd.Series([pd.NA] * len(ra), dtype="Int32")
    dec_bin = pd.Series([pd.NA] * len(dec), dtype="Int32")

    if valid.any():
        ra_v = (ra[valid].to_numpy() % 360.0).astype(np.float64)
        dec_v = dec[valid].to_numpy(dtype=np.float64)
        ra_bin_vals = (np.floor_divide(ra_v, bin_deg) * bin_deg).astype(np.int32)
        dec_bin_vals = (np.floor_divide(dec_v + 90.0, bin_deg) * bin_deg - 90).astype(np.int32)
        ra_bin.loc[valid] = ra_bin_vals
        dec_bin.loc[valid] = dec_bin_vals
    return ra_bin, dec_bin


# -------------------------- IO --------------------------

def collect_closest_frames(closest_dir: Path) -> List[Path]:
    # Accept both legacy names (positions00029_closest.csv) and chunk-style (positions_chunk_00029_closest.csv)
    files = sorted(glob.glob(str(closest_dir / "*_closest.csv")))
    return [Path(f) for f in files]


def load_and_normalize(f: Path) -> pd.DataFrame:
    """
    Load a single *_closest.csv and normalize dtypes.
    Expected columns (subset OK):
      row_id (string), in_ra, in_dec, ra, dec, sep_arcsec/sep_deg,
      cntr (Int64), mjd, w1snr, w2snr, qual_frame, qi_fact, saa_sep, moon_masked
    """
    df = pd.read_csv(f, dtype={"row_id": "string"})

    # Floats
    for col in ("in_ra", "in_dec", "ra", "dec", "mjd", "w1snr", "w2snr", "qi_fact", "saa_sep", "sep_deg", "sep_arcsec"):
        if col in df.columns:
            df[col] = to_float32(df[col])

    # Int-like columns
    if "qual_frame" in df.columns:
        df["qual_frame"] = to_int16(df["qual_frame"])  # small ints
    if "cntr" in df.columns:
        df["cntr"] = to_int64_nullable(df["cntr"])     # <-- key fix

    if "moon_masked" in df.columns:
        df["moon_masked"] = df["moon_masked"].astype("string")

    # Ensure sep_arcsec present (vectorized if needed)
    if "sep_arcsec" not in df.columns or df["sep_arcsec"].isna().all():
        if "sep_deg" in df.columns:
            df["sep_arcsec"] = df["sep_deg"] * 3600.0
        elif {"in_ra", "in_dec", "ra", "dec"}.issubset(df.columns):
            ra1 = np.deg2rad(df["in_ra"].astype("float64").to_numpy())
            dec1 = np.deg2rad(df["in_dec"].astype("float64").to_numpy())
            ra2 = np.deg2rad(df["ra"].astype("float64").to_numpy())
            dec2 = np.deg2rad(df["dec"].astype("float64").to_numpy())
            cos_s = np.sin(dec1) * np.sin(dec2) + np.cos(dec1) * np.cos(dec2) * np.cos(ra1 - ra2)
            cos_s = np.clip(cos_s, -1.0, 1.0)
            df["sep_arcsec"] = (np.arccos(cos_s) * (180.0 / math.pi) * 3600.0).astype("float32")
        else:
            df["sep_arcsec"] = np.float32(np.nan)

    return df


# -------------------------- aggregation --------------------------

def build_flags(all_df: pd.DataFrame, radius_arcsec: float, bin_deg: int) -> pd.DataFrame:
    """
    One row per row_id (closest already deduped). Compute strict flag and carry useful fields.
    """
    if "row_id" not in all_df.columns:
        raise SystemExit("Missing 'row_id' in closest CSVs")

    # Just-in-case dedupe: keep the closest
    if all_df.duplicated("row_id").any():
        all_df = all_df.sort_values(["row_id", "sep_arcsec"], ascending=[True, True]).drop_duplicates("row_id", keep="first")

    flags = pd.DataFrame({
        "row_id": all_df["row_id"].astype("string"),
        "in_ra":  all_df.get("in_ra",  np.nan).astype("float32"),
        "in_dec": all_df.get("in_dec", np.nan).astype("float32"),
        "sep_arcsec": all_df["sep_arcsec"].astype("float32"),
    })

    # Strict match flag
    flags["ir_match_strict"] = flags["sep_arcsec"].le(np.float32(radius_arcsec)).astype("boolean")

    # Carry context if present
    for name in ("mjd", "w1snr", "w2snr", "cntr", "qual_frame", "qi_fact", "saa_sep", "moon_masked"):
        if name in all_df.columns:
            flags[name] = all_df[name]

    # Bins from input coordinates
    ra_bin, dec_bin = compute_bins(flags["in_ra"], flags["in_dec"], bin_deg=bin_deg)
    flags["ra_bin"] = ra_bin
    flags["dec_bin"] = dec_bin

    # Final dtype tidy
    for c in ("mjd", "w1snr", "w2snr", "qi_fact", "saa_sep"):
        if c in flags.columns:
            flags[c] = flags[c].astype("float32")
    if "cntr" in flags.columns:
        flags["cntr"] = flags["cntr"].astype("Int64")   # key fix retained
    if "qual_frame" in flags.columns:
        flags["qual_frame"] = flags["qual_frame"].astype("Int16")
    for c in ("ra_bin", "dec_bin"):
        if c in flags.columns:
            flags[c] = flags[c].astype("Int32")
    return flags


# -------------------------- writers --------------------------

def write_global_parquet(flags: pd.DataFrame, out_root: Path, dataset_name: str) -> Path:
    out_root.mkdir(parents=True, exist_ok=True)
    out_path = out_root / f"{dataset_name}_flags_ALL.parquet"
    flags.to_parquet(out_path, engine="pyarrow", index=False)
    return out_path


def write_partitioned_sidecar(flags: pd.DataFrame, out_root: Path) -> Path:
    sidecar_root = out_root / "sidecar"
    sidecar_root.mkdir(parents=True, exist_ok=True)

    have_bins = flags["ra_bin"].notna() & flags["dec_bin"].notna()
    part = flags.loc[have_bins].copy()
    if part.empty:
        print("[WARN] No rows have bins; sidecar will be empty. ALL.parquet still written.")
        return sidecar_root

    try:
        import pyarrow as pa
        import pyarrow.dataset as ds

        table = pa.Table.from_pandas(part, preserve_index=False)
        ds.write_dataset(
            table,
            base_dir=str(sidecar_root),
            format="parquet",
            partitioning=["ra_bin", "dec_bin"],
            existing_data_behavior="overwrite_or_ignore",
        )
        return sidecar_root
    except Exception as e:
        print(f"[WARN] Arrow dataset write failed ({e}); falling back to grouped writes.")
        for (r, d), g in part.groupby(["ra_bin", "dec_bin"], dropna=True):
            subdir = sidecar_root / f"ra_bin={int(r)}" / f"dec_bin={int(d)}"
            subdir.mkdir(parents=True, exist_ok=True)
            g.to_parquet(subdir / "part-flags.parquet", engine="pyarrow", index=False)
        return sidecar_root


def write_success_marker(out_root: Path) -> None:
    (out_root / "_SUCCESS").write_text("ok\n", encoding="utf-8")


# -------------------------- CLI --------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Concatenate NEOWISE-SE flags and write sidecar parquet")
    p.add_argument("--closest-dir", required=True, help="Directory containing *_closest.csv files")
    p.add_argument("--master-root", default="", help="(optional) root of master optical parquet")
    p.add_argument("--out-root", required=True, help="Output root for ALL parquet and sidecar tree")
    p.add_argument("--radius-arcsec", type=float, default=5.0, help="Strict match radius in arcsec (default: 5.0)")
    p.add_argument("--bin-deg", type=int, default=5, help="Bin size in degrees for ra_bin/dec_bin (default: 5)")
    p.add_argument("--dataset-name", type=str, default="neowise_se", help="Dataset base name (default: neowise_se)")
    return p.parse_args()


def main(args: argparse.Namespace) -> None:
    closest_dir = Path(args.closest_dir)
    out_root = Path(args.out_root)

    files = collect_closest_frames(closest_dir)
    if not files:
        raise SystemExit(f"No *_closest.csv files found in: {closest_dir}")

    print(f"[INFO] Loading {len(files)} closest CSVs from {closest_dir} ...")
    frames: List[pd.DataFrame] = []
    for i, f in enumerate(files, 1):
        try:
            df = load_and_normalize(f)
            frames.append(df)
        except Exception as e:
            print(f"[WARN] Skipping {f.name}: {e}")
        if i % 50 == 0:
            print(f"[INFO]  ... {i}/{len(files)} files loaded")

    if not frames:
        raise SystemExit("No valid closest CSVs could be loaded.")

    all_df = pd.concat(frames, ignore_index=True)
    print(f"[INFO] Combined rows: {len(all_df):,}")

    flags = build_flags(all_df, radius_arcsec=args.radius_arcsec, bin_deg=args.bin_deg)
    print(f"[INFO] Flags rows (unique row_id): {len(flags):,}")

    all_parquet = write_global_parquet(flags, out_root=out_root, dataset_name=args.dataset_name)
    print(f"[OK] Wrote global flags parquet: {all_parquet}")

    sidecar_path = write_partitioned_sidecar(flags, out_root=out_root)
    print(f"[OK] Wrote sidecar under: {sidecar_path}")

    write_success_marker(out_root)
    print(f"[OK] Wrote marker: {out_root / '_SUCCESS'}")


if __name__ == "__main__":
    args = parse_args()
    main(args)
