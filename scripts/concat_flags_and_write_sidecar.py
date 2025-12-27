
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build NEOWISE-SE IR sidecar and global flags from per-chunk 'closest' CSVs.

Inputs:
  --closest-dir   Directory containing positions_chunk_XXXX_closest.csv files
  --master-root   Root of the master optical parquet (not strictly needed here;
                  kept for interface stability / future bin derivations)
  --out-root      Output root for:
                    - neowise_se_flags_ALL.parquet
                    - sidecar/ (partitioned by ra_bin, dec_bin)
  --radius-arcsec Strict match radius in arcsec (default: 5.0)
  --bin-deg       Bin size in degrees for ra_bin/dec_bin (default: 5)
  --dataset-name  Base name for outputs (default: neowise_se)

Outputs:
  <out-root>/<dataset-name>_flags_ALL.parquet
  <out-root>/sidecar/ra_bin=XX/dec_bin=YY/part-*.parquet
  <out-root>/_SUCCESS  (marker for idempotency)

Notes:
  * 'Strict match' means the ADQL already enforced quality gates and MJD cap,
    and we additionally require sep_arcsec <= radius_arcsec.
  * Bins are computed from optical input positions (in_ra, in_dec) present in
    the closest CSVs (as selected in ADQL). If missing, rows are written with
    ra_bin/dec_bin = None (they remain in the global ALL parquet).
"""

import argparse
import glob
import math
import os
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd


def to_float32(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").astype("float32")


def to_float64(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").astype("float64")


def to_int32(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").astype("Int32")  # pandas NA-aware


def compute_bins(in_ra: pd.Series, in_dec: pd.Series, bin_deg: int) -> Tuple[pd.Series, pd.Series]:
    """
    Compute 5-degree (default) integer-aligned bins matching your parquet layout.
      RA in [0,360), DEC in [-90,90]
      ra_bin ∈ {0,5,10,...,355}
      dec_bin ∈ {-90,-85,...,85}
    """
    # RA wrap, then floor to bin
    ra = pd.to_numeric(in_ra, errors="coerce").fillna(np.nan)
    dec = pd.to_numeric(in_dec, errors="coerce").fillna(np.nan)

    # Compute bins only where both ra and dec are finite
    valid = ra.notna() & dec.notna()
    ra_bin = pd.Series([pd.NA] * len(ra), dtype="Int32")
    dec_bin = pd.Series([pd.NA] * len(dec), dtype="Int32")

    if valid.any():
        ra_v = ra[valid].to_numpy()
        dec_v = dec[valid].to_numpy()

        ra_wrapped = np.mod(ra_v, 360.0)
        ra_bin_vals = (np.floor_divide(ra_wrapped.astype(np.float64), bin_deg) * bin_deg).astype(np.int32)

        # Shift dec to [0,180), bin, then shift back
        dec_shifted = dec_v + 90.0
        dec_bin_vals = (np.floor_divide(dec_shifted.astype(np.float64), bin_deg) * bin_deg - 90).astype(np.int32)

        ra_bin.loc[valid] = ra_bin_vals
        dec_bin.loc[valid] = dec_bin_vals

    return ra_bin, dec_bin


def collect_closest_frames(closest_dir: Path) -> List[Path]:
    pattern = str(closest_dir / "*_closest.csv")
    files = sorted(glob.glob(pattern))
    return [Path(f) for f in files]


def load_and_normalize(f: Path) -> pd.DataFrame:
    """
    Load a single *_closest.csv and normalize dtypes and column names.
    Expected columns (subset ok):
      row_id (string), in_ra, in_dec, ra, dec, sep_arcsec or sep_deg,
      cntr, mjd, w1snr, w2snr, qual_frame, qi_fact, saa_sep, moon_masked
    """
    df = pd.read_csv(f, dtype={"row_id": "string"})
    # Numeric coercion with memory-friendly dtypes
    for col in ("in_ra", "in_dec", "ra", "dec", "mjd", "w1snr", "w2snr", "qi_fact", "saa_sep", "sep_deg", "sep_arcsec"):
        if col in df.columns:
            df[col] = to_float32(df[col])

    for col in ("cntr", "qual_frame"):
        if col in df.columns:
            df[col] = to_int32(df[col])

    if "moon_masked" in df.columns:
        df["moon_masked"] = df["moon_masked"].astype("string")

    # Ensure sep_arcsec exists
    if "sep_arcsec" not in df.columns or df["sep_arcsec"].isna().all():
        if "sep_deg" in df.columns:
            df["sep_arcsec"] = df["sep_deg"] * 3600.0
        else:
            # If we have the inputs, compute geometric separation; else leave NaN
            have_inputs = set(("in_ra", "in_dec", "ra", "dec")).issubset(df.columns)
            if have_inputs and len(df):
                # Vectorized great-circle separation using radians
                d2r = math.pi / 180.0
                ra1 = np.deg2rad(df["in_ra"].to_numpy(dtype="float64"))
                dec1 = np.deg2rad(df["in_dec"].to_numpy(dtype="float64"))
                ra2 = np.deg2rad(df["ra"].to_numpy(dtype="float64"))
                dec2 = np.deg2rad(df["dec"].to_numpy(dtype="float64"))
                cos_s = np.sin(dec1) * np.sin(dec2) + np.cos(dec1) * np.cos(dec2) * np.cos(ra1 - ra2)
                cos_s = np.clip(cos_s, -1.0, 1.0)
                sep_rad = np.arccos(cos_s)
                df["sep_arcsec"] = (sep_rad * (180.0 / math.pi) * 3600.0).astype("float32")
            else:
                df["sep_arcsec"] = np.float32(np.nan)

    return df


def build_flags(all_df: pd.DataFrame, radius_arcsec: float) -> pd.DataFrame:
    """
    Collapse to one row per row_id (closest already produced that), compute strict flag and carry useful fields.
    """
    if "row_id" not in all_df.columns:
        raise SystemExit("Missing 'row_id' in closest CSVs")

    # If accidental dups exist (shouldn't), keep the closest
    if all_df.duplicated("row_id").any():
        all_df = all_df.sort_values(["row_id", "sep_arcsec"], ascending=[True, True]).drop_duplicates("row_id", keep="first")

    flags = pd.DataFrame()
    flags["row_id"] = all_df["row_id"].astype("string")
    flags["in_ra"] = all_df.get("in_ra", pd.Series([np.nan] * len(all_df))).astype("float32")
    flags["in_dec"] = all_df.get("in_dec", pd.Series([np.nan] * len(all_df))).astype("float32")
    flags["sep_arcsec"] = all_df["sep_arcsec"].astype("float32")

    # Strict match semantics
    flags["ir_match_strict"] = flags["sep_arcsec"].le(np.float32(radius_arcsec)).astype("boolean")

    # Carry minimal context from the closest row (optional, keeps size modest)
    for name in ("mjd", "w1snr", "w2snr", "cntr", "qual_frame", "qi_fact", "saa_sep", "moon_masked"):
        if name in all_df.columns:
            flags[name] = all_df[name]

    # Compute bins from in_ra/in_dec
    ra_bin, dec_bin = compute_bins(flags["in_ra"], flags["in_dec"], bin_deg=args.bin_deg)
    flags["ra_bin"] = ra_bin
    flags["dec_bin"] = dec_bin

    # Final dtype tidy
    float_cols = ("in_ra", "in_dec", "sep_arcsec", "mjd", "w1snr", "w2snr", "qi_fact", "saa_sep")
    for c in float_cols:
        if c in flags.columns:
            flags[c] = flags[c].astype("float32")

    for c in ("cntr", "qual_frame", "ra_bin", "dec_bin"):
        if c in flags.columns:
            flags[c] = flags[c].astype("Int32")

    return flags


def write_global_parquet(flags: pd.DataFrame, out_root: Path, dataset_name: str) -> Path:
    out_root.mkdir(parents=True, exist_ok=True)
    out_path = out_root / f"{dataset_name}_flags_ALL.parquet"
    flags.to_parquet(out_path, engine="pyarrow", index=False)
    return out_path


def write_partitioned_sidecar(flags: pd.DataFrame, out_root: Path) -> Optional[Path]:
    """
    Write partitioned parquet under <out-root>/sidecar/ra_bin=XX/dec_bin=YY/.
    Uses pyarrow.dataset when available; otherwise falls back to a simple per-partition loop.
    Rows with missing bins are skipped from sidecar (remain available in the ALL parquet).
    """
    sidecar_root = out_root / "sidecar"
    sidecar_root.mkdir(parents=True, exist_ok=True)

    # Keep only rows with both bins present
    have_bins = flags["ra_bin"].notna() & flags["dec_bin"].notna()
    part = flags.loc[have_bins].copy()

    if part.empty:
        print("[WARN] No rows have in_ra/in_dec → sidecar will be empty; ALL.parquet is still written.")
        return sidecar_root

    try:
        import pyarrow as pa
        import pyarrow.dataset as ds

        table = pa.Table.from_pandas(part, preserve_index=False)
        # Write with overwrite-or-ignore to be idempotent
        ds.write_dataset(
            table,
            base_dir=str(sidecar_root),
            format="parquet",
            partitioning=["ra_bin", "dec_bin"],
            existing_data_behavior="overwrite_or_ignore",
        )
        return sidecar_root

    except Exception as e:
        print(f"[WARN] pyarrow.dataset not available or failed ({e}); falling back to grouped writes.")
        # Fallback: group and write per (ra_bin, dec_bin)
        for (r, d), g in part.groupby(["ra_bin", "dec_bin"], dropna=True):
            subdir = sidecar_root / f"ra_bin={int(r)}" / f"dec_bin={int(d)}"
            subdir.mkdir(parents=True, exist_ok=True)
            # file name aligned with typical parquet file naming
            g.to_parquet(subdir / "part-flags.parquet", engine="pyarrow", index=False)
        return sidecar_root


def write_success_marker(out_root: Path) -> None:
    (out_root / "_SUCCESS").write_text("ok\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Concatenate NEOWISE-SE flags and write sidecar parquet")
    p.add_argument("--closest-dir", required=True, help="Directory containing *_closest.csv files")
    p.add_argument("--master-root", required=False, default="", help="Root of master optical parquet (optional)")
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

    flags = build_flags(all_df, radius_arcsec=args.radius_arcsec)
    print(f"[INFO] Flags rows (unique row_id): {len(flags):,}")

    # Global ALL parquet
    all_parquet = write_global_parquet(flags, out_root=out_root, dataset_name=args.dataset_name)
    print(f"[OK] Wrote global flags parquet: {all_parquet}")

    # Partitioned sidecar
    sidecar_path = write_partitioned_sidecar(flags, out_root=out_root)
    print(f"[OK] Wrote sidecar under: {sidecar_path}")

    # Marker for idempotent Make targets
    write_success_marker(out_root)
    print(f"[OK] Wrote marker: {out_root / '_SUCCESS'}")


if __name__ == "__main__":
    args = parse_args()
    main(args)
