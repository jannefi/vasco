
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Merge per-image SExtractor pass2 catalogs into a tile-level deduped catalog,
write tile-local Parquet partitions (ra_bin/dec_bin, bin_deg=5), and optionally
publish them into a master Parquet dataset.

Robust v2: avoids recursive glob races by using os.walk with directory
exclusions (skips 'parquet' and hidden dirs), catches FileNotFoundError
during traversal, and uses absolute paths for master publishing.

Usage:
python merge_tile_catalogs.py     --tiles-root ./data/tiles     --tolerance-arcsec 0.5     [--publish-parquet] [--overwrite] [--bin-deg 5]

Outputs:
- Per tile: catalogs/parquet/ra_bin=XX/dec_bin=YY/part-tile.parquet
- Optional master: data/local-cats/_master_optical_parquet/ra_bin=XX/dec_bin=YY/part-<tile>.parquet
"""
import argparse
from pathlib import Path
import os
import pandas as pd
import numpy as np
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception as e:
    raise SystemExit(f"[ERROR] pyarrow is required: {e}")

CAND_RA  = ["RA_corr", "ALPHAWIN_J2000", "ALPHA_J2000", "RA", "X_WORLD", "RAJ2000", "ALPHA_J2000"]
CAND_DEC = ["Dec_corr", "DELTAWIN_J2000", "DELTA_J2000", "DEC", "Y_WORLD", "DEJ2000", "DELTA_J2000"]

def find_coord_columns(df):
    ra_col = next((c for c in CAND_RA if c in df.columns), None)
    dec_col = next((c for c in CAND_DEC if c in df.columns), None)
    if not ra_col or not dec_col:
        raise ValueError(f"Could not find RA/Dec columns in: {list(df.columns)}")
    return ra_col, dec_col

def pick_best(group, mag_col="MAG_AUTO", flags_col="FLAGS"):
    sort_cols, asc = [], []
    if flags_col in group.columns:
        sort_cols.append(flags_col); asc.append(True)
    if mag_col in group.columns:
        sort_cols.append(mag_col); asc.append(True)
    if sort_cols:
        g2 = group.sort_values(sort_cols, ascending=asc)
        return g2.iloc[0]
    return group.iloc[0]

def dedupe_by_cells(df, ra_col, dec_col, tol_arcsec):
    tol_deg = tol_arcsec / 3600.0
    df["_ra_cell"] = (df[ra_col] / tol_deg).round().astype("int64")
    df["_dec_cell"] = (df[dec_col] / tol_deg).round().astype("int64")
    out = (df.groupby(["_ra_cell", "_dec_cell"], sort=False)
             .apply(lambda g: pick_best(g), include_groups=False)
             .reset_index(drop=True))
    return out.drop(columns=["_ra_cell", "_dec_cell"], errors="ignore")

def add_bins(df, ra_col, dec_col, bin_deg):
    df[ra_col] = df[ra_col].astype("float32")
    df[dec_col] = df[dec_col].astype("float32")
    ra = df[ra_col].to_numpy(dtype=np.float32) % 360.0
    dec = df[dec_col].to_numpy(dtype=np.float32)
    df["ra_bin"] = np.floor(ra / np.float32(bin_deg)).astype("int16")
    df["dec_bin"] = np.floor((dec + 90.0) / np.float32(bin_deg)).astype("int16")
    return df

def write_partition(root: Path, ra_bin: int, dec_bin: int, df_part: pd.DataFrame, tag: str):
    part_dir = root / f"ra_bin={ra_bin}" / f"dec_bin={dec_bin}"
    part_dir.mkdir(parents=True, exist_ok=True)
    file_path = part_dir / f"part-{tag}.parquet"
    table = pa.Table.from_pandas(df_part, preserve_index=False)
    try:
        pq.write_table(table, str(file_path), compression="zstd", use_dictionary=True)
    except FileNotFoundError:
        # Defensive: ensure directory exists, then retry once
        part_dir.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, str(file_path), compression="zstd", use_dictionary=True)
    return file_path

def iter_catalog_files(catalogs_root: Path):
    """Robust iterator for sextractor_pass2.csv under catalogs/, skipping parquet dirs.
    Uses os.walk with try/except to avoid FileNotFoundError during traversal.
    """
    root = Path(catalogs_root)
    try:
        for dirpath, dirnames, filenames in os.walk(root):
            # Exclude parquet partitions and hidden dirs from traversal
            dirnames[:] = [d for d in dirnames if not d.startswith('.') and d != 'parquet']
            for fn in filenames:
                if fn == 'sextractor_pass2.csv':
                    p = Path(dirpath) / fn
                    # Double-check existence to avoid races
                    try:
                        if p.is_file():
                            yield p
                    except FileNotFoundError:
                        continue
    except FileNotFoundError:
        # Root vanished; yield nothing
        return

def merge_one_tile(tile_path: Path, tol_arcsec: float, overwrite: bool, publish_root: Path | None, bin_deg: float):
    catalogs_root = tile_path / "catalogs"
    files = list(iter_catalog_files(catalogs_root))
    if not files:
        print(f"[SKIP] Tile {tile_path.name}: no sextractor_pass2.csv found")
        return 0
    frames = []
    for f in files:
        df = pd.read_csv(f)
        if df.empty or len(df) == 0:
            print(f"[SKIP] {f}: empty catalog (header only)")
            continue
        ra_col, dec_col = find_coord_columns(df)
        df["tile_id"] = tile_path.name
        df["image_catalog_path"] = str(f.relative_to(tile_path))
        df["image_id"] = f.parent.name if f.parent.name != "catalogs" else tile_path.name
        frames.append(df)
    
    if not frames:
        print(f"[SKIP] Tile {tile_path.name}: all catalogs empty")
        return 0
    raw = pd.concat(frames, ignore_index=True)
    if raw.empty or len(raw) == 0:
        print(f"[SKIP] Tile {tile_path.name}: concatenated catalog is empty")
        return 0
    ra_col, dec_col = find_coord_columns(raw)
    deduped = dedupe_by_cells(raw, ra_col, dec_col, tol_arcsec)
    deduped = add_bins(deduped, ra_col, dec_col, bin_deg)
    # Write tile-local parquet partitions
    tile_parquet_root = catalogs_root / "parquet"
    tile_parquet_root.mkdir(parents=True, exist_ok=True)
    count = 0
    for (rb, db), sub in deduped.groupby(["ra_bin", "dec_bin"], sort=False):
        if sub.empty: continue
        write_partition(tile_parquet_root, int(rb), int(db), sub, tile_path.name)
        count += len(sub)
        if count % 100000 < len(sub):
            print(f"[INFO] Tile {tile_path.name}: wrote {count} rows so far")
    print(f"[DONE] Tile {tile_path.name}: total rows={count}")
    # Optional publish to master parquet
    if publish_root:
        # Pre-create all partition dirs to avoid any FS race
        pre_bins = set()
        for (rb, db), _ in deduped.groupby(["ra_bin", "dec_bin"], sort=False):
            pre_bins.add((int(rb), int(db)))
        for rb, db in pre_bins:
            pre_dir = publish_root / f"ra_bin={rb}" / f"dec_bin={db}"
            pre_dir.mkdir(parents=True, exist_ok=True)
        for (rb, db), sub in deduped.groupby(["ra_bin", "dec_bin"], sort=False):
            if sub.empty: continue
            write_partition(publish_root, int(rb), int(db), sub, tile_path.name)
        print(f"[PUBLISH] Tile {tile_path.name}: published to master dataset")
    return count

def main():
    ap = argparse.ArgumentParser(description="Merge tile catalogs -> Parquet (tile-local + optional master)")
    ap.add_argument("--tiles-root", default="./data/tiles")
    ap.add_argument("--tolerance-arcsec", type=float, default=0.5)
    ap.add_argument("--publish-parquet", action="store_true", help="Also publish to master Parquet dataset")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--bin-deg", type=float, default=5.0)
    args = ap.parse_args()
    tiles_root = Path(args.tiles_root)
    tile_dirs = sorted([p for p in tiles_root.glob("tile-RA*-DEC*") if p.is_dir()])
    publish_root = Path("./data/local-cats/_master_optical_parquet").resolve() if args.publish_parquet else None
    if publish_root:
        publish_root.mkdir(parents=True, exist_ok=True)
    total = 0
    for idx, tile_path in enumerate(tile_dirs, start=1):
        print(f"[RUN] ({idx}/{len(tile_dirs)}) Processing {tile_path.name}")
        rows = merge_one_tile(tile_path, args.tolerance_arcsec, args.overwrite, publish_root, args.bin_deg)
        total += rows
    print(f"[ALL DONE] Processed {len(tile_dirs)} tiles; total rows={total}")

if __name__ == "__main__":
    main()
