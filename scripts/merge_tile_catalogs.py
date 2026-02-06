#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Merge per-image SExtractor pass2 catalogs into a tile-level deduped catalog,
annotate with plate_id (REGION), write tile-local Parquet partitions and
optionally publish to a master Parquet dataset. Layout-aware discovery of tiles.

Authoritative contract:
- `plate_id` == DSS1-red FITS header `REGION` (e.g., "XE325").
- Mapping CSV must provide REGION under column `irsa_region` (preferred) or
  acceptable fallbacks: `REGION`, `region`.
- IRSA `PLATEID`/`PLTLABEL` are intentionally NOT used for plate_id.

Usage examples:
  # per-tile parquet only
  python ./scripts/merge_tile_catalogs.py \
    --tiles-root ./data/tiles_by_sky --tolerance-arcsec 0.5 --bin-deg 5 \
    --plate-map-csv ./data/metadata/tile_to_dss1red.csv

  # also publish to master Parquet dataset
  python ./scripts/merge_tile_catalogs.py \
    --tiles-root ./data/tiles_by_sky --tolerance-arcsec 0.5 --bin-deg 5 \
    --plate-map-csv ./data/metadata/tile_to_dss1red.csv --publish-parquet

Optional:
  --require-plate    # fail tiles that lack a mapping row (default: warn & write empty)
"""

import argparse
from pathlib import Path
import os
import sys
import pandas as pd
import numpy as np

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception as e:
    raise SystemExit(f"[ERROR] pyarrow is required: {e}")

# Column candidates for RA/Dec and schema normalization
CAND_RA = ["RA_corr", "ALPHAWIN_J2000", "ALPHA_J2000", "RA", "X_WORLD", "RAJ2000", "ALPHA_J2000"]
CAND_DEC= ["Dec_corr", "DELTAWIN_J2000", "DELTA_J2000", "DEC", "Y_WORLD", "DEJ2000", "DELTA_J2000"]
_RA_ALIASES = ["ALPHA_J2000", "ALPHAWIN_J2000", "RAJ2000", "RA", "X_WORLD", "RA_corr"]
_DEC_ALIASES= ["DELTA_J2000", "DELTAWIN_J2000", "DEJ2000", "DEC", "Y_WORLD", "Dec_corr"]
_PROV_TEXT = ["tile_id", "image_catalog_path", "image_id"]


# -----------------------------------------------------------------------------
# Tile discovery (flat and sharded)
# -----------------------------------------------------------------------------
def iter_tile_dirs_any(tiles_root: Path):
    """Yield tile directories from flat layout and sharded tiles_by_sky."""
    flat = tiles_root
    if flat.exists():
        for p in sorted(flat.glob("tile-RA*-DEC*")):
            if p.is_dir():
                yield p
    sharded = tiles_root.parent / "tiles_by_sky"
    if sharded.exists():
        for p in sorted(sharded.glob("ra_bin=*/dec_bin=*/tile-RA*-DEC*")):
            if p.is_dir():
                yield p


# -----------------------------------------------------------------------------
# Mapping: tile_id -> plate_id (REGION)
# -----------------------------------------------------------------------------
def load_plate_map(csv_path: Path) -> dict[str, str]:
    """
    Load mapping CSV and return dict: tile_id -> plate_id (REGION).
    Accepts REGION under `irsa_region` (preferred) or fallbacks: `REGION`, `region`.
    """
    if not csv_path.exists():
        print(f"[WARN] plate map CSV not found: {csv_path} (empty plate_id will be written)")
        return {}
    # Read minimally, but be tolerant to extra columns
    try:
        df_head = pd.read_csv(csv_path, nrows=0)
        cols = list(df_head.columns)
    except Exception as e:
        raise SystemExit(f"[ERROR] Failed to read header from mapping CSV: {csv_path} ({e})")

    region_col = None
    for cand in ("irsa_region", "REGION", "region"):
        if cand in cols:
            region_col = cand
            break
    if region_col is None:
        raise SystemExit(
            f"[ERROR] Mapping CSV {csv_path} must contain `irsa_region` (or REGION/region). "
            f"Found columns: {cols}"
        )

    # Read only required columns if available
    usecols = [c for c in ("tile_id", region_col) if c in cols]
    df = pd.read_csv(csv_path, usecols=usecols) if usecols else pd.read_csv(csv_path)
    if "tile_id" not in df.columns:
        raise SystemExit(f"[ERROR] Mapping CSV is missing required column `tile_id`")

    df["tile_id"] = df["tile_id"].astype(str)
    df[region_col] = df[region_col].astype(str)

    mapping = {tid: reg for tid, reg in df[["tile_id", region_col]].itertuples(index=False, name=None)}
    return mapping


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def find_coord_columns(df: pd.DataFrame):
    ra_col = next((c for c in CAND_RA if c in df.columns), None)
    dec_col = next((c for c in CAND_DEC if c in df.columns), None)
    if not ra_col or not dec_col:
        raise ValueError(f"Could not find RA/Dec columns in: {list(df.columns)}")
    return ra_col, dec_col


def pick_best(group: pd.DataFrame, mag_col="MAG_AUTO", flags_col="FLAGS"):
    sort_cols, asc = [], []
    if flags_col in group.columns:
        sort_cols.append(flags_col); asc.append(True)
    if mag_col in group.columns:
        sort_cols.append(mag_col); asc.append(True)
    if sort_cols:
        g2 = group.sort_values(sort_cols, ascending=asc)
        return g2.iloc[0]
    return group.iloc[0]


def _enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    # Floats for RA/Dec-like aliases
    for c in _RA_ALIASES:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("float32")
    for c in _DEC_ALIASES:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("float32")
    # Partition bins as small ints
    if "ra_bin" in df.columns:
        df["ra_bin"] = df["ra_bin"].astype("int16")
    if "dec_bin" in df.columns:
        df["dec_bin"] = df["dec_bin"].astype("int16")
    # Provenance text
    for c in _PROV_TEXT:
        if c in df.columns:
            df[c] = df[c].astype("string")
    # Plate field as string
    if "plate_id" in df.columns:
        df["plate_id"] = df["plate_id"].astype("string")
    return df


def dedupe_by_cells(df: pd.DataFrame, ra_col: str, dec_col: str, tol_arcsec: float) -> pd.DataFrame:
    df[ra_col] = pd.to_numeric(df[ra_col], errors='coerce')
    df[dec_col]= pd.to_numeric(df[dec_col], errors='coerce')
    df = df.dropna(subset=[ra_col, dec_col])
    tol_deg = tol_arcsec / 3600.0
    df["_ra_cell"]  = (df[ra_col]  / tol_deg).round().astype("int64")
    df["_dec_cell"] = (df[dec_col] / tol_deg).round().astype("int64")
    out = (df.groupby(["_ra_cell","_dec_cell"], sort=False)
             .apply(lambda g: pick_best(g), include_groups=False)
             .reset_index(drop=True))
    return out.drop(columns=["_ra_cell","_dec_cell"], errors="ignore")


def add_bins(df: pd.DataFrame, ra_col: str, dec_col: str, bin_deg: float) -> pd.DataFrame:
    df[ra_col]  = pd.to_numeric(df[ra_col], errors="coerce").astype("float32")
    df[dec_col] = pd.to_numeric(df[dec_col], errors="coerce").astype("float32")
    ra  = df[ra_col].to_numpy(dtype=np.float32) % 360.0
    dec = df[dec_col].to_numpy(dtype=np.float32)
    df["ra_bin"]  = np.floor(ra / np.float32(bin_deg)).astype("int16")
    df["dec_bin"] = np.floor((dec + 90.0) / np.float32(bin_deg)).astype("int16")
    return df


def _ensure_dir_is_directory(p: Path):
    if p.exists():
        if not p.is_dir():
            raise RuntimeError(f"Expected directory, found non-dir at: {p}")
    else:
        os.makedirs(str(p), exist_ok=True)


def write_partition(root: Path, ra_bin: int, dec_bin: int, df_part: pd.DataFrame, tag: str) -> Path:
    root_abs = Path(root).resolve()
    part_dir = root_abs / f"ra_bin={ra_bin}" / f"dec_bin={dec_bin}"
    _ensure_dir_is_directory(root_abs)
    _ensure_dir_is_directory(root_abs / f"ra_bin={ra_bin}")
    _ensure_dir_is_directory(part_dir)
    file_path = part_dir / f"part-{tag}.parquet"
    df_part = _enforce_schema(df_part)
    table = pa.Table.from_pandas(df_part, preserve_index=False)
    try:
        pq.write_table(table, str(file_path), compression="zstd", use_dictionary=True)
    except FileNotFoundError:
        _ensure_dir_is_directory(part_dir)
        pq.write_table(table, str(file_path), compression="zstd", use_dictionary=True)
    return file_path


def iter_catalog_files(catalogs_root: Path):
    root = Path(catalogs_root)
    try:
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if not d.startswith('.') and d != 'parquet']
            for fn in filenames:
                if fn == 'sextractor_pass2.csv':
                    p = Path(dirpath) / fn
                    try:
                        if p.is_file():
                            yield p
                    except FileNotFoundError:
                        continue
    except FileNotFoundError:
        return


def is_non_zero_file(fpath: str) -> bool:
    return os.path.isfile(fpath) and os.path.getsize(fpath) > 0


# -----------------------------------------------------------------------------
# Merge one tile
# -----------------------------------------------------------------------------
def merge_one_tile(
    tile_path: Path,
    tol_arcsec: float,
    overwrite: bool,
    publish_root: Path | None,
    bin_deg: float,
    plate_map: dict[str, str],
    require_plate: bool
) -> int:
    catalogs_root = tile_path / "catalogs"
    files = list(iter_catalog_files(catalogs_root))
    if not files:
        print(f"[SKIP] Tile {tile_path.name}: no sextractor_pass2.csv found")
        return 0

    frames = []
    for f in files:
        if not is_non_zero_file(str(f)):
            print(f"[SKIP] {f}: empty file (zero bytes)")
            continue
        probe = pd.read_csv(f, nrows=1)
        if probe.empty:
            print(f"[SKIP] {f}: empty catalog (header only)")
            continue
        df = pd.read_csv(f)
        if df.empty:
            print(f"[SKIP] {f}: empty catalog")
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
    if raw.empty:
        print(f"[SKIP] Tile {tile_path.name}: concatenated catalog is empty")
        return 0

    ra_col, dec_col = find_coord_columns(raw)
    deduped = dedupe_by_cells(raw, ra_col, dec_col, tol_arcsec)
    if deduped.empty:
        print(f"[SKIP] Tile {tile_path.name}: deduped catalog is empty")
        return 0

    # bins
    deduped = add_bins(deduped, ra_col, dec_col, bin_deg)

    # plate_id = REGION from mapping
    plate_id = ""
    if tile_path.name in plate_map:
        plate_id = plate_map[tile_path.name]
    elif require_plate:
        print(f"[ERROR] Missing plate mapping for tile {tile_path.name} and --require-plate set", file=sys.stderr)
        return 0
    else:
        print(f"[WARN] Missing plate mapping for tile {tile_path.name}; writing empty plate_id")

    deduped["plate_id"] = plate_id
    deduped = _enforce_schema(deduped)

    # per-tile parquet
    tile_parquet_root = (catalogs_root / "parquet").resolve()
    _ensure_dir_is_directory(tile_parquet_root)

    count = 0
    for (rb, db), sub in deduped.groupby(["ra_bin", "dec_bin"], sort=False):
        if sub.empty:
            continue
        write_partition(tile_parquet_root, int(rb), int(db), sub, tile_path.name)
        count += len(sub)
        if count % 100000 < len(sub):
            print(f"[INFO] Tile {tile_path.name}: wrote {count} rows so far")
    print(f"[DONE] Tile {tile_path.name}: total rows={count}")

    # publish to master
    if publish_root:
        pre_bins = {(int(rb), int(db)) for (rb, db), _ in deduped.groupby(["ra_bin", "dec_bin"], sort=False)}
        for rb, db in pre_bins:
            (publish_root / f"ra_bin={rb}" / f"dec_bin={db}").mkdir(parents=True, exist_ok=True)
        for (rb, db), sub in deduped.groupby(["ra_bin", "dec_bin"], sort=False):
            if sub.empty:
                continue
            write_partition(publish_root, int(rb), int(db), _enforce_schema(sub.copy()), tile_path.name)
        print(f"[PUBLISH] Tile {tile_path.name}: published to master dataset")

    return count


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Merge tile catalogs -> Parquet (tile-local + optional master) (layout-aware)")
    ap.add_argument("--tiles-root", default="./data/tiles", help="Root folder for tiles (flat or sharded)")
    ap.add_argument("--tolerance-arcsec", type=float, default=0.5, help="Dedup cell tolerance (arcsec)")
    ap.add_argument("--publish-parquet", action='store_true', help="Also publish to master Parquet dataset")
    ap.add_argument("--overwrite", action='store_true', help="(reserved; not used as writer does per-file name)")
    ap.add_argument("--bin-deg", type=float, default=5.0, help="Bin size (deg) for ra_bin/dec_bin")
    ap.add_argument("--plate-map-csv", default="./data/metadata/tile_to_dss1red.csv",
                    help="CSV with columns `tile_id` and REGION under `irsa_region` (fallbacks: REGION/region)")
    ap.add_argument("--require-plate", action='store_true',
                    help="Fail tiles lacking a plate mapping (default: warn & write empty)")

    args = ap.parse_args()
    tiles_root = Path(args.tiles_root)
    tile_dirs = list(iter_tile_dirs_any(tiles_root))
    if not tile_dirs:
        print(f"[WARN] No tiles found under {tiles_root} (or tiles_by_sky sibling). Nothing to do.")
        return

    plate_map = load_plate_map(Path(args.plate_map_csv))

    publish_root = Path("./data/local-cats/_master_optical_parquet").resolve() if args.publish_parquet else None
    if publish_root:
        publish_root.mkdir(parents=True, exist_ok=True)

    total = 0
    for idx, tile_path in enumerate(tile_dirs, start=1):
        print(f"[RUN] ({idx}/{len(tile_dirs)}) Processing {tile_path.name}")
        rows = merge_one_tile(tile_path, args.tolerance_arcsec, args.overwrite,
                              publish_root, args.bin_deg, plate_map, args.require_plate)
        total += rows
    print(f"[ALL DONE] Processed {len(tile_dirs)} tiles; total rows={total}")


if __name__ == "__main__":
    main()