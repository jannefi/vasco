#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Promote tiles from the ingest ("old") master to the curated master.

- Finds tiles present in OLD master but missing in CURATED master.
- Ensures every promoted row carries plate_id:
  * If plate_id column exists but any row is empty -> fill from mapping CSV.
  * If plate_id column is missing in OLD -> fill entirely from mapping CSV.
- Writes to CURATED with partitioning (ra_bin/dec_bin) and filenames:
  data_<tile_id>.parquet (one per tile per (ra_bin,dec_bin)).
- Produces a JSON manifest with promoted tiles, row counts, and timing.

Author: Janne’s pipeline (automation-friendly)
"""

import argparse, json, os, sys, time
from pathlib import Path
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.dataset as ds
import pyarrow.compute as pc

DEF_OLD = "./data/local-cats/_master_optical_parquet"
DEF_CUR = "./data/local-cats/_master_optical_parquet_with_plateid_region"
DEF_MAP = "./data/metadata/tile_to_dss1red.csv"
DEF_OUT = "./logs/promote_master_tiles"

def load_dataset(root: str) -> ds.Dataset:
    # Read only needed columns when scanning tile ids
    return ds.dataset(root, format="parquet", partitioning="hive", exclude_invalid_files=True)

def distinct_tiles(dataset: ds.Dataset) -> set:
    scan = dataset.scanner(columns=["tile_id"])
    tbl = scan.to_table()
    if tbl.num_rows == 0 or "tile_id" not in tbl.column_names:
        return set()
    # Fast unique
    tiles = pc.unique(tbl["tile_id"]).to_pylist()
    return set(t for t in tiles if t)

def read_mapping_csv(map_csv: str) -> pa.Table:
    if not Path(map_csv).exists():
        raise SystemExit(f"[ERROR] mapping CSV not found: {map_csv}")
    # autodetect, then normalize column names
    tbl = pacsv.read_csv(map_csv)
    cols = {c.lower(): c for c in tbl.column_names}
    # prefer irsa_region, then REGION/region
    src = cols.get("irsa_region") or cols.get("region") or cols.get("REGION")
    tid = cols.get("tile_id") or cols.get("TILE_ID")
    if not src or not tid:
        raise SystemExit(f"[ERROR] mapping CSV must have tile_id and one of irsa_region/REGION/region; got: {tbl.column_names}")
    out = pa.table({
        "tile_id": pc.cast(tbl[tid], pa.string()),
        "plate_id": pc.cast(tbl[src], pa.string()),
    })
    # drop empties
    mask = pc.and_(pc.is_valid(out["tile_id"]), pc.is_valid(out["plate_id"]))
    return out.filter(mask)

def ensure_plate_id(table: pa.Table, map_tbl: pa.Table) -> pa.Table:
    # If plate_id exists and fully populated, just return
    if "plate_id" in table.column_names:
        col = table["plate_id"]
        # any null/empty?
        empties = pc.sum(pc.if_else(pc.or_(pc.is_null(col), pc.equal(col, "")), 1, 0)).as_py()
        if empties == 0:
            return table
    else:
        # add empty plate_id to allow join overwrite
        table = table.append_column("plate_id", pa.array([""] * table.num_rows, type=pa.string()))
    # join on tile_id
    joined = table.join(map_tbl, keys="tile_id", right_keys="tile_id", join_type="left outer", coalesce_keys=True)
    # After join, columns: plate_id (left), plate_id_1 (from mapping)
    l = joined["plate_id"]
    r = joined["plate_id_1"] if "plate_id_1" in joined.column_names else None
    new_plate = r if r is not None else l
    if r is not None:
        # replace empties with r
        repl = pc.if_else(pc.or_(pc.is_null(l), pc.equal(l, "")), r, l)
        new_plate = repl
    # drop old plate_id and plate_id_1, add final plate_id
    cols = [c for c in joined.column_names if c not in ("plate_id", "plate_id_1")]
    base = joined.select(cols)
    return base.append_column("plate_id", pc.cast(new_plate, pa.string()))

def write_tile_parts(cur_root: str, tile_tbl: pa.Table, tile_id: str) -> int:
    """
    Writes one or more files: ra_bin/dec_bin/data_<tile>.parquet for each (ra_bin,dec_bin).
    """
    if "ra_bin" not in tile_tbl.column_names or "dec_bin" not in tile_tbl.column_names:
        raise SystemExit("[ERROR] table lacks ra_bin/dec_bin needed for partitioning.")
    # Partitioned write, per tile (so we can control the basename)
    # Use basename_template to keep data_<tile>.parquet
    ds.write_dataset(
        data=tile_tbl,
        base_dir=cur_root,
        format="parquet",
        partitioning=ds.partitioning(pa.schema([("ra_bin", pa.int16()), ("dec_bin", pa.int16())]), flavor="hive"),
        basename_template=f"data_{tile_id}.parquet",
        existing_data_behavior="overwrite_or_ignore",
        file_visitor=None,  # can be used to count per-file rows if needed
    )
    return tile_tbl.num_rows

def main():
    ap = argparse.ArgumentParser(description="Promote tiles from OLD master to CURATED master (append-only).")
    ap.add_argument("--old-root", default=DEF_OLD)
    ap.add_argument("--cur-root", default=DEF_CUR)
    ap.add_argument("--map-csv", default=DEF_MAP)
    ap.add_argument("--tiles-file", help="Optional file with tile_id list; if absent, auto-detect missing tiles in CURATED.")
    ap.add_argument("--limit", type=int, default=0, help="Process at most N tiles for this run (0=all).")
    ap.add_argument("--manifest-dir", default=DEF_OUT)
    ap.add_argument("--require-plate", action="store_true", help="Abort if any tile would produce rows with empty plate_id.")
    ap.add_argument("--dry-run", action="store_true", help="List tiles that WOULD be promoted, then exit 0.")
    args = ap.parse_args()

    old_ds = load_dataset(args.old_root)
    cur_ds = load_dataset(args.cur_root)

    tiles_old = distinct_tiles(old_ds)
    tiles_cur = distinct_tiles(cur_ds)

    if args.tiles_file:
        with open(args.tiles_file, "r", encoding="utf-8") as f:
            tiles = [ln.strip() for ln in f if ln.strip()]
        tiles_to_promote = [t for t in tiles if t not in tiles_cur]
    else:
        tiles_to_promote = sorted(list(tiles_old - tiles_cur))

    if args.limit > 0:
        tiles_to_promote = tiles_to_promote[: args.limit]

    if not tiles_to_promote:
        print("[INFO] No tiles to promote.")
        return

    if args.dry_run:
        print("[DRY-RUN] Tiles to promote:")
        for t in tiles_to_promote:
            print(t)
        return

    map_tbl = read_mapping_csv(args.map_csv)

    # Prepare scanners (minimal projection)
    needed_cols = ["NUMBER","tile_id","plate_id","ra_bin","dec_bin",
                   "ALPHAWIN_J2000","DELTAWIN_J2000","ALPHA_J2000","DELTA_J2000",
                   "X_WORLD","Y_WORLD","FLAGS","image_catalog_path","image_id",
                   "FLUX_AUTO","FLUXERR_AUTO","MAG_AUTO","MAGERR_AUTO","FLUX_RADIUS",
                   "CLASS_STAR","ELLIPTICITY","KRON_RADIUS","BACKGROUND","SPREAD_MODEL",
                   "SPREADERR_MODEL","SNR_WIN","FLUX_APER","FLUXERR_APER","VIGNET"]
    # Keep only those actually present
    schema_names = set(old_ds.schema.names)
    proj = [c for c in needed_cols if c in schema_names] + ["ra_bin","dec_bin","tile_id"]
    proj = list(dict.fromkeys(proj))  # de-dupe, preserve order

    # Manifest
    ts = time.strftime("%Y%m%d_%H%M%S")
    Path(args.manifest_dir).mkdir(parents=True, exist_ok=True)
    manifest_path = Path(args.manifest_dir) / f"promotion_{ts}.json"
    promoted = []

    for idx, tile in enumerate(tiles_to_promote, start=1):
        print(f"[{idx}/{len(tiles_to_promote)}] Promoting {tile} ...")
        flt = old_ds.scanner(filter=pc.field("tile_id") == tile, columns=list(set(proj + ["plate_id"])))
        tbl = flt.to_table()
        if tbl.num_rows == 0:
            print(f"  [WARN] Tile {tile}: no rows (skipped)")
            continue
        tbl = ensure_plate_id(tbl, map_tbl)
        if args.require_plate:
            empties = pc.sum(pc.if_else(pc.or_(pc.is_null(tbl["plate_id"]), pc.equal(tbl["plate_id"], "")), 1, 0)).as_py()
            if empties > 0:
                print(f"  [ERROR] Tile {tile}: {empties} rows would have empty plate_id; aborting.")
                sys.exit(2)
        rows = write_tile_parts(args.cur_root, tbl, tile)
        print(f"  [OK] {tile}: {rows} rows")
        promoted.append({"tile_id": tile, "rows": int(rows)})

    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump({
            "timestamp_utc": ts,
            "old_root": str(args.old_root),
            "cur_root": str(args.cur_root),
            "mapping_csv": str(args.map_csv),
            "require_plate": args.require_plate,
            "tiles_promoted": promoted,
            "count_tiles": len(promoted),
            "count_rows": sum(p["rows"] for p in promoted),
        }, f, indent=2)
    print(f"[DONE] Promotion manifest -> {manifest_path}")

if __name__ == "__main__":
    main()
