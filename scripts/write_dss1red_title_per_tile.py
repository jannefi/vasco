
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Write a per-tile DSS1-red title file under data/tiles/<tile>/raw/dss1red_title.txt
based on the mapping CSV produced by map_irsa_dss1red_to_tiles.py.
"""
import argparse
import csv
import os
from pathlib import Path

def bool_arg(x: str) -> bool:
    return str(x).strip().lower() in ("1", "true", "yes", "y")

def main():
    ap = argparse.ArgumentParser(description="Write per-tile DSS1-red title files from mapping CSV")
    ap.add_argument("--tiles-dir", default="./data/tiles")
    ap.add_argument("--mapping-csv", default="./data/metadata/tile_to_dss1red.csv")
    ap.add_argument("--irsa-json-dir", default="./data/dss1red_headers")
    ap.add_argument("--prefer-local-header", default="true", help="prefer ./data/tiles/<tile>/raw/*.header.json if present")
    ap.add_argument("--overwrite", default="true", help="overwrite existing dss1red_title.txt")
    args = ap.parse_args()

    tiles_root = Path(args.tiles_dir)
    irsa_json_root = Path(args.irsa_json_dir)
    prefer_local = bool_arg(args.prefer_local_header)
    overwrite = bool_arg(args.overwrite)

    mapping_csv = Path(args.mapping_csv)
    if not mapping_csv.exists():
        raise SystemExit(f"[ERROR] mapping CSV not found: {mapping_csv}")

    written = 0
    skipped = 0
    with mapping_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        required_cols = {
            "tile_id", "irsa_platelabel", "irsa_plateid", "irsa_region",
            "irsa_date_obs", "irsa_filename", "tile_fits", "irsa_center_sep_deg"
        }
        missing = [c for c in required_cols if c not in reader.fieldnames]
        if missing:
            raise SystemExit(f"[ERROR] mapping CSV missing columns: {missing}")

        for row in reader:
            tid  = row.get("tile_id", "").strip()
            if not tid:
                continue
            raw = tiles_root / tid / "raw"
            raw.mkdir(parents=True, exist_ok=True)
            title_path = raw / "dss1red_title.txt"
            if title_path.exists() and not overwrite:
                skipped += 1
                continue

            # Resolve SOURCE path (local header preferred, otherwise IRSA sidecar, otherwise base name)
            src_path_rel = ""
            tile_fits_base = row.get("tile_fits", "").strip()
            irsa_filename  = row.get("irsa_filename", "").strip()

            if prefer_local and tile_fits_base:
                local_json = raw / f"{tile_fits_base}.header.json"
                if local_json.exists():
                    src_path_rel = os.path.relpath(local_json, raw)

            if not src_path_rel and irsa_filename:
                irsa_json = irsa_json_root / f"{irsa_filename}.header.json"
                if irsa_json.exists():
                    src_path_rel = os.path.relpath(irsa_json, raw)

            if not src_path_rel:
                src_path_rel = irsa_filename if irsa_filename else ""

            # ---- Title content (adds FITS line) ----
            content_lines = [
                f"PLTLABEL: {row.get('irsa_platelabel','').strip()}",
                f"PLATEID:  {row.get('irsa_plateid','').strip()}",
                f"REGION:   {row.get('irsa_region','').strip()}",
                f"DATE-OBS: {row.get('irsa_date_obs','').strip()}",
                f"FITS:     {irsa_filename}",               # <— NEW
                f"SOURCE:   {src_path_rel}",
                f"SEP_DEG:  {row.get('irsa_center_sep_deg','').strip()}",
            ]
            title_path.write_text("\n".join(content_lines) + "\n", encoding="utf-8")
            written += 1

    print({"written": written, "skipped": skipped, "out": str(tiles_root)})

if __name__ == "__main__":
    raise SystemExit(main())
