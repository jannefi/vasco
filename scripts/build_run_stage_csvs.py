#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_run_stage_csvs.py

Create run-scoped CSV artifacts for Post-pipeline “shrinking set” fetchers.

Outputs (under ./work/runs/run-<date>/ by default):
  - source_extractor_final_filtered.csv   (master S1, canonical schema + optional annotations)
  - tile_manifest.csv                    (per-tile accounting + PS1 + plate-edge + plate_id)
  - stage_S1.csv                         (canonical stage CSV; identical content to master unless --include-annotations differs)
  - upload_positional.csv                (src_id,ra,dec) and chunked variants (<=2000 rule)
  - upload_skybot.csv                    (src_id,ra,dec,epoch_mjd) and chunked variants (epoch optional)

Contract (canonical schema):
  src_id   = tile_id + ":" + object_id
  tile_id  = tile folder name (tile-RA...-DEC...)
  object_id = internal NUMBER renamed (never emit NUMBER/number in upload CSVs)
  ra/dec   = prefer WCS-fixed coords when present; else fallbacks

Notes:
- PRE-DEDUP and PRE-PLATE-EDGE CUT by default. We only *annotate* edge class from
  data/metadata/tile_plate_edge_report.csv. Optionally you can exclude later via --exclude-edge.
- PS1-eligibility filtering can be driven by allow/exclude list files (recommended).
"""

import argparse
import csv
import datetime as _dt
import os
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple


# ----------------------------
# Tile discovery (flat + sharded)
# ----------------------------
_PATTERNS = [
    "tile-RA*-DEC*",
    "tile_RA*_DEC*",
    "tile-RA*_DEC*",
    "tile_RA*-DEC*",
]

def iter_tile_dirs(tiles_root: Path) -> Iterable[Path]:
    """Yield tile dirs under tiles_root (flat) and tiles_by_sky sibling (sharded)."""
    tiles_root = Path(tiles_root)
    if tiles_root.exists():
        # direct tile dir
        if tiles_root.is_dir() and tiles_root.name.startswith("tile-RA"):
            yield tiles_root
        # flat
        for pat in _PATTERNS:
            for p in sorted(tiles_root.glob(pat)):
                if p.is_dir():
                    yield p

    sharded = tiles_root.parent / "tiles_by_sky"
    if sharded.exists():
        for pat in _PATTERNS:
            for p in sorted(sharded.glob(f"ra_bin=*/dec_bin=*/{pat}")):
                if p.is_dir():
                    yield p


# ----------------------------
# Plate map (tile_id -> plate_id / REGION)  (reuse logic from merge_tile_catalogs.py)
# ----------------------------
def load_plate_map(csv_path: Path) -> Dict[str, str]:
    """
    Expect columns: tile_id + one of (irsa_region, REGION, region).
    Returns dict: tile_id -> plate_id (REGION).
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        return {}

    # detect REGION column name
    with csv_path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        r = csv.reader(f)
        hdr = next(r, [])
    cols = [c.strip() for c in hdr]
    region_col = None
    for cand in ("irsa_region", "REGION", "region"):
        if cand in cols:
            region_col = cand
            break
    if region_col is None or "tile_id" not in cols:
        return {}

    idx_tile = cols.index("tile_id")
    idx_reg = cols.index(region_col)

    out: Dict[str, str] = {}
    with csv_path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        r = csv.reader(f)
        _ = next(r, None)
        for row in r:
            if not row or len(row) <= max(idx_tile, idx_reg):
                continue
            tid = str(row[idx_tile]).strip()
            reg = str(row[idx_reg]).strip()
            if tid:
                out[tid] = reg
    return out


# ----------------------------
# Plate edge report loader (tile_id -> edge class/metrics)
# ----------------------------
def load_edge_report(csv_path: Path) -> Dict[str, dict]:
    """
    Reads data/metadata/tile_plate_edge_report.csv.
    Returns dict: tile_id -> row dict (subset of fields).
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        return {}
    out: Dict[str, dict] = {}
    with csv_path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            tid = (row.get("tile_id") or "").strip()
            if not tid:
                continue
            out[tid] = {
                "plate_id_edge": (row.get("plate_id") or "").strip(),
                "class_px": (row.get("class_px") or "").strip(),
                "class_arcsec": (row.get("class_arcsec") or "").strip(),
                "min_edge_dist_px": (row.get("min_edge_dist_px") or "").strip(),
                "min_edge_dist_arcsec": (row.get("min_edge_dist_arcsec") or "").strip(),
                "notes_edge": (row.get("notes") or "").strip(),
            }
    return out


# ----------------------------
# PS1 eligibility lists
# ----------------------------
def read_list_file(path: Optional[str]) -> Set[str]:
    if not path:
        return set()
    p = Path(path)
    if not p.exists():
        return set()
    return set(ln.strip() for ln in p.read_text(encoding="utf-8", errors="ignore").splitlines() if ln.strip())


# ----------------------------
# RA/Dec column picking
# ----------------------------
_RA_CANDS = ["RA_corr", "RA_CORR", "ALPHAWIN_J2000", "ALPHA_J2000", "RA", "X_WORLD", "RAJ2000"]
_DEC_CANDS = ["Dec_corr", "DEC_corr", "DEC_CORR", "DELTAWIN_J2000", "DELTA_J2000", "DEC", "Y_WORLD", "DEJ2000"]

def detect_header_cols(csv_path: Path) -> List[str]:
    with csv_path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        r = csv.reader(f)
        return [c.strip() for c in next(r, [])]

def pick_radec_cols(cols: List[str]) -> Optional[Tuple[str, str]]:
    colset = set(cols)
    ra = next((c for c in _RA_CANDS if c in colset), None)
    dec = next((c for c in _DEC_CANDS if c in colset), None)
    if ra and dec:
        return ra, dec
    # fallback pairs sometimes appear
    for a, b in [("ra", "dec"), ("RA_ICRS", "DE_ICRS")]:
        if a in colset and b in colset:
            return a, b
    return None

def pick_object_id_col(cols: List[str]) -> Optional[str]:
    # internal catalog typically has NUMBER; sometimes objectnumber etc.
    for cand in ("NUMBER", "number", "object_id", "objectnumber", "objID"):
        if cand in cols:
            return cand
    return None


# ----------------------------
# Chunk writer
# ----------------------------
def write_chunks(rows: List[dict], out_path: Path, fieldnames: List[str], chunk_size: int, chunk_prefix: str):
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # full file
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    # chunk files
    if len(rows) <= chunk_size:
        return []

    chunks = []
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        idx = (i // chunk_size) + 1
        chunk_path = out_path.with_name(f"{chunk_prefix}_{idx:07d}.csv")
        with chunk_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(chunk)
        chunks.append(chunk_path)
    return chunks


# ----------------------------
# Main
# ----------------------------
def main():
    ap = argparse.ArgumentParser(description="Build run-scoped stage CSVs for shrinking-set fetchers (CSV contract).")
    ap.add_argument("--tiles-root", default="./data/tiles_by_sky", help="Tile root (flat or sharded; tiles_by_sky recommended).")
    ap.add_argument("--edge-report-csv", default="./data/metadata/tile_plate_edge_report.csv",
                    help="Tile-plate edge report CSV (annotation source).")
    ap.add_argument("--plate-map-csv", default="./data/metadata/tile_to_dss1red.csv",
                    help="Mapping CSV with tile_id -> irsa_region/REGION used as plate_id.")
    ap.add_argument("--ps1-eligible-list", default="./work/triage/tiles_ps1_eligible.txt",
                    help="Allowlist of PS1-eligible tile directories (recommended).")
    ap.add_argument("--ps1-excluded-list", default="./work/triage/tiles_ps1_excluded.txt",
                    help="List of PS1-excluded tile directories (for provenance/reporting).")
    ap.add_argument("--run-root", default="./work/runs", help="Root for run folders.")
    ap.add_argument("--run-tag", default="", help="Optional run tag. Default: timestamp (run-YYYYMMDD_HHMMSS).")
    ap.add_argument("--chunk-size", type=int, default=2000, help="Chunk size for upload/stage files.")
    ap.add_argument("--exclude-edge", action="store_true",
                    help="OPTIONAL: exclude tiles classified as near_edge/edge_touch/off_plate (deferred by policy; default off).")
    ap.add_argument("--edge-class-field", choices=["class_px", "class_arcsec"], default="class_px",
                    help="Which edge class to use if --exclude-edge is enabled.")
    ap.add_argument("--catalog-name", default="catalogs/sextractor_pass2.filtered.csv",
                    help="Relative path under tile dir to read survivors from.")
    args = ap.parse_args()

    # run folder
    ts = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_tag = args.run_tag.strip() or f"run-{ts}"
    run_dir = Path(args.run_root) / run_tag
    run_dir.mkdir(parents=True, exist_ok=True)

    # provenance copies
    eligible_set = read_list_file(args.ps1_eligible_list)
    excluded_set = read_list_file(args.ps1_excluded_list)

    if eligible_set:
        (run_dir / "tiles_ps1_eligible.txt").write_text("\n".join(sorted(eligible_set)) + "\n", encoding="utf-8")
    if excluded_set:
        (run_dir / "tiles_ps1_excluded.txt").write_text("\n".join(sorted(excluded_set)) + "\n", encoding="utf-8")

    # mappings
    plate_map = load_plate_map(Path(args.plate_map_csv))
    edge_map = load_edge_report(Path(args.edge_report_csv))

    # manifest rows
    manifest_rows = []
    out_rows = []  # canonical stage rows (S1)

    tiles = list(iter_tile_dirs(Path(args.tiles_root)))
    # de-dup tile dirs by tile_id to avoid double walks if both flat+sharded yield same
    seen_tile_ids = set()
    uniq_tiles = []
    for td in tiles:
        if td.name.startswith("tile-RA") and td.name not in seen_tile_ids:
            seen_tile_ids.add(td.name)
            uniq_tiles.append(td)

    # helper: decide if tile is PS1-eligible
    def is_ps1_eligible(td: Path) -> bool:
        if not eligible_set:
            # if no list is provided, default to "include all" (policy can be applied later)
            return True
        return str(td) in eligible_set

    # optional edge exclusion
    def edge_excluded(tile_id: str) -> bool:
        if not args.exclude_edge:
            return False
        rec = edge_map.get(tile_id, {})
        cls = (rec.get(args.edge_class_field) or "").strip()
        return cls in ("near_edge", "edge_touch", "off_plate")

    # read per-tile survivor catalogs
    for td in uniq_tiles:
        tile_id = td.name
        tile_path_str = str(td)

        ps1_ok = is_ps1_eligible(td)
        edge_drop = edge_excluded(tile_id)

        cat_path = td / args.catalog_name
        n_in = 0
        n_out = 0
        note = ""

        plate_id = plate_map.get(tile_id, "")
        edge_rec = edge_map.get(tile_id, {})

        if not cat_path.exists() or cat_path.stat().st_size == 0:
            note = "missing/empty survivors csv"
        else:
            cols = detect_header_cols(cat_path)
            radec = pick_radec_cols(cols)
            objcol = pick_object_id_col(cols)

            if not radec:
                note = "missing RA/Dec columns"
            elif not objcol:
                note = "missing object id column (NUMBER)"
            else:
                ra_col, dec_col = radec
                # stream rows to avoid loading huge files in memory
                with cat_path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
                    dr = csv.DictReader(f)
                    for row in dr:
                        n_in += 1
                        if edge_drop:
                            continue
                        if not ps1_ok:
                            continue

                        try:
                            obj_raw = row.get(objcol, "")
                            object_id = int(float(obj_raw))  # tolerate "1234.0"
                        except Exception:
                            continue
                        try:
                            ra = float(row.get(ra_col, "nan"))
                            dec = float(row.get(dec_col, "nan"))
                        except Exception:
                            continue

                        src_id = f"{tile_id}:{object_id}"
                        out_rows.append({
                            "src_id": src_id,
                            "tile_id": tile_id,
                            "object_id": object_id,
                            "ra": ra,
                            "dec": dec,
                            # annotations (kept in master CSV; not used in upload views)
                            "plate_id": plate_id,
                            "ps1_eligible": 1 if ps1_ok else 0,
                            "edge_class_px": edge_rec.get("class_px", ""),
                            "edge_class_arcsec": edge_rec.get("class_arcsec", ""),
                        })
                        n_out += 1

        manifest_rows.append({
            "tile_id": tile_id,
            "tile_path": tile_path_str,
            "plate_id_map": plate_id,
            "edge_plate_id": edge_rec.get("plate_id_edge", ""),
            "edge_class_px": edge_rec.get("class_px", ""),
            "edge_class_arcsec": edge_rec.get("class_arcsec", ""),
            "ps1_eligible": 1 if ps1_ok else 0,
            "excluded_by_edge": 1 if edge_drop else 0,
            "rows_in_tile_filtered_csv": n_in,
            "rows_emitted_to_S1": n_out,
            "notes": note,
        })

    # write manifest
    manifest_path = run_dir / "tile_manifest.csv"
    mf_fields = [
        "tile_id","tile_path","plate_id_map","edge_plate_id",
        "edge_class_px","edge_class_arcsec","ps1_eligible","excluded_by_edge",
        "rows_in_tile_filtered_csv","rows_emitted_to_S1","notes"
    ]
    with manifest_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=mf_fields)
        w.writeheader()
        w.writerows(manifest_rows)

    # de-dup src_id within this run (contract requires uniqueness per stage)
    # keep first occurrence (pre-dedup policy; later stages may do scientific dedupe)
    seen = set()
    unique_rows = []
    dup_count = 0
    for r in out_rows:
        sid = r["src_id"]
        if sid in seen:
            dup_count += 1
            continue
        seen.add(sid)
        unique_rows.append(r)

    # master S1 file with annotations
    master_path = run_dir / "source_extractor_final_filtered.csv"
    master_fields = ["src_id","tile_id","object_id","ra","dec","plate_id","ps1_eligible","edge_class_px","edge_class_arcsec"]
    with master_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=master_fields)
        w.writeheader()
        w.writerows(unique_rows)

    # stage S1 canonical (same as master but without the annotation columns if desired)
    stage_path = run_dir / "stage_S1.csv"
    stage_fields = ["src_id","tile_id","object_id","ra","dec"]
    stage_rows = [{k: r[k] for k in stage_fields} for r in unique_rows]
    with stage_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=stage_fields)
        w.writeheader()
        w.writerows(stage_rows)

    # upload views
    upload_pos_fields = ["src_id","ra","dec"]
    upload_pos_rows = [{k: r[k] for k in upload_pos_fields} for r in unique_rows]
    upload_pos_path = run_dir / "upload_positional.csv"
    write_chunks(upload_pos_rows, upload_pos_path, upload_pos_fields, args.chunk_size, "upload_positional_chunk")

    # SkyBoT upload view (epoch placeholder)
    upload_sky_fields = ["src_id","ra","dec","epoch_mjd"]
    upload_sky_rows = [{"src_id": r["src_id"], "ra": r["ra"], "dec": r["dec"], "epoch_mjd": ""} for r in unique_rows]
    upload_sky_path = run_dir / "upload_skybot.csv"
    write_chunks(upload_sky_rows, upload_sky_path, upload_sky_fields, args.chunk_size, "upload_skybot_chunk")

    # summary
    summary = run_dir / "RUN_SUMMARY.txt"
    summary.write_text(
        "\n".join([
            f"run_dir: {run_dir}",
            f"tiles_scanned: {len(uniq_tiles)}",
            f"tiles_manifest_rows: {len(manifest_rows)}",
            f"S1_rows_raw: {len(out_rows)}",
            f"S1_rows_unique_src_id: {len(unique_rows)}",
            f"S1_src_id_duplicates_dropped: {dup_count}",
            f"ps1_eligible_list_present: {bool(eligible_set)}",
            f"ps1_excluded_list_present: {bool(excluded_set)}",
            f"exclude_edge_enabled: {bool(args.exclude_edge)} (policy says deferred; default off)",
            f"edge_report_csv: {args.edge_report_csv}",
            f"plate_map_csv: {args.plate_map_csv}",
            f"master_csv: {master_path.name}",
            f"stage_csv: {stage_path.name}",
            f"upload_positional: {upload_pos_path.name}",
            f"upload_skybot: {upload_sky_path.name} (epoch_mjd blank; fill via epoch stage later)",
        ]) + "\n",
        encoding="utf-8"
    )

    print(f"[OK] wrote run folder: {run_dir}")
    print(f"[OK] master: {master_path} rows={len(unique_rows)} (dropped_dup_src_id={dup_count})")
    print(f"[OK] manifest: {manifest_path} rows={len(manifest_rows)}")
    print(f"[OK] uploads: {upload_pos_path.name}, {upload_sky_path.name} (chunk_size={args.chunk_size})")


if __name__ == "__main__":
    main()
