#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_run_stage_csvs.py
Create run-scoped CSV artifacts for Post-pipeline “shrinking set” fetchers.

Outputs (under ./work/runs/run-<date>/ by default):
 - source_extractor_final_filtered.csv (master S1, canonical schema + annotations)
 - source_extractor_final_filtered__dedup.csv (derived; dedup across tiles)
 - source_extractor_final_filtered__edge_core.csv (derived; dedup + edge-core only)
 - tile_manifest.csv (per-tile accounting + PS1 + plate-edge + plate_id)
 - stage_S1.csv (FINAL stage CSV; driven from edge-core set by default)
 - upload_positional.csv (FINAL: src_id,ra,dec) and chunked variants (<=chunk-size)
 - upload_skybot.csv (FINAL: src_id,ra,dec,epoch_mjd) and chunked variants (epoch optional)

Optional debug/audit (kept alongside finals):
 - stage_S1__raw.csv
 - upload_positional__raw.csv (+ chunks)
 - upload_skybot__raw.csv (+ chunks)

Contract (canonical schema):
 src_id = tile_id + ":" + object_id
 tile_id = tile folder name (tile-RA...-DEC...)
 object_id = internal NUMBER renamed (never emit NUMBER/number in upload CSVs)
 ra/dec = prefer WCS-fixed coords when present; else fallbacks

Edge policy (per CSV contract):
 is_core = (edge_class_px == 'core') OR (edge_class_arcsec == 'core')
 The edge cut is applied only in the derived edge_core set.

Dedup policy:
 Default dedup uses rounded (plate_id, ra, dec) key across tiles
 and prefers a core tile representative when collisions occur.
"""

import argparse
import csv
import datetime as _dt
import glob
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
# Plate map (tile_id -> plate_id / REGION)
# ----------------------------
def load_plate_map(csv_path: Path) -> Dict[str, str]:
    """
    Expect columns: tile_id + one of (irsa_region, REGION, region).
    Returns dict: tile_id -> plate_id (REGION).
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        return {}

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
    return set(
        ln.strip()
        for ln in p.read_text(encoding="utf-8", errors="ignore").splitlines()
        if ln.strip()
    )


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

    for a, b in [("ra", "dec"), ("RA_ICRS", "DE_ICRS")]:
        if a in colset and b in colset:
            return a, b
    return None


def pick_object_id_col(cols: List[str]) -> Optional[str]:
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
    chunks = []
    if len(rows) <= chunk_size:
        return chunks

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
# Derived-set helpers
# ----------------------------
def is_edge_core(row: dict) -> bool:
    """Permissive core rule: core if either class says core."""
    return (row.get("edge_class_px") == "core") or (row.get("edge_class_arcsec") == "core")


def dedup_rows_by_plate_radec_round(rows: List[dict], ndigits: int) -> Tuple[List[dict], int]:
    """
    Dedup across tiles by rounded (plate_id, ra, dec) key.
    Prefer edge-core representative when collisions occur.
    Returns (deduped_rows, dropped_count).
    """
    best: Dict[Tuple[str, float, float], dict] = {}
    dropped = 0

    for r in rows:
        plate = str(r.get("plate_id") or "")
        ra = float(r["ra"])
        dec = float(r["dec"])
        key = (plate, round(ra, ndigits), round(dec, ndigits))

        if key not in best:
            best[key] = r
            continue

        # collision: prefer edge-core over non-core
        cur = best[key]
        cur_core = is_edge_core(cur)
        new_core = is_edge_core(r)

        if (not cur_core) and new_core:
            best[key] = r
            dropped += 1
        else:
            dropped += 1

    # keep stable-ish order by re-walking original and selecting chosen rows
    chosen_ids = set(id(v) for v in best.values())
    out = [r for r in rows if id(r) in chosen_ids]
    return out, dropped


# ----------------------------
# Main
# ----------------------------
def main():
    ap = argparse.ArgumentParser(description="Build run-scoped stage CSVs for shrinking-set fetchers (CSV contract).")
    ap.add_argument("--tiles-root", default="./data/tiles_by_sky",
                    help="Tile root (flat or sharded; tiles_by_sky recommended).")
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
    ap.add_argument("--catalog-name", default="catalogs/sextractor_pass2.filtered.csv",
                    help="Relative path under tile dir to read survivors from.")

    # Derived sets controls
    ap.add_argument("--dedup-enable", action="store_true",
                    help="Enable dedup across tiles (derived __dedup + __edge_core sets).")
    ap.add_argument("--dedup-round-digits", type=int, default=6,
                    help="Dedup rounding digits for ra/dec degrees (default 6 ~ 0.36 arcsec).")
    ap.add_argument("--write-raw-stage-and-uploads", action="store_true",
                    help="Also write stage/uploads for the raw (non-dedup, non-edge-cut) set.")
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

    # manifest rows + base S1 rows
    manifest_rows: List[dict] = []
    out_rows: List[dict] = []  # base S1 rows (PS1 allowlist applied)

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
            # if no list is provided, default to include all
            return True
        return str(td) in eligible_set

    # read per-tile survivor catalogs
    for td in uniq_tiles:
        tile_id = td.name
        tile_path_str = str(td)
        ps1_ok = is_ps1_eligible(td)

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

                with cat_path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
                    dr = csv.DictReader(f)
                    for row in dr:
                        n_in += 1

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
                            # annotations
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
            "excluded_by_edge": 0,  # edge cut is applied only in derived sets
            "rows_in_tile_filtered_csv": n_in,
            "rows_emitted_to_S1": n_out,
            "notes": note,
        })

    # write manifest
    manifest_path = run_dir / "tile_manifest.csv"
    mf_fields = [
        "tile_id", "tile_path", "plate_id_map", "edge_plate_id",
        "edge_class_px", "edge_class_arcsec", "ps1_eligible", "excluded_by_edge",
        "rows_in_tile_filtered_csv", "rows_emitted_to_S1", "notes"
    ]
    with manifest_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=mf_fields)
        w.writeheader()
        w.writerows(manifest_rows)

    # de-dup src_id within base set (contract requires uniqueness per stage)
    seen = set()
    base_rows: List[dict] = []
    dup_srcid_dropped = 0
    for r in out_rows:
        sid = r["src_id"]
        if sid in seen:
            dup_srcid_dropped += 1
            continue
        seen.add(sid)
        base_rows.append(r)

    # derived sets
    dedup_rows = base_rows
    dedup_dropped = 0
    if args.dedup_enable:
        dedup_rows, dedup_dropped = dedup_rows_by_plate_radec_round(base_rows, args.dedup_round_digits)

    edge_core_rows = [r for r in dedup_rows if is_edge_core(r)]
    edge_noncore_rows = [r for r in dedup_rows if not is_edge_core(r)]

    # write master S1 (base)
    master_base_path = run_dir / "source_extractor_final_filtered.csv"
    master_fields = ["src_id", "tile_id", "object_id", "ra", "dec", "plate_id", "ps1_eligible", "edge_class_px", "edge_class_arcsec"]
    with master_base_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=master_fields)
        w.writeheader()
        w.writerows(base_rows)

    # write derived masters
    master_dedup_path = run_dir / "source_extractor_final_filtered__dedup.csv"
    with master_dedup_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=master_fields)
        w.writeheader()
        w.writerows(dedup_rows)

    master_edge_core_path = run_dir / "source_extractor_final_filtered__edge_core.csv"
    with master_edge_core_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=master_fields)
        w.writeheader()
        w.writerows(edge_core_rows)

    master_edge_noncore_path = run_dir / "source_extractor_final_filtered__edge_noncore.csv"
    with master_edge_noncore_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=master_fields)
        w.writeheader()
        w.writerows(edge_noncore_rows)

    # FINAL stage/uploads are driven from edge_core_rows
    final_rows = edge_core_rows

    # stage S1 (FINAL)
    stage_fields = ["src_id", "tile_id", "object_id", "ra", "dec"]
    stage_path = run_dir / "stage_S1.csv"
    with stage_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=stage_fields)
        w.writeheader()
        for r in final_rows:
            w.writerow({k: r[k] for k in stage_fields})

    # upload views (FINAL)
    upload_pos_fields = ["src_id", "ra", "dec"]
    upload_pos_rows = [{k: r[k] for k in upload_pos_fields} for r in final_rows]
    upload_pos_path = run_dir / "upload_positional.csv"
    write_chunks(upload_pos_rows, upload_pos_path, upload_pos_fields, args.chunk_size, "upload_positional_chunk")

    upload_sky_fields = ["src_id", "ra", "dec", "epoch_mjd"]
    upload_sky_rows = [{"src_id": r["src_id"], "ra": r["ra"], "dec": r["dec"], "epoch_mjd": ""} for r in final_rows]
    upload_sky_path = run_dir / "upload_skybot.csv"
    write_chunks(upload_sky_rows, upload_sky_path, upload_sky_fields, args.chunk_size, "upload_skybot_chunk")

    # optional raw stage/uploads
    if args.write_raw_stage_and_uploads:
        stage_raw_path = run_dir / "stage_S1__raw.csv"
        with stage_raw_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=stage_fields)
            w.writeheader()
            for r in base_rows:
                w.writerow({k: r[k] for k in stage_fields})

        upload_pos_raw_path = run_dir / "upload_positional__raw.csv"
        upload_pos_raw_rows = [{k: r[k] for k in upload_pos_fields} for r in base_rows]
        write_chunks(upload_pos_raw_rows, upload_pos_raw_path, upload_pos_fields, args.chunk_size, "upload_positional__raw_chunk")

        upload_sky_raw_path = run_dir / "upload_skybot__raw.csv"
        upload_sky_raw_rows = [{"src_id": r["src_id"], "ra": r["ra"], "dec": r["dec"], "epoch_mjd": ""} for r in base_rows]
        write_chunks(upload_sky_raw_rows, upload_sky_raw_path, upload_sky_fields, args.chunk_size, "upload_skybot__raw_chunk")

    # summary
    summary = run_dir / "RUN_SUMMARY.txt"
    summary.write_text(
        "\n".join([
            f"run_dir: {run_dir}",
            f"tiles_scanned: {len(uniq_tiles)}",
            f"tiles_manifest_rows: {len(manifest_rows)}",
            f"S1_rows_raw: {len(out_rows)}",
            f"S1_rows_unique_src_id: {len(base_rows)}",
            f"S1_src_id_duplicates_dropped: {dup_srcid_dropped}",
            f"ps1_eligible_list_present: {bool(eligible_set)}",
            f"ps1_excluded_list_present: {bool(excluded_set)}",
            f"edge_cut_policy: derived edge_core only (is_core = class_px=='core' OR class_arcsec=='core')",
            f"dedup_enabled: {bool(args.dedup_enable)}",
            f"dedup_method: plate_id + rounded(ra,dec) with ndigits={args.dedup_round_digits}",
            f"dedup_rows: {len(dedup_rows)} (dropped={dedup_dropped})",
            f"edge_core_rows: {len(edge_core_rows)}",
            f"edge_noncore_rows: {len(edge_noncore_rows)}",
            f"final_rows_for_stage_and_uploads: {len(final_rows)} (edge_core set)",
            f"edge_report_csv: {args.edge_report_csv}",
            f"plate_map_csv: {args.plate_map_csv}",
            f"master_csv: {master_base_path.name}",
            f"master_dedup_csv: {master_dedup_path.name}",
            f"master_edge_core_csv: {master_edge_core_path.name}",
            f"stage_csv: {stage_path.name}",
            f"upload_positional: {upload_pos_path.name}",
            f"upload_skybot: {upload_sky_path.name} (epoch_mjd blank; fill via epoch stage later)",
        ]) + "\n",
        encoding="utf-8"
    )

    print(f"[OK] wrote run folder: {run_dir}")
    print(f"[OK] master(base): {master_base_path} rows={len(base_rows)} (dropped_dup_src_id={dup_srcid_dropped})")
    print(f"[OK] derived: {master_dedup_path.name} rows={len(dedup_rows)}; {master_edge_core_path.name} rows={len(edge_core_rows)}")
    print(f"[OK] FINAL stage/uploads from edge_core: rows={len(final_rows)}; chunk_size={args.chunk_size}")
    print(f"[OK] manifest: {manifest_path} rows={len(manifest_rows)}")


if __name__ == "__main__":
    main()
