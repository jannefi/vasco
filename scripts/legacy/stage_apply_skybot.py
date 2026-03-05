#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
stage_apply_skybot.py

Project SkyBoT flags from a *non-dedup* S1 edge-core set onto the *deduped* S1
representatives (plate_id + angular dedupe tolerance), then emit the next
shrinking-set CSV (S2) for downstream fetchers (PTF/SuperCOSMOS/etc).

Inputs:
  1) non-dedup edge-core CSV (has ra/dec, plate_id) — used to build dedup clusters
  2) dedup edge-core CSV (representatives) — used as base for S2
  3) SkyBoT parts Parquets — row-level flags keyed by src_id (tile_id:object_id)

Outputs (written into --dedup-run-dir):
  - stage_S2_skybot.csv
  - upload_positional_S2.csv + upload_positional_S2_chunk_*.csv
  - STAGE_LEDGER.csv (append-only; creates if missing)

Policy:
  - Dedup clustering uses true angular separation <= tol_arcsec per plate_id.
  - Robust neighbor search uses XYZ unit-sphere spatial hashing.
  - SkyBoT “flagged” definition:
        skybot_flagged := has_skybot_match OR wide_skybot_match
    (wide corresponds to 60 arcsec proximity screen as in the worker.)  
  - Projection rule:
        rep is flagged if ANY member of its cluster is flagged.

Notes:
  - This script does NOT change query radius or field strategy; it only shrinks the set.
"""

import argparse
import csv
import glob
import math
from pathlib import Path
from typing import Dict, List, Tuple, Set, Optional

import pandas as pd


def angsep_arcsec(ra1_deg: float, dec1_deg: float, ra2_deg: float, dec2_deg: float) -> float:
    """Great-circle separation in arcsec using haversine (stable for small angles)."""
    ra1 = math.radians(ra1_deg % 360.0)
    ra2 = math.radians(ra2_deg % 360.0)
    dec1 = math.radians(dec1_deg)
    dec2 = math.radians(dec2_deg)
    dra = ra2 - ra1
    ddec = dec2 - dec1
    s1 = math.sin(ddec / 2.0)
    s2 = math.sin(dra / 2.0)
    a = s1 * s1 + math.cos(dec1) * math.cos(dec2) * s2 * s2
    a = min(1.0, max(0.0, a))
    c = 2.0 * math.asin(math.sqrt(a))
    return math.degrees(c) * 3600.0


def radec_to_unit_xyz(ra_deg: float, dec_deg: float) -> Tuple[float, float, float]:
    """RA/Dec (deg) -> unit sphere XYZ."""
    ra = math.radians(ra_deg % 360.0)
    dec = math.radians(dec_deg)
    cosd = math.cos(dec)
    x = cosd * math.cos(ra)
    y = cosd * math.sin(ra)
    z = math.sin(dec)
    return x, y, z


def tol_arcsec_to_chord(tol_arcsec: float) -> float:
    """Angular tolerance -> chord length on unit sphere."""
    tol_rad = (tol_arcsec / 3600.0) * (math.pi / 180.0)
    return 2.0 * math.sin(tol_rad / 2.0)


class UnionFind:
    def __init__(self, n: int):
        self.p = list(range(n))
        self.rank = [0] * n

    def find(self, a: int) -> int:
        while self.p[a] != a:
            self.p[a] = self.p[self.p[a]]
            a = self.p[a]
        return a

    def union(self, a: int, b: int):
        ra = self.find(a)
        rb = self.find(b)
        if ra == rb:
            return
        if self.rank[ra] < self.rank[rb]:
            self.p[ra] = rb
        elif self.rank[ra] > self.rank[rb]:
            self.p[rb] = ra
        else:
            self.p[rb] = ra
            self.rank[ra] += 1


def load_edge_core_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    need = ["src_id", "tile_id", "object_id", "ra", "dec", "plate_id", "ps1_eligible", "edge_class_px", "edge_class_arcsec"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise SystemExit(f"[ERROR] missing columns in {path}: {missing}")
    # normalize
    df["src_id"] = df["src_id"].astype(str)
    df["tile_id"] = df["tile_id"].astype(str)
    df["plate_id"] = df["plate_id"].astype(str)
    df["ra"] = df["ra"].astype(float)
    df["dec"] = df["dec"].astype(float)
    return df


def choose_rep_src_id(cluster_src_ids: List[str]) -> str:
    """
    Deterministic representative choice for a cluster.
    Since the deduped run already chose a rep, this is only used as a fallback;
    we prefer to map to the actual rep present in the dedup CSV when possible.
    """
    return sorted(cluster_src_ids)[0]


def build_srcid_to_rep_mapping(nondedup_df: pd.DataFrame, dedup_df: pd.DataFrame, tol_arcsec: float) -> Dict[str, str]:
    """
    Build mapping: every non-dedup src_id -> rep src_id (as present in dedup_df if possible).
    Clustering is done per plate_id by angular separation <= tol_arcsec using XYZ spatial hashing.
    """
    tol_arcsec = float(tol_arcsec)
    cell = tol_arcsec_to_chord(tol_arcsec)
    if cell <= 0:
        raise SystemExit("[ERROR] invalid tol_arcsec")

    reps_set: Set[str] = set(dedup_df["src_id"].tolist())

    mapping: Dict[str, str] = {}

    for plate_id, sub in nondedup_df.groupby("plate_id", sort=False):
        rows = sub.reset_index(drop=True)
        n = len(rows)
        if n == 0:
            continue
        if n == 1:
            sid = rows.loc[0, "src_id"]
            mapping[sid] = sid if sid in reps_set else sid
            continue

        xyz = [radec_to_unit_xyz(rows.loc[i, "ra"], rows.loc[i, "dec"]) for i in range(n)]
        bins: Dict[Tuple[int, int, int], List[int]] = {}

        def key(x: float, y: float, z: float) -> Tuple[int, int, int]:
            return int(x / cell), int(y / cell), int(z / cell)

        uf = UnionFind(n)

        for i in range(n):
            ra_i = float(rows.loc[i, "ra"])
            dec_i = float(rows.loc[i, "dec"])
            x, y, z = xyz[i]
            ix, iy, iz = key(x, y, z)

            for dx in (-1, 0, 1):
                for dy in (-1, 0, 1):
                    for dz in (-1, 0, 1):
                        cand = bins.get((ix + dx, iy + dy, iz + dz))
                        if not cand:
                            continue
                        for j in cand:
                            ra_j = float(rows.loc[j, "ra"])
                            dec_j = float(rows.loc[j, "dec"])
                            if angsep_arcsec(ra_i, dec_i, ra_j, dec_j) <= tol_arcsec:
                                uf.union(i, j)

            bins.setdefault((ix, iy, iz), []).append(i)

        comps: Dict[int, List[int]] = {}
        for i in range(n):
            root = uf.find(i)
            comps.setdefault(root, []).append(i)

        for members in comps.values():
            cluster_src = [rows.loc[i, "src_id"] for i in members]
            # Prefer a representative that exists in the deduped CSV
            rep_candidates = [sid for sid in cluster_src if sid in reps_set]
            if rep_candidates:
                rep = sorted(rep_candidates)[0]
            else:
                rep = choose_rep_src_id(cluster_src)
            for sid in cluster_src:
                mapping[sid] = rep

    return mapping


def load_skybot_flags_parts(parts_glob: str) -> pd.DataFrame:
    """
    Load SkyBoT parts and reduce to (src_id, skybot_flagged).
    skybot_flagged := has_skybot_match OR wide_skybot_match
    Parts schema is produced by skybot_fetch_chunk.py.  
    """
    files = sorted(glob.glob(parts_glob))
    if not files:
        raise SystemExit(f"[ERROR] no SkyBoT parts found at: {parts_glob}")
    df = pd.read_parquet(files)
    need = ["src_id", "has_skybot_match", "wide_skybot_match"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise SystemExit(f"[ERROR] SkyBoT parts missing columns: {missing}")
    df["src_id"] = df["src_id"].astype(str)
    df["skybot_flagged"] = df["has_skybot_match"].fillna(False).astype(bool) | df["wide_skybot_match"].fillna(False).astype(bool)
    return df[["src_id", "skybot_flagged"]]


def append_stage_ledger(run_dir: Path, stage_name: str, rows_in: int, rows_flagged: int, rows_out: int, notes: str = ""):
    ledger_path = run_dir / "STAGE_LEDGER.csv"
    exists = ledger_path.exists()
    fieldnames = ["ts_utc", "stage", "rows_in", "rows_flagged", "rows_out", "notes"]
    row = {
        "ts_utc": pd.Timestamp.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "stage": stage_name,
        "rows_in": int(rows_in),
        "rows_flagged": int(rows_flagged),
        "rows_out": int(rows_out),
        "notes": notes,
    }
    with ledger_path.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if not exists:
            w.writeheader()
        w.writerow(row)


def write_chunks(rows: List[dict], out_path: Path, fieldnames: List[str], chunk_size: int, chunk_prefix: str):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    if len(rows) <= chunk_size:
        return

    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        idx = (i // chunk_size) + 1
        chunk_path = out_path.with_name(f"{chunk_prefix}_{idx:07d}.csv")
        with chunk_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(chunk)


def main():
    ap = argparse.ArgumentParser(description="Apply SkyBoT flags to deduped survivors and emit next shrinking set (S2).")
    ap.add_argument("--nondedup-run-dir", required=True, help="Run dir containing non-dedup edge-core CSV (for mapping).")
    ap.add_argument("--dedup-run-dir", required=True, help="Run dir containing dedup edge-core CSV (representatives).")
    ap.add_argument("--tol-arcsec", type=float, default=0.25, help="Dedup tolerance arcsec for mapping (default 0.25).")
    ap.add_argument("--chunk-size", type=int, default=2000, help="Chunk size for next-stage upload CSVs.")
    ap.add_argument("--skybot-parts-glob", default="work/scos_chunks/skybot/parts/flags_skybot__*.parquet",
                    help="Glob for SkyBoT parts parquet files.")
    ap.add_argument("--stage-name", default="S2_skybot", help="Stage name for ledger.")
    args = ap.parse_args()

    nondedup_run = Path(args.nondedup_run_dir)
    dedup_run = Path(args.dedup_run_dir)

    nd_csv = nondedup_run / "source_extractor_final_filtered__edge_core.csv"
    d_csv = dedup_run / "source_extractor_final_filtered__edge_core.csv"
    if not nd_csv.exists():
        raise SystemExit(f"[ERROR] missing non-dedup CSV: {nd_csv}")
    if not d_csv.exists():
        raise SystemExit(f"[ERROR] missing dedup CSV: {d_csv}")

    print(f"[INFO] loading non-dedup edge_core: {nd_csv}")
    nd = load_edge_core_csv(nd_csv)
    print(f"[INFO] non-dedup rows: {len(nd)}")

    print(f"[INFO] loading dedup edge_core: {d_csv}")
    d = load_edge_core_csv(d_csv)
    print(f"[INFO] dedup rows: {len(d)}")

    print(f"[INFO] loading SkyBoT parts: {args.skybot_parts_glob}")
    sb = load_skybot_flags_parts(args.skybot_parts_glob)
    print(f"[INFO] skybot part rows: {len(sb)} (distinct src_id={sb['src_id'].nunique()})")

    # Build mapping old src_id -> rep src_id
    print(f"[INFO] building src_id -> rep mapping (tol={args.tol_arcsec}\") …")
    mapping = build_srcid_to_rep_mapping(nd, d, args.tol_arcsec)
    print(f"[INFO] mapping size: {len(mapping)}")

    # Map SkyBoT flags to reps and OR within reps
    sb["rep_src_id"] = sb["src_id"].map(mapping)
    # Any missing mapping means those src_id weren't in nd CSV; keep as itself
    sb["rep_src_id"] = sb["rep_src_id"].fillna(sb["src_id"])
    rep_flags = sb.groupby("rep_src_id", as_index=False)["skybot_flagged"].any()
    rep_flags = rep_flags.rename(columns={"rep_src_id": "src_id"})

    # Join onto dedup survivors and shrink
    merged = d.merge(rep_flags, on="src_id", how="left")
    merged["skybot_flagged"] = merged["skybot_flagged"].fillna(False).astype(bool)

    rows_in = len(merged)
    rows_flagged = int(merged["skybot_flagged"].sum())
    survivors = merged.loc[~merged["skybot_flagged"]].copy()
    rows_out = len(survivors)

    print(f"[RESULT] rows_in(dedup_edge_core)={rows_in}  skybot_flagged={rows_flagged}  rows_out={rows_out}")

    # Write stage_S2 CSV (full schema kept for audit)
    stage_s2_path = dedup_run / "stage_S2_skybot.csv"
    cols_out = ["src_id", "tile_id", "object_id", "ra", "dec", "plate_id", "ps1_eligible", "edge_class_px", "edge_class_arcsec"]
    survivors[cols_out].to_csv(stage_s2_path, index=False)
    print(f"[OK] wrote: {stage_s2_path}")

    # Write upload_positional for next stage + chunks (minimal view)
    upload_fields = ["src_id", "ra", "dec"]
    upload_rows = survivors[upload_fields].to_dict(orient="records")
    upload_path = dedup_run / "upload_positional_S2.csv"
    write_chunks(upload_rows, upload_path, upload_fields, args.chunk_size, "upload_positional_S2_chunk")
    print(f"[OK] wrote: {upload_path} (+ chunks if needed)")

    # Ledger
    append_stage_ledger(dedup_run, args.stage_name, rows_in, rows_flagged, rows_out,
                        notes=f"SkyBoT flagged := has_skybot_match OR wide_skybot_match; projected via plate_id+<= {args.tol_arcsec}\" mapping")
    print(f"[OK] updated: {dedup_run/'STAGE_LEDGER.csv'}")


if __name__ == "__main__":
    main()
