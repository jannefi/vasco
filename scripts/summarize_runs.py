#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Summarize VASCO runs to compact MD + CSVs (FAST v3 + IR flags),
layout-aware: supports ./data/tiles and ./data/tiles_by_sky.

Fixes (2026-02):
- Fix pandas IndexingError: "Unalignable boolean Series provided as indexer"
  by ensuring boolean masks are aligned to df.index.
- Fix IR any>=5 SNR calculation (use boolean OR).
- Support new Post 1.6 NEOWISE sidecar schema:
  (tile_id, NUMBER, has_ir_match, dist_arcsec) in addition to legacy schemas.
"""

import argparse
import glob
import csv
from pathlib import Path
from typing import Dict, List, Tuple, Iterable
import numpy as np
import pandas as pd

# ---------- layout helpers ----------
def iter_tile_dirs_any(data_dir: Path) -> Iterable[Path]:
    """Yield tile directories from flat and sharded layouts."""
    tiles_flat = data_dir / "tiles"
    tiles_sharded = data_dir / "tiles_by_sky"
    if tiles_flat.exists():
        for p in sorted(tiles_flat.glob("tile-*")):
            if p.is_dir():
                yield p
    if tiles_sharded.exists():
        for p in sorted(tiles_sharded.glob("ra_bin=*/dec_bin=*/tile-*")):
            if p.is_dir():
                yield p

# ---------- existing helpers over CSVs ----------
def rows_minus_header(path: str) -> int:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            n = sum(1 for _ in f)
        return max(0, n - 1)
    except Exception:
        return 0

def glob_sum(patterns):
    if isinstance(patterns, str):
        patterns = [patterns]
    total = 0
    for pat in patterns:
        for p in glob.glob(pat):
            total += rows_minus_header(p)
    return total

# ---------- per-tile summaries ----------
def summarize_tile(tile_dir: Path) -> Dict:
    catalogs = tile_dir / "catalogs"
    xmatch = tile_dir / "xmatch"

    det = glob_sum(str(catalogs / "sextractor_pass2.csv"))

    gaia_match = glob_sum([
        str(xmatch / "sex_gaia_xmatch_cdss_within5arcsec.csv"),
        str(xmatch / "sex_gaia_xmatch_cdss.csv"),
        str(xmatch / "sex_gaia_xmatch.csv"),
        str(xmatch / "sex_gaia_xmatch_cdss_within5arcsec_within5arcsec.csv"),
    ])
    ps1_match = glob_sum([
        str(xmatch / "sex_ps1_xmatch_cdss_within5arcsec.csv"),
        str(xmatch / "sex_ps1_xmatch_cdss.csv"),
        str(xmatch / "sex_ps1_xmatch.csv"),
        str(xmatch / "sex_ps1_xmatch_cdss_within5arcsec_within5arcsec.csv"),
    ])

    gaia_ids = glob_sum(str(xmatch / "gaia_ids.csv"))
    ps1_ids  = glob_sum(str(xmatch / "ps1_ids.csv"))
    any_ids  = glob_sum(str(xmatch / "matched_any_ids.csv"))
    any_ids_u = glob_sum(str(xmatch / "matched_any_ids_unique.csv"))

    gaia_un_cdss = glob_sum(str(xmatch / "sex_gaia_unmatched_cdss.csv"))
    ps1_un_cdss  = glob_sum(str(xmatch / "sex_ps1_unmatched_cdss.csv"))
    gaia_un_cdss_pos = glob_sum(str(xmatch / "sex_gaia_unmatched_cdss_pos.csv"))
    ps1_un_cdss_pos  = glob_sum(str(xmatch / "sex_ps1_unmatched_cdss_pos.csv"))
    gaia_un_local = glob_sum(str(xmatch / "sex_gaia_unmatched.csv"))
    ps1_un_local  = glob_sum(str(xmatch / "sex_ps1_unmatched.csv"))
    usnob_un = glob_sum(str(xmatch / "sex_usnob_unmatched.csv"))

    final_no_opt = glob_sum(str(xmatch / "no_optical_counterparts.csv"))

    return {
        "tile_id": tile_dir.name,
        "detections": det,
        "gaia_matched": gaia_match,
        "ps1_matched": ps1_match,
        "gaia_ids": gaia_ids,
        "ps1_ids": ps1_ids,
        "matched_any_ids": any_ids,
        "matched_any_ids_unique": any_ids_u,
        "gaia_unmatched_cdss": gaia_un_cdss,
        "ps1_unmatched_cdss": ps1_un_cdss,
        "gaia_unmatched_cdss_pos": gaia_un_cdss_pos,
        "ps1_unmatched_cdss_pos": ps1_un_cdss_pos,
        "gaia_unmatched_local": gaia_un_local,
        "ps1_unmatched_local": ps1_un_local,
        "usnob_unmatched": usnob_un,
        "final_no_optical_counterparts": final_no_opt,
    }

def summarize_tiles_root(tiles_root: Path) -> Tuple[Dict, List[str], List[Dict]]:
    tile_dirs = list(iter_tile_dirs_any(tiles_root.parent))
    per_tile_counts = [summarize_tile(td) for td in tile_dirs]

    agg = {
        "detections": 0,
        "gaia_matched": 0, "ps1_matched": 0,
        "gaia_ids": 0, "ps1_ids": 0,
        "matched_any_ids": 0, "matched_any_ids_unique": 0,
        "gaia_unmatched_cdss": 0, "ps1_unmatched_cdss": 0,
        "gaia_unmatched_cdss_pos": 0, "ps1_unmatched_cdss_pos": 0,
        "gaia_unmatched_local": 0, "ps1_unmatched_local": 0, "usnob_unmatched": 0,
        "final_no_optical_counterparts": 0,
        "tiles_total": len(tile_dirs),
        "tiles_with_catalogs": 0,
        "tiles_with_xmatch": 0,
        "tiles_with_final": 0,
        "tiles_with_wcsfix": 0,
    }

    for td in tile_dirs:
        has_catalog = (td / "catalogs" / "sextractor_pass2.csv").exists()
        xm_dir = td / "xmatch"
        has_xmatch = xm_dir.exists() and any(xm_dir.glob("sex_*_xmatch*.csv"))
        has_final = (xm_dir / "no_optical_counterparts.csv").exists()
        if has_catalog:
            agg["tiles_with_catalogs"] += 1
        if has_xmatch:
            agg["tiles_with_xmatch"] += 1
        if has_final:
            agg["tiles_with_final"] += 1
        if (td / "final_catalog_wcsfix.csv").exists():
            agg["tiles_with_wcsfix"] += 1

    for r in per_tile_counts:
        for k in [
            "detections", "gaia_matched", "ps1_matched",
            "gaia_ids", "ps1_ids",
            "matched_any_ids", "matched_any_ids_unique",
            "gaia_unmatched_cdss", "ps1_unmatched_cdss",
            "gaia_unmatched_cdss_pos", "ps1_unmatched_cdss_pos",
            "gaia_unmatched_local", "ps1_unmatched_local", "usnob_unmatched",
            "final_no_optical_counterparts",
        ]:
            agg[k] += r.get(k, 0)

    def pct(n, d): return (100.0 * n / d) if (d > 0 and n >= 0) else 0.0
    agg.update({
        "matched_any_ids_unique_pct": pct(agg["matched_any_ids_unique"], agg["detections"]),
        "gaia_ids_pct": pct(agg["gaia_ids"], agg["detections"]),
        "ps1_ids_pct": pct(agg["ps1_ids"], agg["detections"]),
        "gaia_unmatched_cdss_pct": pct(agg["gaia_unmatched_cdss"], agg["detections"]),
        "ps1_unmatched_cdss_pct": pct(agg["ps1_unmatched_cdss"], agg["detections"]),
        "final_no_optical_counterparts_pct": pct(agg["final_no_optical_counterparts"], agg["detections"]),
        "gaia_matched_pct": pct(agg["gaia_matched"], agg["detections"]),
        "ps1_matched_pct": pct(agg["ps1_matched"], agg["detections"]),
        "gaia_unmatched_local_pct": pct(agg["gaia_unmatched_local"], agg["detections"]),
        "ps1_unmatched_local_pct": pct(agg["ps1_unmatched_local"], agg["detections"]),
        "usnob_unmatched_pct": pct(agg["usnob_unmatched"], agg["detections"]),
    })

    tile_names = [r["tile_id"] for r in per_tile_counts]
    return agg, tile_names, per_tile_counts

# ---------- IR flags summary ----------
def safe_num(df: pd.DataFrame, name: str, default: float = 0.0) -> pd.Series:
    if name in df.columns:
        return pd.to_numeric(df[name], errors="coerce")
    return pd.Series(np.full(len(df), default), index=df.index, dtype="float64")

def _aligned_sep_series(df: pd.DataFrame) -> pd.Series:
    """
    Return a sep-like Series aligned to df.index.
    Preference order:
    - dist_arcsec (new sidecar)
    - sep_arcsec  (legacy)
    - else all-NaN (aligned)
    """
    if "dist_arcsec" in df.columns:
        return pd.to_numeric(df["dist_arcsec"], errors="coerce")
    if "sep_arcsec" in df.columns:
        return pd.to_numeric(df["sep_arcsec"], errors="coerce")
    return pd.Series(np.full(len(df), np.nan), index=df.index, dtype="float64")

def summarize_ir_flags(flags_parquet: Path, radius_arcsec: float) -> Dict:
    out = {
        "ir_total_rows": 0,
        "ir_strict_matches": 0,
        "ir_strict_match_rate": 0.0,
        "ir_sep_arcsec_median": float("nan"),
        "ir_sep_arcsec_p95": float("nan"),
        "ir_w1_snr_ge5": 0,
        "ir_w2_snr_ge5": 0,
        "ir_any_snr_ge5": 0,
        "ir_rows_with_bins": 0,
        "ir_partitions_with_bins": 0,
    }
    if not flags_parquet.exists():
        return out

    try:
        df = pd.read_parquet(flags_parquet, engine="pyarrow")
    except Exception:
        return out

    total = len(df)
    out["ir_total_rows"] = int(total)
    if total == 0:
        return out

    # Determine strict matches
    if "has_ir_match" in df.columns:
        # New Post 1.6 sidecar
        strict_mask = df["has_ir_match"].astype("boolean").fillna(False)
    elif "ir_match_strict" in df.columns:
        # Legacy boolean-ish column
        strict_mask = (
            df["ir_match_strict"]
            .replace({"True": True, "False": False})
            .astype("boolean")
            .fillna(False)
        )
    else:
        # Legacy sep-only: sep <= radius
        sep = _aligned_sep_series(df)
        strict_mask = sep.le(radius_arcsec).fillna(False)

    matches = df.loc[strict_mask]

    mcount = int(len(matches))
    out["ir_strict_matches"] = mcount
    out["ir_strict_match_rate"] = (mcount / total) if total else 0.0

    # Separation stats (use whatever sep column exists, aligned)
    sep_m = _aligned_sep_series(matches).dropna()
    out["ir_sep_arcsec_median"] = float(sep_m.median()) if len(sep_m) else float("nan")
    out["ir_sep_arcsec_p95"] = float(sep_m.quantile(0.95)) if len(sep_m) else float("nan")

    # SNR stats (only if present; else zeros)
    w1 = safe_num(df, "w1snr")
    w2 = safe_num(df, "w2snr")
    out["ir_w1_snr_ge5"] = int((w1 >= 5).sum())
    out["ir_w2_snr_ge5"] = int((w2 >= 5).sum())
    out["ir_any_snr_ge5"] = int(((w1 >= 5) | (w2 >= 5)).sum())

    # Optional bins for partition diagnostics
    if ("ra_bin" in df.columns) and ("dec_bin" in df.columns):
        rb = df["ra_bin"]
        db = df["dec_bin"]
        out["ir_rows_with_bins"] = int((rb.notna() & db.notna()).sum())
        out["ir_partitions_with_bins"] = int(len(pd.DataFrame({"rb": rb, "db": db}).dropna().drop_duplicates()))

    return out

# ---------- writers ----------
def write_compact_lines_md(base_dir: str, sections: List[Dict]) -> str:
    out = Path(base_dir) / "run_summary.md"
    lines = ["# VASCO Run Summary\n\n"]
    for r in sections:
        lines.append(f"## {r.get('label','run')}\n")
        lines.append(f"- tiles_total: {r.get('tiles_total', 0)}\n")
        lines.append(f"- tiles_with_catalogs: {r.get('tiles_with_catalogs', 0)}\n")
        lines.append(f"- tiles_with_xmatch: {r.get('tiles_with_xmatch', 0)}\n")
        lines.append(f"- tiles_with_final: {r.get('tiles_with_final', 0)}\n")
        lines.append(f"- detections (PASS2): {r.get('detections', 0)}\n")
        lines.append(f"- GAIA matched (≤5\"): {r.get('gaia_matched', 0)}\n")
        lines.append(f"- PS1 matched (≤5\"): {r.get('ps1_matched', 0)}\n")
        lines.append(f"- matched_any_ids_unique (canonical): {r.get('matched_any_ids_unique', 0)}\n")
        lines.append(f"- matched_any_ids_unique %: {r.get('matched_any_ids_unique_pct', 0.0):.2f}\n")
        lines.append(f"- gaia_ids (IDs from CDS): {r.get('gaia_ids', 0)}\n")
        lines.append(f"- ps1_ids (IDs from CDS): {r.get('ps1_ids', 0)}\n")
        lines.append(f"- GAIA unmatched (CDS): {r.get('gaia_unmatched_cdss', 0)}\n")
        lines.append(f"- PS1 unmatched (CDS): {r.get('ps1_unmatched_cdss', 0)}\n")
        lines.append(f"- GAIA unmatched (local): {r.get('gaia_unmatched_local', 0)}\n")
        lines.append(f"- PS1 unmatched (local): {r.get('ps1_unmatched_local', 0)}\n")
        lines.append(f"- USNOB unmatched: {r.get('usnob_unmatched', 0)}\n")
        lines.append(f"- final_no_optical_counterparts: {r.get('final_no_optical_counterparts', 0)}\n")
        lines.append(f"- GAIA matched %: {r.get('gaia_matched_pct', 0.0):.2f}\n")
        lines.append(f"- PS1 matched %: {r.get('ps1_matched_pct', 0.0):.2f}\n")
        lines.append(f"- GAIA unmatched (CDS) %: {r.get('gaia_unmatched_cdss_pct', 0.0):.2f}\n")
        lines.append(f"- PS1 unmatched (CDS) %: {r.get('ps1_unmatched_cdss_pct', 0.0):.2f}\n")
        lines.append(f"- GAIA unmatched (local) %: {r.get('gaia_unmatched_local_pct', 0.0):.2f}\n")
        lines.append(f"- PS1 unmatched (local) %: {r.get('ps1_unmatched_local_pct', 0.0):.2f}\n")
        lines.append(f"- USNOB unmatched %: {r.get('usnob_unmatched_pct', 0.0):.2f}\n")
        lines.append(f"- tiles_with_wcsfix: {r.get('tiles_with_wcsfix', 0)}\n")
        lines.append(f"- final_no_optical_counterparts %: {r.get('final_no_optical_counterparts_pct', 0.0):.2f}\n")
        lines.append(f"- IR strict matches (≤ {r.get('ir_radius_arcsec', 5.0):.1f}\"): {r.get('ir_strict_matches', 0)}\n")
        lines.append(f"- IR strict match rate: {r.get('ir_strict_match_rate', 0.0):.3f}\n")
        lines.append(f"- IR sep_arcsec median: {r.get('ir_sep_arcsec_median', float('nan')):.3f}\n")
        lines.append(f"- IR sep_arcsec p95: {r.get('ir_sep_arcsec_p95', float('nan')):.3f}\n")
        lines.append(
            f"- IR SNR bands: W1≥5={r.get('ir_w1_snr_ge5', 0)} "
            f"W2≥5={r.get('ir_w2_snr_ge5', 0)} any≥5={r.get('ir_any_snr_ge5', 0)}\n"
        )
        lines.append(
            f"- IR partitions_with_bins: {r.get('ir_partitions_with_bins', 0)} "
            f"rows_with_bins: {r.get('ir_rows_with_bins', 0)}\n\n"
        )
    out.write_text("".join(lines), encoding="utf-8")
    return str(out)

def write_summary_csv(base_dir: str, rows: List[Dict]) -> str:
    out = Path(base_dir) / "run_summary.csv"
    cols = [
        "label",
        "tiles_total", "tiles_with_catalogs", "tiles_with_xmatch", "tiles_with_final",
        "detections", "gaia_matched", "ps1_matched",
        "gaia_ids", "ps1_ids", "matched_any_ids", "matched_any_ids_unique",
        "gaia_unmatched_cdss", "ps1_unmatched_cdss",
        "gaia_unmatched_local", "ps1_unmatched_local", "usnob_unmatched",
        "final_no_optical_counterparts",
        "matched_any_ids_unique_pct", "gaia_ids_pct", "ps1_ids_pct",
        "gaia_matched_pct", "ps1_matched_pct",
        "gaia_unmatched_cdss_pct", "ps1_unmatched_cdss_pct",
        "gaia_unmatched_local_pct", "ps1_unmatched_local_pct", "usnob_unmatched_pct",
        "tiles_with_wcsfix",
        "final_no_optical_counterparts_pct",
        "ir_total_rows",
        "ir_strict_matches",
        "ir_strict_match_rate",
        "ir_sep_arcsec_median",
        "ir_sep_arcsec_p95",
        "ir_w1_snr_ge5",
        "ir_w2_snr_ge5",
        "ir_any_snr_ge5",
        "ir_partitions_with_bins",
        "ir_rows_with_bins",
    ]
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, 0) for k in cols})
    return str(out)

def write_tiles_names_csv(base_dir: str, tile_names: List[str]) -> str:
    out = Path(base_dir) / "run_summary_tiles.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["tile_id"])
        for t in tile_names:
            w.writerow([t])
    return str(out)

def write_tiles_counts_csv(base_dir: str, per_tile_counts: List[Dict]) -> str:
    out = Path(base_dir) / "run_summary_tiles_counts.csv"
    cols = [
        "tile_id",
        "detections",
        "gaia_matched", "ps1_matched",
        "gaia_ids", "ps1_ids", "matched_any_ids", "matched_any_ids_unique",
        "gaia_unmatched_cdss", "ps1_unmatched_cdss",
        "gaia_unmatched_cdss_pos", "ps1_unmatched_cdss_pos",
        "gaia_unmatched_local", "ps1_unmatched_local", "usnob_unmatched",
        "final_no_optical_counterparts",
    ]
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in per_tile_counts:
            w.writerow({k: r.get(k, 0) for k in cols})
    return str(out)

# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser(description="Summarize VASCO runs (layout-aware).")
    ap.add_argument("--data-dir", default="./data")
    ap.add_argument("--run", default=None)
    ap.add_argument(
        "--irflags-parquet",
        default="./data/local-cats/_master_optical_parquet_irflags/neowise_se_flags_ALL_by_tile_number.parquet",
        help="NEOWISE IR flags parquet (supports legacy and Post-1.6 schemas).",
    )
    ap.add_argument("--radius-arcsec", type=float, default=5.0)
    args = ap.parse_args()

    flags_p = Path(args.irflags_parquet)

    if args.run is None:
        tiles_root_parent = Path(args.data_dir)
        core, tile_names, per_tile_counts = summarize_tiles_root(tiles_root_parent / "tiles")
        core.update({"label": Path(args.data_dir).name})
    else:
        run_dir = Path(args.run)
        core, tile_names, per_tile_counts = summarize_tiles_root(run_dir / "tiles")
        core.update({"label": run_dir.name})

    core["ir_radius_arcsec"] = args.radius_arcsec
    ir = summarize_ir_flags(flags_p, args.radius_arcsec)
    core.update(ir)

    base = args.run if args.run is not None else args.data_dir
    md_path = write_compact_lines_md(base, [core])
    csv_path = write_summary_csv(base, [core])
    tiles_csv = write_tiles_names_csv(base, tile_names)
    tiles_counts_csv = write_tiles_counts_csv(base, per_tile_counts)

    print("Wrote", md_path)
    print("Wrote", csv_path)
    print("Wrote", tiles_csv)
    print("Wrote", tiles_counts_csv)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())