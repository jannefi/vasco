
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Summarize VASCO runs to compact Markdown + CSVs (FAST v3-aware),
and include IR strict-match metrics from Post 1.5 (global flags parquet).

Outputs (default to <DATA_DIR>):
- run_summary.md
- run_summary.csv
- run_summary_tiles.csv
- run_summary_tiles_counts.csv

Modern layout: <DATA_DIR>/tiles/<tile>/{catalogs,xmatch}/...
Legacy --run <RUN_DIR> remains supported.

IR flags parquet (from Post 1.5):
- default: ./data/local-cats/_master_optical_parquet_irflags/neowise_se_flags_ALL.parquet
- can be overridden via --irflags-parquet
"""

import argparse
import glob
import csv
from pathlib import Path
from typing import Dict, List, Tuple

import math
import numpy as np
import pandas as pd


# -------------------------- helpers over CSVs --------------------------

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


# -------------------------- per-tile visible summaries --------------------------

def summarize_tile(tile_dir: Path) -> Dict:
    catalogs = tile_dir / "catalogs"
    xmatch = tile_dir / "xmatch"

    # Detections from PASS2
    det = glob_sum(str(catalogs / "sextractor_pass2.csv"))

    # Matched rows from xmatch CSVs (legacy visible rows)
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

    # FAST v3: ID projections + union (canonical)
    gaia_ids = glob_sum(str(xmatch / "gaia_ids.csv"))
    ps1_ids = glob_sum(str(xmatch / "ps1_ids.csv"))
    matched_any_ids = glob_sum(str(xmatch / "matched_any_ids.csv"))
    matched_any_ids_unique = glob_sum(str(xmatch / "matched_any_ids_unique.csv"))

    # Unmatched (CDS ID-based + optional positional)
    gaia_unmatch_cdss = glob_sum(str(xmatch / "sex_gaia_unmatched_cdss.csv"))
    ps1_unmatch_cdss = glob_sum(str(xmatch / "sex_ps1_unmatched_cdss.csv"))
    gaia_unmatch_cdss_pos = glob_sum(str(xmatch / "sex_gaia_unmatched_cdss_pos.csv"))
    ps1_unmatch_cdss_pos = glob_sum(str(xmatch / "sex_ps1_unmatched_cdss_pos.csv"))

    # Legacy/local unmatched
    gaia_unmatch_local = glob_sum(str(xmatch / "sex_gaia_unmatched.csv"))
    ps1_unmatch_local = glob_sum(str(xmatch / "sex_ps1_unmatched.csv"))
    usnob_unmatch = glob_sum(str(xmatch / "sex_usnob_unmatched.csv"))

    # Strict final residual (no Gaia & no PS1)
    final_no_optical = glob_sum(str(xmatch / "no_optical_counterparts.csv"))

    return {
        "tile_id": tile_dir.name,
        "detections": det,
        # visible matched (legacy)
        "gaia_matched": gaia_match,
        "ps1_matched": ps1_match,
        # FAST v3: IDs + union
        "gaia_ids": gaia_ids,
        "ps1_ids": ps1_ids,
        "matched_any_ids": matched_any_ids,
        "matched_any_ids_unique": matched_any_ids_unique,
        # unmatched (CDS + positional)
        "gaia_unmatched_cdss": gaia_unmatch_cdss,
        "ps1_unmatched_cdss": ps1_unmatch_cdss,
        "gaia_unmatched_cdss_pos": gaia_unmatch_cdss_pos,
        "ps1_unmatched_cdss_pos": ps1_unmatch_cdss_pos,
        # legacy/local
        "gaia_unmatched_local": gaia_unmatch_local,
        "ps1_unmatched_local": ps1_unmatch_local,
        "usnob_unmatched": usnob_unmatch,
        # strict residual
        "final_no_optical_counterparts": final_no_optical,
    }


def summarize_tiles_root(tiles_root: Path) -> Tuple[Dict, List[str], List[Dict]]:
    tile_dirs = sorted([p for p in tiles_root.glob("*") if p.is_dir()])
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
        "tiles_with_wcsfix": 0,
        # tile-derived metrics
        "tiles_total": len(tile_dirs),
        "tiles_with_catalogs": 0,
        "tiles_with_xmatch": 0,
        "tiles_with_final": 0,
    }

    # Presence flags per tile
    for td in tile_dirs:
        has_catalog = (td / "catalogs" / "sextractor_pass2.csv").exists()
        xm_dir = td / "xmatch"
        has_xmatch = xm_dir.exists() and any(xm_dir.glob("sex_*_xmatch*.csv"))
        has_final = (xm_dir / "no_optical_counterparts.csv").exists()
        if has_catalog: agg["tiles_with_catalogs"] += 1
        if has_xmatch: agg["tiles_with_xmatch"] += 1
        if has_final: agg["tiles_with_final"] += 1
        has_wcsfix = (td / "final_catalog_wcsfix.csv").exists()
        if has_wcsfix: agg["tiles_with_wcsfix"] += 1

    # Totals
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

    # Percentages (canonical: union-of-unique IDs vs detections)
    agg.update({
        "matched_any_ids_unique_pct": pct(agg["matched_any_ids_unique"], agg["detections"]),
        "gaia_ids_pct": pct(agg["gaia_ids"], agg["detections"]),
        "ps1_ids_pct": pct(agg["ps1_ids"], agg["detections"]),
        "gaia_unmatched_cdss_pct": pct(agg["gaia_unmatched_cdss"], agg["detections"]),
        "ps1_unmatched_cdss_pct": pct(agg["ps1_unmatched_cdss"], agg["detections"]),
        "final_no_optical_counterparts_pct": pct(agg["final_no_optical_counterparts"], agg["detections"]),
        # Back-compat with older MD: keep these too
        "gaia_matched_pct": pct(agg["gaia_matched"], agg["detections"]),
        "ps1_matched_pct": pct(agg["ps1_matched"], agg["detections"]),
        "gaia_unmatched_local_pct": pct(agg["gaia_unmatched_local"], agg["detections"]),
        "ps1_unmatched_local_pct": pct(agg["ps1_unmatched_local"], agg["detections"]),
        "usnob_unmatched_pct": pct(agg["usnob_unmatched"], agg["detections"]),
    })

    tile_names = [r["tile_id"] for r in per_tile_counts]
    return agg, tile_names, per_tile_counts


# -------------------------- IR flags (global) --------------------------

def summarize_ir_flags(flags_parquet: Path, radius_arcsec: float) -> Dict:
    """
    Read the global flags parquet and compute strict-match metrics.

    Returns dict with keys:
      ir_total_rows, ir_strict_matches, ir_strict_match_rate,
      ir_sep_arcsec_median, ir_sep_arcsec_p95,
      ir_w1_snr_ge5, ir_w2_snr_ge5, ir_any_snr_ge5,
      ir_rows_with_bins, ir_partitions_with_bins
    If file missing or unreadable, returns minimal dict with zeros/NAs.
    """
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
        print(f"[WARN] IR flags parquet not found: {flags_parquet}")
        return out

    try:
        df = pd.read_parquet(flags_parquet, engine="pyarrow")
    except Exception as e:
        print(f"[WARN] Failed to read IR flags parquet: {e}")
        return out

    total = len(df)
    out["ir_total_rows"] = int(total)
    if total == 0:
        return out

    # Prefer boolean 'ir_match_strict' if present; else fall back to sep threshold
    if "ir_match_strict" in df.columns:
        strict = df["ir_match_strict"].replace({"True": True, "False": False}).astype("boolean").fillna(False)
        matches = df[strict]
    else:
        sep = pd.to_numeric(df.get("sep_arcsec", pd.Series([], dtype="float64")), errors="coerce")
        matches = df[sep.le(radius_arcsec)]

    mcount = int(len(matches))
    out["ir_strict_matches"] = mcount
    out["ir_strict_match_rate"] = (mcount / total) if total else 0.0

    # Separation stats (strict matches only)
    sep_m = pd.to_numeric(matches.get("sep_arcsec", pd.Series([], dtype="float64")), errors="coerce").dropna()
    out["ir_sep_arcsec_median"] = float(sep_m.median()) if len(sep_m) else float("nan")
    out["ir_sep_arcsec_p95"] = float(sep_m.quantile(0.95)) if len(sep_m) else float("nan")

    # SNR bands (overall)
    w1 = pd.to_numeric(df.get("w1snr", pd.Series(np.zeros(total))), errors="coerce")
    w2 = pd.to_numeric(df.get("w2snr", pd.Series(np.zeros(total))), errors="coerce")
    out["ir_w1_snr_ge5"] = int((w1 >= 5).sum())
    out["ir_w2_snr_ge5"] = int((w2 >= 5).sum())
    out["ir_any_snr_ge5"] = int(((w1 >= 5) | (w2 >= 5)).sum())

    # Partition awareness
    if ("ra_bin" in df.columns) and ("dec_bin" in df.columns):
        rb = df["ra_bin"]; db = df["dec_bin"]
        out["ir_rows_with_bins"] = int((rb.notna() & db.notna()).sum())
        out["ir_partitions_with_bins"] = int(len(pd.DataFrame({"rb": rb, "db": db}).dropna().drop_duplicates()))
    return out


# -------------------------- glue: assemble per-run + IR --------------------------

def summarize_current(data_dir: str, flags_parquet: Path, radius_arcsec: float):
    tiles_root = Path(data_dir) / "tiles"
    core, tile_names, per_tile_counts = summarize_tiles_root(tiles_root)
    core.update({"label": Path(data_dir).name})

    # IR flags overlay (global)
    ir = summarize_ir_flags(flags_parquet, radius_arcsec)
    core.update(ir)
    return core, tile_names, per_tile_counts


def summarize_run(run_dir: str, flags_parquet: Path, radius_arcsec: float):
    tiles_root = Path(run_dir) / "tiles"
    core, tile_names, per_tile_counts = summarize_tiles_root(tiles_root)
    core.update({"label": Path(run_dir).name})

    ir = summarize_ir_flags(flags_parquet, radius_arcsec)
    core.update(ir)
    return core, tile_names, per_tile_counts


# -------------------------- writers --------------------------

def write_compact_lines_md(base_dir: str, sections: List[Dict]) -> str:
    out = Path(base_dir) / "run_summary.md"
    lines = []
    lines.append("# VASCO Run Summary\n\n")
    for r in sections:
        lines.append(f"## {r.get('label','run')}\n")
        # tile-derived metrics first
        lines.append(f"- tiles_total: {r.get('tiles_total', 0)}\n")
        lines.append(f"- tiles_with_catalogs: {r.get('tiles_with_catalogs', 0)}\n")
        lines.append(f"- tiles_with_xmatch: {r.get('tiles_with_xmatch', 0)}\n")
        lines.append(f"- tiles_with_final: {r.get('tiles_with_final', 0)}\n")

        # detections and matched/unmatched
        lines.append(f"- detections (PASS2): {r.get('detections', 0)}\n")
        lines.append(f"- GAIA matched (≤5\"): {r.get('gaia_matched', 0)}\n")
        lines.append(f"- PS1 matched (≤5\"): {r.get('ps1_matched', 0)}\n")

        # FAST v3 union/IDs
        lines.append(f"- matched_any_ids_unique (canonical): {r.get('matched_any_ids_unique', 0)}\n")
        lines.append(f"- matched_any_ids_unique %: {r.get('matched_any_ids_unique_pct', 0.0):.2f}\n")
        lines.append(f"- gaia_ids (IDs from CDS): {r.get('gaia_ids', 0)}\n")
        lines.append(f"- ps1_ids (IDs from CDS): {r.get('ps1_ids', 0)}\n")

        # unmatched CDS/local + final residual
        lines.append(f"- GAIA unmatched (CDS): {r.get('gaia_unmatched_cdss', 0)}\n")
        lines.append(f"- PS1 unmatched (CDS): {r.get('ps1_unmatched_cdss', 0)}\n")
        lines.append(f"- GAIA unmatched (local): {r.get('gaia_unmatched_local', 0)}\n")
        lines.append(f"- PS1 unmatched (local): {r.get('ps1_unmatched_local', 0)}\n")
        lines.append(f"- USNOB unmatched: {r.get('usnob_unmatched', 0)}\n")
        lines.append(f"- final_no_optical_counterparts: {r.get('final_no_optical_counterparts', 0)}\n")

        # percentages (include legacy ones for back-compat)
        lines.append(f"- GAIA matched %: {r.get('gaia_matched_pct', 0.0):.2f}\n")
        lines.append(f"- PS1 matched %: {r.get('ps1_matched_pct', 0.0):.2f}\n")
        lines.append(f"- GAIA unmatched (CDS) %: {r.get('gaia_unmatched_cdss_pct', 0.0):.2f}\n")
        lines.append(f"- PS1 unmatched (CDS) %: {r.get('ps1_unmatched_cdss_pct', 0.0):.2f}\n")
        lines.append(f"- GAIA unmatched (local) %: {r.get('gaia_unmatched_local_pct', 0.0):.2f}\n")
        lines.append(f"- PS1 unmatched (local) %: {r.get('ps1_unmatched_local_pct', 0.0):.2f}\n")
        lines.append(f"- USNOB unmatched %: {r.get('usnob_unmatched_pct', 0.0):.2f}\n")
        lines.append(f"- tiles_with_wcsfix: {r.get('tiles_with_wcsfix', 0)}\n")
        lines.append(f"- final_no_optical_counterparts %: {r.get('final_no_optical_counterparts_pct', 0.0):.2f}\n")

        # ---- New: IR flags global section ----
        lines.append(f"- IR strict matches (≤ {r.get('ir_radius_arcsec', 5.0):.1f}\"): {r.get('ir_strict_matches', 0)}\n")
        lines.append(f"- IR strict match rate: {r.get('ir_strict_match_rate', 0.0):.3f}\n")
        lines.append(f"- IR sep_arcsec median: {r.get('ir_sep_arcsec_median', float('nan')):.3f}\n")
        lines.append(f"- IR sep_arcsec p95: {r.get('ir_sep_arcsec_p95', float('nan')):.3f}\n")
        lines.append(f"- IR SNR bands: W1≥5={r.get('ir_w1_snr_ge5', 0)} W2≥5={r.get('ir_w2_snr_ge5', 0)} any≥5={r.get('ir_any_snr_ge5', 0)}\n")
        lines.append(f"- IR partitions_with_bins: {r.get('ir_partitions_with_bins', 0)} rows_with_bins: {r.get('ir_rows_with_bins', 0)}\n")
        lines.append("\n")

    out.write_text(''.join(lines), encoding="utf-8")
    return str(out)


def write_summary_csv(base_dir: str, rows: List[Dict]) -> str:
    out = Path(base_dir) / "run_summary.csv"
    cols = [
        "label",
        # tile-derived metrics
        "tiles_total", "tiles_with_catalogs", "tiles_with_xmatch", "tiles_with_final",
        # detections/matches (legacy visible rows)
        "detections", "gaia_matched", "ps1_matched",
        # FAST v3: IDs and union (canonical matched totals)
        "gaia_ids", "ps1_ids", "matched_any_ids", "matched_any_ids_unique",
        # unmatched (CDS/local)
        "gaia_unmatched_cdss", "ps1_unmatched_cdss",
        "gaia_unmatched_local", "ps1_unmatched_local", "usnob_unmatched",
        "final_no_optical_counterparts",
        # percentages (union/IDs + keep legacy)
        "matched_any_ids_unique_pct", "gaia_ids_pct", "ps1_ids_pct",
        "gaia_matched_pct", "ps1_matched_pct",
        "gaia_unmatched_cdss_pct", "ps1_unmatched_cdss_pct",
        "gaia_unmatched_local_pct", "ps1_unmatched_local_pct", "usnob_unmatched_pct",
        "tiles_with_wcsfix",
        "final_no_optical_counterparts_pct",
        # ---- New IR metrics (global flags parquet) ----
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
        # FAST v3
        "gaia_ids", "ps1_ids", "matched_any_ids", "matched_any_ids_unique",
        # unmatched (CDS + positional)
        "gaia_unmatched_cdss", "ps1_unmatched_cdss",
        "gaia_unmatched_cdss_pos", "ps1_unmatched_cdss_pos",
        # legacy/local + final residual
        "gaia_unmatched_local", "ps1_unmatched_local", "usnob_unmatched",
        "final_no_optical_counterparts",
    ]
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in per_tile_counts:
            w.writerow({k: r.get(k, 0) for k in cols})
    return str(out)


# -------------------------- CLI --------------------------

def main():
    ap = argparse.ArgumentParser(description="Summarize VASCO runs to compact MD + CSVs (FAST v3 + IR flags).")
    ap.add_argument("--data-dir", default="./data")
    ap.add_argument("--run", default=None)
    # New: IR flags parquet + strict radius
    ap.add_argument("--irflags-parquet", default="./data/local-cats/_master_optical_parquet_irflags/neowise_se_flags_ALL.parquet")
    ap.add_argument("--radius-arcsec", type=float, default=5.0)
    args = ap.parse_args()

    flags_p = Path(args.irflags_parquet)

    if args.run is None:
        core, tile_names, per_tile_counts = summarize_current(args.data_dir, flags_p, args.radius_arcsec)
        # stash radius for MD printing
        core["ir_radius_arcsec"] = args.radius_arcsec
        md_path = write_compact_lines_md(args.data_dir, [core])
        csv_path = write_summary_csv(args.data_dir, [core])
        tiles_csv = write_tiles_names_csv(args.data_dir, tile_names)
        tiles_counts_csv = write_tiles_counts_csv(args.data_dir, per_tile_counts)
        print("Wrote", md_path)
        print("Wrote", csv_path)
        print("Wrote", tiles_csv)
        print("Wrote", tiles_counts_csv)
        return 0

    # Legacy per-run
    core, tile_names, per_tile_counts = summarize_run(args.run, flags_p, args.radius_arcsec)
    core["ir_radius_arcsec"] = args.radius_arcsec
    md_path = write_compact_lines_md(args.run, [core])
    csv_path = write_summary_csv(args.run, [core])
    tiles_csv = write_tiles_names_csv(args.run, tile_names)
    tiles_counts_csv = write_tiles_counts_csv(args.run, per_tile_counts)
    print("Wrote", md_path)
    print("Wrote", csv_path)
    print("Wrote", tiles_csv)
    print("Wrote", tiles_counts_csv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
