#!/usr/bin/env python3
import os
import sys
import csv
import argparse
from pathlib import Path

def parse_args():
    ap = argparse.ArgumentParser(description="Summarize VASCO runs (candidates, CDS within5, unmatched)")
    ap.add_argument('--data-dir', default='./data', help='Input/output data directory (default: ./data)')
    ap.add_argument('--mag-limit', type=float, default=20.0, help='Upper magnitude limit for candidate summary (default: 20.0)')
    ap.add_argument('--mag-min', type=float, default=0.0, help='Lower magnitude limit for candidate summary (default: 0.0)')
    ap.add_argument('--max-md-candidates', type=int, default=50, help='Max candidates per tile in Markdown (default: 50)')
    return ap.parse_args()

def find_run_tiles(data_dir: Path):
    runs = sorted((data_dir / 'runs').glob('run-*'))
    tiles = []
    for run in runs:
        tiles += list((run / 'tiles').glob('*'))
    return tiles

def _read_mag(row):
    for key in ("MAG_AUTO", "mag_auto", "MAG", "mag"):
        if key in row:
            try:
                return float(row[key])
            except Exception:
                return None
    return None

def parse_candidates(cat_csv: Path, mag_min: float, mag_limit: float):
    candidates = []
    try:
        with open(cat_csv, newline='') as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader):
                mag = _read_mag(row)
                if mag is not None and mag_min < mag < mag_limit:
                    candidates.append({
                        "ID": row.get("ID", str(idx+1)),
                        "RA": row.get("ALPHA_J2000", row.get("RA", "")),
                        "Dec": row.get("DELTA_J2000", row.get("DEC", "")),
                        "MAG_AUTO": mag
                    })
    except Exception as e:
        print(f"[WARN] Could not parse {cat_csv}: {e}")
    return candidates

def count_rows(csv_path: Path) -> int:
    try:
        with open(csv_path, newline='') as f:
            reader = csv.reader(f)
            header = next(reader, None)
            return sum(1 for _ in reader)
    except Exception:
        return 0

def summarize_tile(tile_dir: Path, mag_min: float, mag_limit: float):
    summary = {
        "Tile": tile_dir.name,
        "RA": "",
        "Dec": "",
        "RunFolder": str(tile_dir),
        "N_Candidates": 0,
        "Candidates": [],
        "QAPlots": [],
        # Unmatched (local tools)
        "N_GaiaUnmatched": 0,
        "N_PS1Unmatched": 0,
        "N_USNOBUnmatched": 0,
        # CDS within5 counts
        "N_GaiaWithin5": 0,
        "N_PS1Within5": 0,
    }
    # RA/Dec from tile name
    parts = tile_dir.name.split('_')
    if len(parts) >= 3:
        try:
            summary["RA"] = float(parts[1])
            summary["Dec"] = float(parts[2])
        except Exception:
            pass
    # Candidates: prefer final_catalog.csv; fallback to SExtractor PASS2
    cat_csv = tile_dir / "final_catalog.csv"
    if not cat_csv.exists():
        cat_csv = tile_dir / "catalogs" / "sextractor_pass2.csv"
    if cat_csv.exists():
        summary["Candidates"] = parse_candidates(cat_csv, mag_min, mag_limit)
        summary["N_Candidates"] = len(summary["Candidates"])
    # QA plots
    summary["QAPlots"] = [str(p) for p in tile_dir.glob("qa_*.png")]
    # Xmatch directory
    xdir = tile_dir / 'xmatch'
    if xdir.exists():
        # Unmatched files (from local helper script)
        unmatched = {
            "GaiaUnmatched": xdir / "sex_gaia_unmatched.csv",
            "PS1Unmatched": xdir / "sex_ps1_unmatched.csv",
            "USNOBUnmatched": xdir / "sex_usnob_unmatched.csv",
        }
        for key, path in unmatched.items():
            if path.exists():
                setattr(summary, key, None)  # placeholder, not used for md list in this minimal summary
                summary[f"N_{key}"] = count_rows(path)
        # CDS within5 files
        gaia5 = xdir / "sex_gaia_xmatch_cdss_within5arcsec.csv"
        ps15  = xdir / "sex_ps1_xmatch_cdss_within5arcsec.csv"
        summary["N_GaiaWithin5"] = count_rows(gaia5) if gaia5.exists() else 0
        summary["N_PS1Within5"]  = count_rows(ps15)  if ps15.exists()  else 0
    return summary

def write_csv(summaries, out_csv: Path):
    with open(out_csv, "w", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            "Tile", "RA", "Dec", "RunFolder", "N_Candidates",
            "N_GaiaWithin5", "N_PS1Within5",
            "N_GaiaUnmatched", "N_PS1Unmatched", "N_USNOBUnmatched",
        ])
        writer.writeheader()
        for s in summaries:
            writer.writerow({k: s.get(k, 0) for k in writer.fieldnames})

def write_markdown(summaries, out_md: Path, mag_min: float, mag_limit: float, max_md_candidates: int):
    with open(out_md, "w", encoding="utf-8") as f:
        f.write(f"# VASCO Run Summary\n\n")
        f.write(f"**Magnitude range:** {mag_min} < MAG_AUTO < {mag_limit}\n\n")
        for s in summaries:
            f.write(f"## Tile: {s['Tile']} (RA={s['RA']}, Dec={s['Dec']})\n")
            f.write(f"- Run folder: `{s['RunFolder']}`\n")
            f.write(f"- Candidates in range: {s['N_Candidates']}\n")
            f.write(f"- CDS Gaia ≤5\" matches: {s['N_GaiaWithin5']}\n")
            f.write(f"- CDS PS1 ≤5\" matches: {s['N_PS1Within5']}\n")
            f.write(f"- Gaia unmatched: {s.get('N_GaiaUnmatched',0)}\n")
            f.write(f"- PS1 unmatched: {s.get('N_PS1Unmatched',0)}\n")
            f.write(f"- USNOB unmatched: {s.get('N_USNOBUnmatched',0)}\n")
            if s["QAPlots"]:
                f.write(f"- QA Plots:\n")
                for q in s["QAPlots"]:
                    rel = os.path.relpath(q, start=os.path.dirname(out_md))
                    f.write(f"  - ![]({rel})\n")
            # Candidates table (top N by brightness)
            if s["N_Candidates"] > 0:
                f.write("\n### Top {} Candidates:\n".format(max_md_candidates))
                f.write("\nID | RA | Dec | MAG_AUTO\n")
                f.write("---|---|---|---\n")
                sorted_c = sorted(s["Candidates"], key=lambda x: x["MAG_AUTO"])[:max_md_candidates]
                for c in sorted_c:
                    f.write(f"{c['ID']} | {c['RA']} | {c['Dec']} | {c['MAG_AUTO']}\n")
            f.write("\n---\n")

def main():
    args = parse_args()
    data_dir = Path(args.data_dir)
    tiles = find_run_tiles(data_dir)
    print(f"[INFO] Found {len(tiles)} tiles under {data_dir}/runs")
    summaries = [summarize_tile(td, args.mag_min, args.mag_limit) for td in tiles]
    out_csv = data_dir / "run_summary.csv"
    out_md  = data_dir / "run_summary.md"
    write_csv(summaries, out_csv)
    write_markdown(summaries, out_md, args.mag_min, args.mag_limit, args.max_md_candidates)
    print(f"[INFO] Wrote summary CSV: {out_csv}")
    print(f"[INFO] Wrote summary Markdown: {out_md}")

if __name__ == "__main__":
    main()
