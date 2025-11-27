
#!/usr/bin/env python3
import os
import sys
import csv
import argparse
from pathlib import Path

def parse_args():
    ap = argparse.ArgumentParser(description="Summarize VASCO runs with magnitude filter and unmatched catalogs")
    ap.add_argument('--data-dir', default='./data', help='Input/output data directory (default: ./data)')
    ap.add_argument('--mag-limit', type=float, default=20.0, help='Upper magnitude limit for candidate summary (default: 20.0)')
    ap.add_argument('--mag-min', type=float, default=0.0, help='Lower magnitude limit for candidate summary (default: 0.0)')
    ap.add_argument('--max-md-candidates', type=int, default=50, help='Max candidates per tile in Markdown (default: 50)')
    return ap.parse_args()

def find_run_tiles(data_dir):
    runs = sorted(Path(data_dir, "runs").glob("run-*"))
    tiles = []
    for run in runs:
        for tile in (run / "tiles").glob("*"):
            tiles.append(tile)
    return tiles

def parse_candidates(cat_csv, mag_min, mag_limit):
    candidates = []
    try:
        with open(cat_csv, newline='') as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader):
                mag = None
                for key in ["MAG_AUTO", "mag_auto", "MAG", "mag"]:
                    if key in row:
                        try:
                            mag = float(row[key])
                        except Exception:
                            mag = None
                        break
                # Apply lower and upper bounds
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

def parse_unmatched(unmatched_csv, mag_min, mag_limit):
    unmatched = []
    try:
        with open(unmatched_csv, newline='') as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader):
                mag = None
                for key in ["MAG_AUTO", "mag_auto", "MAG", "mag"]:
                    if key in row:
                        try:
                            mag = float(row[key])
                        except Exception:
                            mag = None
                        break
                if mag is not None and mag_min < mag < mag_limit:
                    unmatched.append({
                        "ID": row.get("ID", str(idx+1)),
                        "RA": row.get("ALPHA_J2000", row.get("RA", "")),
                        "Dec": row.get("DELTA_J2000", row.get("DEC", "")),
                        "MAG_AUTO": mag
                    })
    except Exception as e:
        print(f"[WARN] Could not parse {unmatched_csv}: {e}")
    return unmatched

def summarize_tile(tile_dir, mag_min, mag_limit):
    summary = {
        "Tile": tile_dir.name,
        "RA": "",
        "Dec": "",
        "RunFolder": str(tile_dir),
        "N_Candidates": 0,
        "Candidates": [],
        "QAPlots": [],
        "N_GaiaUnmatched": 0,
        "N_PS1Unmatched": 0,
        "N_USNOBUnmatched": 0,
        "GaiaUnmatched": [],
        "PS1Unmatched": [],
        "USNOBUnmatched": []
    }
    # Try to extract RA/Dec from tile name
    parts = tile_dir.name.split("_")
    if len(parts) >= 3:
        try:
            summary["RA"] = float(parts[1])
            summary["Dec"] = float(parts[2])
        except Exception:
            pass

    # Find catalog
    cat_csv = tile_dir / "final_catalog.csv"
    if cat_csv.exists():
        summary["Candidates"] = parse_candidates(cat_csv, mag_min, mag_limit)
        summary["N_Candidates"] = len(summary["Candidates"])

    # Find QA plots
    summary["QAPlots"] = [str(p) for p in tile_dir.glob("qa_*.png")]

    # Check for unmatched catalogs
    xmatch_dir = tile_dir / "xmatch"
    unmatched_files = {
        "GaiaUnmatched": xmatch_dir / "sex_gaia_unmatched.csv",
        "PS1Unmatched": xmatch_dir / "sex_ps1_unmatched.csv",
        "USNOBUnmatched": xmatch_dir / "sex_usnob_unmatched.csv"
    }
    for key, path in unmatched_files.items():
        if path.exists():
            summary[key] = parse_unmatched(path, mag_min, mag_limit)
            summary["N_" + key] = len(summary[key])

    return summary

def write_csv(summaries, out_csv):
    with open(out_csv, "w", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            "Tile", "RA", "Dec", "RunFolder", "N_Candidates",
            "N_GaiaUnmatched", "N_PS1Unmatched", "N_USNOBUnmatched"
        ])
        writer.writeheader()
        for s in summaries:
            writer.writerow({k: s[k] for k in writer.fieldnames})

def write_markdown(summaries, out_md, mag_min, mag_limit, max_md_candidates):
    with open(out_md, "w", encoding="utf-8") as f:
        f.write(f"# VASCO Run Summary\n\n")
        f.write(f"**Magnitude range:** {mag_min} < MAG_AUTO < {mag_limit}\n\n")
        for s in summaries:
            f.write(f"## Tile: {s['Tile']} (RA={s['RA']}, Dec={s['Dec']})\n")
            f.write(f"- Run folder: `{s['RunFolder']}`\n")
            f.write(f"- Candidates with {mag_min} < MAG_AUTO < {mag_limit}: {s['N_Candidates']}\n")
            f.write(f"- Gaia unmatched: {s['N_GaiaUnmatched']}\n")
            f.write(f"- PS1 unmatched: {s['N_PS1Unmatched']}\n")
            f.write(f"- USNOB unmatched: {s['N_USNOBUnmatched']}\n")
            if s["QAPlots"]:
                f.write(f"- QA Plots:\n")
                for q in s["QAPlots"]:
                    rel_path = os.path.relpath(q, start=os.path.dirname(out_md))
                    f.write(f"  - ![]({rel_path})\n")
            # Candidates: show top N by brightness (lowest MAG_AUTO)
            if s["N_Candidates"] > 0:
                f.write(f"\n### Top {max_md_candidates} Candidates:\n")
                f.write("| ID | RA | Dec | MAG_AUTO |\n|---|---|---|---|\n")
                sorted_cands = sorted(s["Candidates"], key=lambda x: x["MAG_AUTO"])
                for c in sorted_cands[:max_md_candidates]:
                    f.write(f"| {c['ID']} | {c['RA']} | {c['Dec']} | {c['MAG_AUTO']} |\n")
            for key, label in [("GaiaUnmatched", "Gaia"), ("PS1Unmatched", "PS1"), ("USNOBUnmatched", "USNOB")]:
                if s["N_" + key] > 0:
                    f.write(f"\n### Top {max_md_candidates} {label} Unmatched Candidates:\n")
                    f.write("| ID | RA | Dec | MAG_AUTO |\n|---|---|---|---|\n")
                    sorted_unmatched = sorted(s[key], key=lambda x: x["MAG_AUTO"])
                    for c in sorted_unmatched[:max_md_candidates]:
                        f.write(f"| {c['ID']} | {c['RA']} | {c['Dec']} | {c['MAG_AUTO']} |\n")
            f.write("\n---\n")

def main():
    args = parse_args()
    data_dir = Path(args.data_dir)
    mag_limit = args.mag_limit
    mag_min = args.mag_min
    max_md_candidates = args.max_md_candidates

    print(f"[INFO] Scanning runs in {data_dir}/runs ...")
    tiles = find_run_tiles(data_dir)
    print(f"[INFO] Found {len(tiles)} tiles.")

    summaries = []
    for tile_dir in tiles:
        s = summarize_tile(tile_dir, mag_min, mag_limit)
        summaries.append(s)

    out_csv = data_dir / "run_summary.csv"
    out_md = data_dir / "run_summary.md"
    write_csv(summaries, out_csv)
    write_markdown(summaries, out_md, mag_min, mag_limit, max_md_candidates)
    print(f"[INFO] Wrote summary CSV: {out_csv}")
    print(f"[INFO] Wrote summary Markdown: {out_md}")

if __name__ == "__main__":
    main()
