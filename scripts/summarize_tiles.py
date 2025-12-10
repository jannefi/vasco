#!/usr/bin/env python3
"""
Summarize tile-first runs under data/tiles/* to Markdown + CSV.
Outputs:
- data/tiles_summary.md
- data/tiles_summary.csv
"""
import os, glob, csv, json
from pathlib import Path

DATA_DIR = Path("data")
TILES_ROOT = DATA_DIR / "tiles"

def rows_minus_header(path: str) -> int:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            n = sum(1 for _ in f)
        return max(0, n - 1)
    except Exception:
        return 0

def summarize_tile(tile_dir: Path) -> dict:
    run_dir = tile_dir / "run"
    catalogs_dir = tile_dir / "catalogs"
    xmatch_dir = tile_dir / "xmatch"

    det = rows_minus_header(str(catalogs_dir / "sextractor_pass2.csv"))
    # Matched (prefer CDS within5; fallback to raw cds)
    gaia_match = rows_minus_header(str(xmatch_dir / "sex_gaia_xmatch_cdss_within5arcsec.csv"))
    if gaia_match == 0:
        gaia_match = rows_minus_header(str(xmatch_dir / "sex_gaia_xmatch_cdss.csv"))
    ps1_match = rows_minus_header(str(xmatch_dir / "sex_ps1_xmatch_cdss_within5arcsec.csv"))
    if ps1_match == 0:
        ps1_match = rows_minus_header(str(xmatch_dir / "sex_ps1_xmatch_cdss.csv"))

    # Unmatched (CDS vs local)
    gaia_un_cdss = rows_minus_header(str(xmatch_dir / "sex_gaia_unmatched_cdss.csv"))
    ps1_un_cdss = rows_minus_header(str(xmatch_dir / "sex_ps1_unmatched_cdss.csv"))
    gaia_un_local = rows_minus_header(str(xmatch_dir / "sex_gaia_unmatched.csv"))
    ps1_un_local = rows_minus_header(str(xmatch_dir / "sex_ps1_unmatched.csv"))
    usnob_un = rows_minus_header(str(xmatch_dir / "sex_usnob_unmatched.csv"))

    def pct(n,d): return (100.0*n/d) if (d>0 and n>=0) else 0.0

    return {
        "tile": tile_dir.name,
        "detections": det,
        "gaia_matched": gaia_match,
        "ps1_matched": ps1_match,
        "gaia_unmatched_cdss": gaia_un_cdss,
        "ps1_unmatched_cdss": ps1_un_cdss,
        "gaia_unmatched_local": gaia_un_local,
        "ps1_unmatched_local": ps1_un_local,
        "usnob_unmatched": usnob_un,
        "gaia_matched_pct": pct(gaia_match, det),
        "ps1_matched_pct": pct(ps1_match, det),
        "gaia_unmatched_cdss_pct": pct(gaia_un_cdss, det),
        "ps1_unmatched_cdss_pct": pct(ps1_un_cdss, det),
        "gaia_unmatched_local_pct": pct(gaia_un_local, det),
        "ps1_unmatched_local_pct": pct(ps1_un_local, det),
        "usnob_unmatched_pct": pct(usnob_un, det),
    }

def write_md(rows: list[dict]) -> Path:
    out = DATA_DIR / "tiles_summary.md"
    lines = []
    lines.append("")
    lines.append("# VASCO Tiles Summary (matched / unmatched)")
    lines.append("")
    # Table: counts
    lines.append(
        " tile | detections | GAIA≤5\" | PS1≤5\" | GAIA unmatched (CDS) | PS1 unmatched (CDS) | GAIA unmatched (local) | PS1 unmatched (local) | USNOB unmatched "
    )
    lines.append(
        " --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: "
    )
    for r in rows:
        lines.append(
            f" {r['tile']} | {r['detections']} | {r['gaia_matched']} | {r['ps1_matched']} | "
            f"{r['gaia_unmatched_cdss']} | {r['ps1_unmatched_cdss']} | {r['gaia_unmatched_local']} | "
            f"{r['ps1_unmatched_local']} | {r['usnob_unmatched']} "
        )
    lines.append("")
    # Percentages
    lines.append("## Percentages relative to detections")
    lines.append("")
    lines.append(
        " tile | GAIA matched % | PS1 matched % | GAIA unmatched (CDS) % | PS1 unmatched (CDS) % | GAIA unmatched (local) % | PS1 unmatched (local) % | USNOB unmatched % "
    )
    lines.append(
        " --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: "
    )
    for r in rows:
        lines.append(
            f" {r['tile']} | {r['gaia_matched_pct']:.2f} | {r['ps1_matched_pct']:.2f} | "
            f"{r['gaia_unmatched_cdss_pct']:.2f} | {r['ps1_unmatched_cdss_pct']:.2f} | "
            f"{r['gaia_unmatched_local_pct']:.2f} | {r['ps1_unmatched_local_pct']:.2f} | {r['usnob_unmatched_pct']:.2f} "
        )
    lines.append("")
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return out

def write_csv(rows: list[dict]) -> Path:
    out = DATA_DIR / "tiles_summary.csv"
    cols = [
        "tile","detections","gaia_matched","ps1_matched",
        "gaia_unmatched_cdss","ps1_unmatched_cdss","gaia_unmatched_local","ps1_unmatched_local","usnob_unmatched",
        "gaia_matched_pct","ps1_matched_pct","gaia_unmatched_cdss_pct","ps1_unmatched_cdss_pct",
        "gaia_unmatched_local_pct","ps1_unmatched_local_pct","usnob_unmatched_pct"
    ]
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in cols})
    return out

def main():
    tiles = sorted([p for p in TILES_ROOT.glob("*") if p.is_dir()])
    rows = [summarize_tile(td) for td in tiles]
    md = write_md(rows)
    csvp = write_csv(rows)
    print("Wrote", md)
    print("Wrote", csvp)

if __name__ == "__main__":
    main()
