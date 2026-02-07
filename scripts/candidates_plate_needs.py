#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Map candidates to their nearest POSS-I (DSS1-red) plate and report availability.

Changes vs legacy:
- Reads plate centers from repo headers under: metadata/plates/headers/
  (no dependency on legacy IRSA index CSV).
- Uses plate_id == REGION as the canonical identifier.
- Optional --available-regions <txt> (one REGION per line) to decide present/missing.
  Without it, status becomes "unknown" to avoid false assumptions.
"""

import csv
import glob
import json
import math
import os
from pathlib import Path
from typing import Dict, List, Tuple, Optional

HEADERS_DIR = Path("metadata/plates/headers")

def ang_distance_deg(ra1: float, dec1: float, ra2: float, dec2: float) -> float:
    """Great-circle (haversine) separation in degrees."""
    r1, d1, r2, d2 = map(math.radians, [ra1, dec1, ra2, dec2])
    sd = math.sin((d2 - d1) / 2.0) ** 2
    sr = math.sin((r2 - r1) / 2.0) ** 2
    a = sd + math.cos(d1) * math.cos(d2) * sr
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return math.degrees(c)

def load_repo_headers(headers_dir: Path) -> List[Dict]:
    """
    Scan metadata/plates/headers for canonical JSON sidecars.
    Expect filenames like dss1red_<REGION>.fits.header.json (but tolerate variants).
    """
    rows: List[Dict] = []
    if not headers_dir.exists():
        raise SystemExit(f"Headers dir not found: {headers_dir}")

    # Patterns to tolerate a few common names, while preferring canonical.
    patterns = [
        "dss1red_*.fits.header.json",
        "*.fits.header.json",
        "*.header.json",
    ]
    seen_regions = set()
    for pat in patterns:
        for p in sorted(headers_dir.glob(pat)):
            try:
                j = json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                continue
            hdr = j.get("header", {}) or {}

            region = str(hdr.get("REGION", "")).strip()
            if not region or region in seen_regions:
                continue

            pra = hdr.get("PLATERA", None)
            pdec = hdr.get("PLATEDEC", None)
            try:
                pra_f = float(pra) if pra is not None and f"{pra}".strip() != "" else float("nan")
                pdec_f = float(pdec) if pdec is not None and f"{pdec}".strip() != "" else float("nan")
            except Exception:
                pra_f, pdec_f = float("nan"), float("nan")

            rows.append({
                "plate_id": region,           # canonical
                "REGION": region,             # verbatim for traceability
                "PLTLABEL": str(hdr.get("PLTLABEL", "")).strip(),
                "PLATEID":  str(hdr.get("PLATEID",  "")).strip(),
                "DATE-OBS": str(hdr.get("DATE-OBS", "")).strip(),
                "SCANNUM":  str(hdr.get("SCANNUM",  "")).strip(),
                "PLATERA":  pra_f,
                "PLATEDEC": pdec_f,
            })
            seen_regions.add(region)
    if not rows:
        raise SystemExit(f"No header JSON files found in {headers_dir}")
    return rows

def nearest_plate(plates: List[Dict], ra: float, dec: float, radius_deg: float) -> Tuple[Optional[Dict], Optional[float]]:
    best, best_sep = None, 1e9
    for r in plates:
        pra, pdec = r["PLATERA"], r["PLATEDEC"]
        if math.isnan(pra) or math.isnan(pdec):
            continue
        sep = ang_distance_deg(ra, dec, pra, pdec)
        if sep < best_sep:
            best_sep, best = sep, r
    return (best, best_sep) if (best is not None and best_sep <= radius_deg) else (None, None)

def load_available_regions(path: Optional[Path]) -> Optional[set]:
    if path is None:
        return None
    if not path.exists():
        raise SystemExit(f"--available-regions file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        regs = {line.strip() for line in f if line.strip()}
    return regs

def main():
    import argparse
    ap = argparse.ArgumentParser(description="Map candidates to nearest plate; report availability.")
    ap.add_argument("--headers-dir", default=str(HEADERS_DIR),
                    help="Repo headers folder (default: metadata/plates/headers)")
    ap.add_argument("--candidates-dir", default="./data/candidates",
                    help="Folder with candidate CSV files (default: ./data/candidates)")
    ap.add_argument("--out-dir", default="./data/metadata",
                    help="Output folder for reports (default: ./data/metadata)")
    ap.add_argument("--radius-deg", type=float, default=3.5,
                    help="Accept nearest plate if within this angular radius (deg). Default: 3.5")
    ap.add_argument("--available-regions", default="",
                    help="Optional text file (one REGION per line) indicating plates available locally.")
    args = ap.parse_args()

    headers_dir = Path(args.headers_dir)
    cands_dir   = Path(args.candidates_dir)
    out_dir     = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    plates = load_repo_headers(headers_dir)
    avail = load_available_regions(Path(args.available_regions)) if args.available_regions else None

    cand_files = sorted(glob.glob(str(cands_dir / "*.csv")))
    if not cand_files:
        raise SystemExit(f"No candidate CSVs found under {cands_dir}")

    per_candidate: List[Dict] = []
    missing_regions: Dict[str, Dict] = {}

    for cf in cand_files:
        with open(cf, "r", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                try:
                    ra = float(row.get("ra_deg"))
                    dec = float(row.get("dec_deg"))
                except Exception:
                    continue
                num = (row.get("number") or "").strip()

                plate, sep = nearest_plate(plates, ra, dec, radius_deg=args.radius_deg)
                if plate is None:
                    per_candidate.append({
                        "source_file": os.path.basename(cf),
                        "number": num,
                        "ra_deg": ra, "dec_deg": dec,
                        "status": "no_nearby_plate",
                        "plate_id": "", "REGION": "", "sep_deg": ""
                    })
                    continue

                reg = plate["plate_id"]
                if avail is None:
                    status = "unknown"   # we won't infer presence without an explicit list
                else:
                    status = "present" if reg in avail else "missing"

                per_candidate.append({
                    "source_file": os.path.basename(cf),
                    "number": num,
                    "ra_deg": ra, "dec_deg": dec,
                    "status": status,
                    "plate_id": reg,
                    "REGION": reg,   # keep verbatim
                    "PLTLABEL": plate["PLTLABEL"],
                    "PLATEID":  plate["PLATEID"],
                    "DATE-OBS": plate["DATE-OBS"],
                    "SCANNUM":  plate["SCANNUM"],
                    "sep_deg": f"{sep:.3f}",
                })

                if status == "missing":
                    mr = missing_regions.setdefault(reg, {
                        "plate_id": reg,
                        "REGION": reg,
                        "PLTLABEL": plate["PLTLABEL"],
                        "PLATEID":  plate["PLATEID"],
                        "DATE-OBS": plate["DATE-OBS"],
                        "SCANNUM":  plate["SCANNUM"],
                        "count_candidates": 0,
                    })
                    mr["count_candidates"] += 1

    # Write per-candidate mapping
    out1 = out_dir / "candidates_with_plate_and_status.csv"
    with open(out1, "w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "source_file", "number", "ra_deg", "dec_deg",
            "status", "plate_id", "REGION", "PLTLABEL", "PLATEID",
            "DATE-OBS", "SCANNUM", "sep_deg"
        ]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(per_candidate)

    # Write shopping list (only meaningful if available-regions provided)
    out2 = out_dir / "plates_to_download.csv"
    with open(out2, "w", newline="", encoding="utf-8") as f:
        if avail is not None:
            fieldnames = ["plate_id", "REGION", "PLTLABEL", "PLATEID", "DATE-OBS", "SCANNUM", "count_candidates"]
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(sorted(missing_regions.values(), key=lambda x: -x["count_candidates"]))
        else:
            # Create an informational stub so downstream scripts don't break.
            w = csv.writer(f)
            w.writerow(["info"])
            w.writerow(["No --available-regions provided; list would be speculative."])

    print({
        "written": [str(out1), str(out2)],
        "candidates": len(per_candidate),
        "missing_regions": len(missing_regions) if avail is not None else "n/a"
    })

if __name__ == "__main__":
    main()
