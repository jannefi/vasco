
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv, glob, math, os
from pathlib import Path

def ang_distance_deg(ra1, dec1, ra2, dec2):
    # haversine for spherical separation in degrees
    r1, d1, r2, d2 = map(math.radians, [ra1, dec1, ra2, dec2])
    sd = math.sin((d2 - d1) / 2.0) ** 2
    sr = math.sin((r2 - r1) / 2.0) ** 2
    a = sd + math.cos(d1) * math.cos(d2) * sr
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return math.degrees(c)

def load_irsa_index(index_path):
    rows = []
    with open(index_path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            # tolerate missing numeric fields
            try:
                pra = float(row.get("PLATERA", "") or "nan")
                pdec = float(row.get("PLATEDEC", "") or "nan")
            except:
                pra, pdec = float("nan"), float("nan")
            rows.append({
                "REGION": (row.get("REGION") or "").strip(),
                "PLTLABEL": (row.get("PLTLABEL") or "").strip(),
                "PLATEID": (row.get("PLATEID") or "").strip(),
                "DATE-OBS": (row.get("DATE-OBS") or "").strip(),
                "SCANNUM": (row.get("SCANNUM") or "").strip(),
                "PLATERA": pra,
                "PLATEDEC": pdec,
                "filename": (row.get("filename") or "").strip(),
            })
    return rows

def nearest_plate(irsa_rows, ra, dec, radius_deg=3.5):
    best, best_sep = None, 1e9
    for r in irsa_rows:
        pra, pdec = r["PLATERA"], r["PLATEDEC"]
        if math.isnan(pra) or math.isnan(pdec):
            continue
        sep = ang_distance_deg(ra, dec, pra, pdec)
        if sep < best_sep:
            best_sep, best = sep, r
    # accept only if within plate radius + margin
    return (best, best_sep) if best_sep <= radius_deg else (None, None)

def main():
    # choose the index you’re using; default to ./data/metadata
    index_path = Path("./data/metadata/irsa_dss1red_index.csv")
    if not index_path.exists():
        index_path = Path("./data/tests/irsa_dss1red_index.csv")
    if not index_path.exists():
        raise SystemExit("IRSA index CSV not found in ./data/metadata or ./data/tests")

    irsa_rows = load_irsa_index(str(index_path))
    # map REGION -> present flag
    have_region = {r["REGION"] for r in irsa_rows if r["REGION"]}

    out_dir = Path("./data/metadata")
    out_dir.mkdir(parents=True, exist_ok=True)

    # Gather candidates from all CSVs in ./data/candidates
    cand_files = sorted(glob.glob("./data/candidates/*.csv"))
    if not cand_files:
        raise SystemExit("No candidate CSVs found under ./data/candidates")

    per_candidate = []
    missing_regions = {}

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
                plate, sep = nearest_plate(irsa_rows, ra, dec, radius_deg=3.5)
                if plate is None:
                    per_candidate.append({
                        "source_file": os.path.basename(cf),
                        "number": num,
                        "ra_deg": ra, "dec_deg": dec,
                        "status": "no_nearby_plate", "REGION": "", "sep_deg": ""
                    })
                    continue
                reg = plate["REGION"]
                status = "present" if reg in have_region else "missing"
                per_candidate.append({
                    "source_file": os.path.basename(cf),
                    "number": num,
                    "ra_deg": ra, "dec_deg": dec,
                    "status": status,
                    "REGION": reg,
                    "PLTLABEL": plate["PLTLABEL"],
                    "PLATEID": plate["PLATEID"],
                    "DATE-OBS": plate["DATE-OBS"],
                    "SCANNUM": plate["SCANNUM"],
                    "sep_deg": f"{sep:.3f}",
                })
                if status == "missing" and reg:
                    missing_regions.setdefault(reg, {
                        "REGION": reg,
                        "PLTLABEL": plate["PLTLABEL"],
                        "PLATEID": plate["PLATEID"],
                        "DATE-OBS": plate["DATE-OBS"],
                        "SCANNUM": plate["SCANNUM"],
                        "count_candidates": 0
                    })
                    missing_regions[reg]["count_candidates"] += 1

    # Write per-candidate mapping
    out1 = out_dir / "candidates_with_plate_and_status.csv"
    with open(out1, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["source_file", "number", "ra_deg", "dec_deg",
                      "status", "REGION", "PLTLABEL", "PLATEID",
                      "DATE-OBS", "SCANNUM", "sep_deg"]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(per_candidate)

    # Write shopping list (unique missing REGIONs)
    out2 = out_dir / "plates_to_download.csv"
    with open(out2, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["REGION", "PLTLABEL", "PLATEID", "DATE-OBS", "SCANNUM", "count_candidates"]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(sorted(missing_regions.values(), key=lambda x: -x["count_candidates"]))

    print({"written": [str(out1), str(out2)], "candidates": len(per_candidate), "missing_regions": len(missing_regions)})

if __name__ == "__main__":
    main()

