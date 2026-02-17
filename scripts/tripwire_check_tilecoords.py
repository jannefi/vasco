#!/usr/bin/env python3
from __future__ import annotations
import argparse, csv, math, re
from pathlib import Path

# tile_id or row_id includes: tile-RA280.475-DEC-30.669
RX_TILE = re.compile(r"tile-RA(?P<ra>[-+0-9.]+)-DEC(?P<dec>[-+0-9.]+)")

def ang_sep_deg(ra1, dec1, ra2, dec2) -> float:
    # haversine on sphere, returns degrees
    r1, d1, r2, d2 = map(math.radians, [ra1, dec1, ra2, dec2])
    sd = math.sin((d2-d1)/2)**2 + math.cos(d1)*math.cos(d2)*math.sin((r2-r1)/2)**2
    return math.degrees(2*math.asin(min(1.0, math.sqrt(sd))))

def extract_tile_center(s: str):
    m = RX_TILE.search(s or "")
    if not m:
        return None
    try:
        ra0 = float(m.group("ra")) % 360.0
        dec0 = float(m.group("dec"))
        return ra0, dec0
    except Exception:
        return None

def main():
    ap = argparse.ArgumentParser(description="Tripwire check: ensure coords are near tile center encoded in tile-RA..-DEC..")
    ap.add_argument("--csv", required=True, help="Input CSV (expects RA/Dec or ra/dec and tile_id or row_id)")
    ap.add_argument("--tripwire-deg", type=float, default=1.5, help="Flag rows farther than this many degrees from tile center")
    ap.add_argument("--max-rows", type=int, default=0, help="Limit rows processed (0=all)")
    ap.add_argument("--out", default="", help="Output CSV for offenders (default: alongside input)")
    args = ap.parse_args()

    inp = Path(args.csv)
    if not inp.exists():
        raise SystemExit(f"[ERROR] not found: {inp}")

    outp = Path(args.out) if args.out else inp.with_name(inp.stem + f"_tripwire_gt{args.tripwire_deg}deg.csv")

    checked = 0
    bad = 0

    with inp.open(newline="") as f, outp.open("w", newline="") as fo:
        rdr = csv.DictReader(f)
        if not rdr.fieldnames:
            raise SystemExit(f"[ERROR] no header in {inp}")

        low = {c.lower(): c for c in rdr.fieldnames}
        # Accept both RA/Dec and ra/dec
        ra_col = low.get("ra") or low.get("ra_out") or low.get("ra_row") or low.get("ra_icrs") or low.get("ra")
        dec_col = low.get("dec") or low.get("dec_out") or low.get("dec_row") or low.get("de") or low.get("dec")
        # Your new export uses "RA" and "Dec" (case-sensitive) -> lower map finds them
        ra_col = ra_col or low.get("ra")  # already
        dec_col = dec_col or low.get("dec")

        # But because DictReader preserves original case, we must map to original:
        ra_col = low.get("ra") or low.get("ra".lower())  # safe
        dec_col = low.get("dec") or low.get("dec".lower())

        # More robust: explicitly try common original spellings
        for cand in ["RA", "ra", "RA_row", "ra_row"]:
            if cand.lower() in low:
                ra_col = low[cand.lower()]
                break
        for cand in ["Dec", "DEC", "dec", "Dec_row", "dec_row"]:
            if cand.lower() in low:
                dec_col = low[cand.lower()]
                break

        tile_col = low.get("tile_id")
        rowid_col = low.get("row_id")

        if not ra_col or not dec_col:
            raise SystemExit(f"[ERROR] could not find RA/Dec columns in {rdr.fieldnames}")

        if not (tile_col or rowid_col):
            raise SystemExit(f"[ERROR] need tile_id or row_id column to extract tile center; have {rdr.fieldnames}")

        w = csv.writer(fo)
        w.writerow(["row_id","tile_id","RA","Dec","tile_RA","tile_Dec","sep_deg"])

        for r in rdr:
            if args.max_rows and checked >= args.max_rows:
                break

            ra_s = r.get(ra_col)
            dec_s = r.get(dec_col)
            if ra_s is None or dec_s is None:
                continue
            try:
                ra = float(ra_s) % 360.0
                dec = float(dec_s)
            except Exception:
                continue

            tile_str = r.get(tile_col, "") if tile_col else ""
            if not tile_str and rowid_col:
                tile_str = r.get(rowid_col, "")

            ctr = extract_tile_center(tile_str)
            if ctr is None:
                continue
            tra, tdec = ctr

            sep = ang_sep_deg(ra, dec, tra, tdec)
            checked += 1
            if sep > args.tripwire_deg:
                bad += 1
                w.writerow([r.get(rowid_col,"") if rowid_col else "", r.get(tile_col,"") if tile_col else "",
                            ra, dec, tra, tdec, f"{sep:.6f}"])

    frac = (bad/checked) if checked else 0.0
    print(f"[RESULT] checked={checked} bad={bad} ({frac:.4%}) tripwire={args.tripwire_deg}°")
    print(f"[WROTE] {outp}")

if __name__ == "__main__":
    main()
