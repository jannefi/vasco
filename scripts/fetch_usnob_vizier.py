#!/usr/bin/env python3
"""
Fetch USNO-B1.0 (VizieR I/284) around a given center and save CSV.
Usage:
  python scripts/fetch_usnob_vizier.py --ra 19:16:45.76 --dec +51:28:52.40 --radius-arcmin 30 --out usnob.csv
Coordinates may be sexagesimal or decimal.
"""
import argparse
from pathlib import Path
from vasco.external_fetch_usnob_vizier import fetch_usnob_neighbourhood
from vasco.utils.coords import parse_ra, parse_dec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--ra", required=True, help="RA (sexagesimal hh:mm:ss.s or decimal degrees)"
    )
    ap.add_argument(
        "--dec", required=True, help="Dec (sexagesimal ±dd:mm:ss.s or decimal degrees)"
    )
    ap.add_argument("--radius-arcmin", type=float, default=30.0)
    ap.add_argument("--out", default="usnob.csv")
    args = ap.parse_args()

    # Parse coords
    try:
        ra_deg = float(args.ra)
    except Exception:
        ra_deg = float(parse_ra(args.ra))
    try:
        dec_deg = float(args.dec)
    except Exception:
        dec_deg = float(parse_dec(args.dec))

    # Use a temp tile_dir structure under current working dir
    tile_dir = Path("tmp_usnob_tile")
    tile_dir.mkdir(parents=True, exist_ok=True)
    (tile_dir / "catalogs").mkdir(parents=True, exist_ok=True)

    out_csv = fetch_usnob_neighbourhood(tile_dir, ra_deg, dec_deg, args.radius_arcmin)
    # Move/rename to requested output
    Path(args.out).write_text(
        Path(out_csv).read_text(encoding="utf-8"), encoding="utf-8"
    )
    print("Wrote", args.out)


if __name__ == "__main__":
    main()
