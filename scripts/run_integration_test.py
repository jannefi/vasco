#!/usr/bin/env python
"""Minimal integration test harness for VASCO integrations.

Usage examples:
  python scripts/run_integration_test.py --prefer-stsci
  python scripts/run_integration_test.py --mask-usno --ra 180 --dec 0 --usno /path/USNO-B1.0.fits
  python scripts/run_integration_test.py --xmatch --t1 cat1.csv --t2 cat2.csv --out xmatch.csv --radius 2
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure project root is on sys.path when running from anywhere
THIS = Path(__file__).resolve()
ROOT = THIS.parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from vasco.pipeline_integration import (
    prefer_image_service,
    mask_bright_stars_usno_b1,
    xmatch_with_stilts,
)


def main(argv=None):
    p = argparse.ArgumentParser(description="VASCO integration test harness")
    p.add_argument(
        "--prefer-stsci",
        action="store_true",
        help="Log preference for STScI DSS (and warn on SkyView).",
    )
    p.add_argument(
        "--prefer",
        choices=["stsci", "skyview"],
        help="Explicitly log preference for a given service.",
    )

    # Bright star mask
    p.add_argument(
        "--mask-usno", action="store_true", help="Call USNO-B1 placeholder mask."
    )
    p.add_argument("--usno", type=str, help="Path to USNO-B1.0 catalog (FITS/CSV).")
    p.add_argument("--ra", type=float, default=180.0)
    p.add_argument("--dec", type=float, default=0.0)
    p.add_argument("--radius-deg", type=float, default=0.5)

    # STILTS xmatch
    p.add_argument("--xmatch", action="store_true", help="Run STILTS sky cross-match.")
    p.add_argument("--t1", type=str, help="Input table 1 (CSV/FITS).")
    p.add_argument("--t2", type=str, help="Input table 2 (CSV/FITS).")
    p.add_argument("--out", type=str, help="Output path for matched table.")
    p.add_argument("--join", type=str, default="1and2")
    p.add_argument("--matcher", type=str, default="sky")
    p.add_argument("--radius", type=float, default=1.0, help="Match radius in arcsec.")

    args = p.parse_args(argv)

    # 1) Image service preference
    if args.prefer_stsci:
        prefer_image_service("stsci")
    if args.prefer:
        prefer_image_service(args.prefer)

    # 2) Bright star masking (placeholder)
    if args.mask_usno:
        if not args.usno:
            p.error("--mask-usno requires --usno PATH")
        mask_bright_stars_usno_b1(
            args.usno, args.ra, args.dec, radius_deg=args.radius_deg
        )

    # 3) STILTS xmatch
    if args.xmatch:
        missing = [k for k in ("t1", "t2", "out") if getattr(args, k) is None]
        if missing:
            p.error("--xmatch requires --t1, --t2 and --out")
        xmatch_with_stilts(
            args.t1,
            args.t2,
            args.out,
            join_type=args.join,
            matcher=args.matcher,
            radius_arcsec=args.radius,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
