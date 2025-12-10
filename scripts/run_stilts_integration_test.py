#!/usr/bin/env python3
"""Minimal smoke test for STILTS cross-match wrapper.

Usage:
  python scripts/run_stilts_integration_test.py       --t1 catalog1.csv --t2 catalog2.csv       --out xmatch_out.csv --ra1 ra --dec1 dec --ra2 ra --dec2 dec       --radius 2 --join 1and2
"""
from __future__ import annotations
import argparse
from vasco.utils.stilts_wrapper import stilts_xmatch


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--t1", required=True)
    ap.add_argument("--t2", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--ra1", default="ra")
    ap.add_argument("--dec1", default="dec")
    ap.add_argument("--ra2", default="ra")
    ap.add_argument("--dec2", default="dec")
    ap.add_argument("--radius", type=float, default=1.0)
    ap.add_argument("--join", default="1and2")
    ap.add_argument("--find", default=None)
    args = ap.parse_args()

    print("[INFO] Running STILTS cross-match (tskymatch2 → tmatch2 fallback)")
    stilts_xmatch(
        args.t1,
        args.t2,
        args.out,
        ra1=args.ra1,
        dec1=args.dec1,
        ra2=args.ra2,
        dec2=args.dec2,
        radius_arcsec=args.radius,
        join_type=args.join,
        find=args.find,
        ofmt="csv" if args.out.lower().endswith(".csv") else None,
    )
    print("[OK] Wrote", args.out)


if __name__ == "__main__":
    raise SystemExit(main())
