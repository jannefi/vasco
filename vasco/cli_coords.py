from __future__ import annotations
import argparse
from .utils.coords import parse_ra, parse_dec


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        prog="vasco.cli_coords",
        description="Convert RA/Dec (sexagesimal or decimal) to decimal degrees.",
    )
    p.add_argument("--ra", default=None, help='RA as "hh:mm:ss.ss" or decimal degrees')
    p.add_argument(
        "--dec", default=None, help='Dec as "+dd:mm:ss.ss" or decimal degrees'
    )
    p.add_argument("--precision", type=int, default=9)
    args = p.parse_args(argv)
    if args.ra is None and args.dec is None:
        p.error("Provide --ra and/or --dec")
    if args.ra is not None:
        print(f"{parse_ra(args.ra):.{args.precision}f}")
    if args.dec is not None:
        print(f"{parse_dec(args.dec):+.{args.precision}f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
