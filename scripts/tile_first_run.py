#!/usr/bin/env python3
import argparse, subprocess, sys
from pathlib import Path


def stem(survey: str, ra: float, dec: float, size_arcmin: int) -> str:
    return f"{survey}_{ra:.6f}_{dec:.6f}_{size_arcmin}arcmin"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--survey", default="poss1-e")
    ap.add_argument("--ra", type=float, required=True)
    ap.add_argument("--dec", type=float, required=True)
    ap.add_argument("--size-arcmin", type=int, default=30)
    ap.add_argument("--pixel-scale-arcsec", type=float, default=1.7)
    ap.add_argument("--export", default="csv")
    ap.add_argument("--hist-col", default="FWHM_IMAGE")
    ap.add_argument("--xmatch-radius-arcsec", type=float, default=5.0)
    ap.add_argument("--cds", action="store_true")
    args = ap.parse_args()

    tiles_root = Path("data") / "tiles"
    tiles_root.mkdir(parents=True, exist_ok=True)
    tstem = stem(args.survey, args.ra, args.dec, args.size_arcmin)
    tdir = tiles_root / tstem
    (tdir / "run").mkdir(parents=True, exist_ok=True)

    # Skip if already processed
    if (tdir / "run" / "pass2.ldac").exists():
        print("[INFO] already processed:", tstem)
        sys.exit(0)

    cmd = [
        sys.executable,
        "-m",
        "vasco.cli_pipeline",
        "one2pass",
        "--ra",
        str(args.ra),
        "--dec",
        str(args.dec),
        "--size-arcmin",
        str(args.size_arcmin),
        "--survey",
        args.survey,
        "--pixel-scale-arcsec",
        str(args.pixel_scale_arcsec),
        "--export",
        args.export,
        "--hist-col",
        args.hist_col,
        "--workdir",
        str(tdir),
    ]
    if args.cds:
        cmd += [
            "--xmatch-backend",
            "cds",
            "--xmatch-radius-arcsec",
            str(args.xmatch_radius_arcsec),
        ]
    print("CMD>", " ".join(cmd))
    sys.exit(subprocess.call(cmd))


if __name__ == "__main__":
    main()
