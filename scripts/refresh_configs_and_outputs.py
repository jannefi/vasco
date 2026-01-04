
#!/usr/bin/env python3
"""
Refresh tile-local SExtractor/PSFEx configs and gate files so the pipeline
restages configs from repo/configs and re-runs steps 2–6.

Usage:
  python scripts/refresh_configs_and_outputs.py --tiles-root ./data --layout auto \
      --remove-pass1 --remove-pass2 --remove-xmatch --dry-run
"""
import argparse, sys
from pathlib import Path

CONFIG_NAMES = {
    # pass1
    "sex_pass1.sex", "sex_default.param", "default.nnw", "default.conv",
    # psfex
    "psfex.conf",
    # pass2
    "sex_pass2.sex", "default.param", "default.nnw", "default.conv",
}

GATE_FILES = {"pass1.ldac", "pass2.ldac"}

def iter_tile_dirs(root: Path):
    for base in [root / "tiles", root / "tiles_by_sky"]:
        if not base.exists(): continue
        for p in base.rglob("tile-RA*-DEC*"):
            if p.is_dir():
                yield p

def rm(p: Path, dry: bool):
    if p.exists():
        print(("[DRY] would remove " if dry else "remove ") + str(p))
        if not dry:
            try: p.unlink()
            except Exception as e: print("  !", e)

def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--tiles-root", default="./data")
    ap.add_argument("--layout", default="auto")  # kept for symmetry; not used
    ap.add_argument("--remove-pass1", action="store_true")
    ap.add_argument("--remove-pass2", action="store_true")
    ap.add_argument("--remove-xmatch", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)

    root = Path(args.tiles_root)

    for tile in iter_tile_dirs(root):
        # remove tile-local config overrides from tile/ and tile/configs/
        for sub in [tile, tile / "configs"]:
            for name in CONFIG_NAMES:
                rm(sub / name, args.dry_run)

        # remove gating outputs
        if args.remove_pass1: rm(tile / "pass1.ldac", args.dry_run)
        if args.remove_pass2: rm(tile / "pass2.ldac", args.dry_run)

        # optional: remove derived CSV and xmatches
        if args.remove_xmatch:
            cat = tile / "catalogs"
            if cat.exists():
                for pat in ["sextractor_pass2.csv", "sextractor_pass2.filtered.csv",
                            "sextractor_spike_rejected.csv"]:
                    for f in cat.glob(pat): rm(f, args.dry_run)
            xdir = tile / "xmatch"
            if xdir.exists():
                for f in xdir.glob("sex_*_xmatch*.csv"): rm(f, args.dry_run)

if __name__ == "__main__":
    raise SystemExit(main())

