
#!/usr/bin/env python3
"""
Refresh tile-local configs and remove all gating outputs so the pipeline
restages configs and re-runs steps 2–6 with visible progress.

Default behavior (aggressive):
  - removes tile-local config overrides (sex_pass1.sex, sex_default.param, default.nnw, default.conv,
    psfex.conf, sex_pass2.sex, default.param)
  - removes pass1.ldac, pass2.ldac
  - removes xmatch bases: sex_*_xmatch.csv, sex_*_xmatch_cdss.csv
  - removes within-5″ files: *_within5arcsec.csv
  - removes per-tile RUN_SUMMARY.md
  - removes step-4 status/log placeholders: STEP4_XMATCH_STATUS.json, STEP4_CDS.log

Usage:
  python scripts/refresh_configs_and_outputs.py --tiles-root ./data [--dry-run]

Optional flags:
  --keep-within5            keep existing within-5″ files (default: removed)
  --keep-summary            keep RUN_SUMMARY.md (default: removed)
  --skip-config-overrides   skip removing tile-local config overrides
"""

import argparse
from pathlib import Path

# Config filenames staged by pipeline_split
CONFIG_NAMES = {
    "sex_pass1.sex", "sex_default.param", "default.nnw", "default.conv",  # pass1
    "psfex.conf",                                                         # psfex
    "sex_pass2.sex", "default.param", "default.nnw", "default.conv",      # pass2
}

# Gate outputs per step
GATE_FILES = {"pass1.ldac", "pass2.ldac", "RUN_SUMMARY.md"}

# Xmatch & within-5″ patterns
XMATCH_BASE_PATTERNS = ("sex_*_xmatch.csv", "sex_*_xmatch_cdss.csv")
WITHIN5_PATTERNS     = ("*_within5arcsec.csv",)

# Step 4 status/log placeholders
STEP4_AUX = ("STEP4_XMATCH_STATUS.json", "STEP4_CDS.log")

def iter_tiles(root: Path):
    for base in (root / "tiles", root / "tiles_by_sky"):
        if not base.exists():
            continue
        yield from (p for p in base.rglob("tile-RA*-DEC*") if p.is_dir())

def rm(path: Path, dry: bool, counters: dict):
    try:
        if path.exists():
            counters["removed"] += 1
            if dry:
                print(f"[DRY] remove {path}")
            else:
                path.unlink()
    except Exception as e:
        print(f"[WARN] failed to remove {path}: {e}")

def main(argv=None):
    ap = argparse.ArgumentParser("refresh for full rerun (aggressive)")
    ap.add_argument("--tiles-root", default="./data")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--keep-within5", action="store_true")
    ap.add_argument("--keep-summary", action="store_true")
    ap.add_argument("--skip-config-overrides", action="store_true")
    args = ap.parse_args(argv)

    root = Path(args.tiles_root)
    counters = {"tiles": 0, "removed": 0}

    for tile in iter_tiles(root):
        counters["tiles"] += 1

        # 1) remove tile-local config overrides (tile/ and tile/configs/)
        if not args.skip_config_overrides:
            for sub in (tile, tile / "configs"):
                for name in CONFIG_NAMES:
                    rm(sub / name, args.dry_run, counters)

        # 2) remove step gate files
        for name in GATE_FILES:
            if args.keep_summary and name == "RUN_SUMMARY.md":
                continue
            rm(tile / name, args.dry_run, counters)

        # 3) remove catalogs derived from old params (optional/harmless)
        cat = tile / "catalogs"
        if cat.exists():
            for fname in ("sextractor_pass2.csv",
                          "sextractor_pass2.filtered.csv",
                          "sextractor_spike_rejected.csv",
                          "MNRAS_SUMMARY.md",
                          "MNRAS_SUMMARY.json"):
                rm(cat / fname, args.dry_run, counters)

        # 4) remove xmatch base & within-5″
        xdir = tile / "xmatch"
        if xdir.exists():
            for pat in XMATCH_BASE_PATTERNS:
                for f in xdir.glob(pat):
                    rm(f, args.dry_run, counters)
            if not args.keep_within5:
                for pat in WITHIN5_PATTERNS:
                    for f in xdir.glob(pat):
                        rm(f, args.dry_run, counters)
            for aux in STEP4_AUX:
                rm(xdir / aux, args.dry_run, counters)

    print(f"[SUMMARY] tiles scanned={counters['tiles']} files removed={counters['removed']}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
