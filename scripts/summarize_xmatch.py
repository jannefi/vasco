#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
summarize_xmatch.py (layout-aware)
- Prints per-tile matched/unmatched counts for GAIA/PS1/USNOB.
- Accepts either a run directory (containing ./tiles) or a data directory.
- Supports flat ./data/tiles and sharded ./data/tiles_by_sky/ra_bin=*/dec_bin=*/tile-*.
"""
import sys
from pathlib import Path

def count_rows(path: Path) -> int:
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            return max(0, sum(1 for _ in f) - 1)
    except Exception:
        return 0

def iter_tile_dirs_any(base: Path):
    flat = base / "tiles"
    if flat.exists():
        for p in sorted(flat.glob("tile-*")):
            if p.is_dir():
                yield p
    sharded = base / "tiles_by_sky"
    if sharded.exists():
        for p in sorted(sharded.glob("ra_bin=*/dec_bin=*/tile-*")):
            if p.is_dir():
                yield p

def summarize_one_tile(tile_dir: Path):
    xdir = tile_dir / "xmatch"
    gaia_x  = xdir / "sex_gaia_xmatch.csv"
    gaia_cd = xdir / "sex_gaia_xmatch_cdss.csv"
    ps1_x   = xdir / "sex_ps1_xmatch.csv"
    ps1_cd  = xdir / "sex_ps1_xmatch_cdss.csv"
    usno_x  = xdir / "sex_usnob_xmatch.csv"
    gaia_un      = xdir / "sex_gaia_unmatched.csv"
    gaia_un_cdss = xdir / "sex_gaia_unmatched_cdss.csv"
    ps1_un       = xdir / "sex_ps1_unmatched.csv"
    ps1_un_cdss  = xdir / "sex_ps1_unmatched_cdss.csv"
    usno_un      = xdir / "sex_usnob_unmatched.csv"

    n_gaia_matched = count_rows(gaia_cd) if gaia_cd.exists() else count_rows(gaia_x)
    n_ps1_matched  = count_rows(ps1_cd)  if ps1_cd.exists()  else count_rows(ps1_x)
    n_usno_matched = count_rows(usno_x)  if usno_x.exists()  else 0

    n_gaia_unmatched = count_rows(gaia_un_cdss) if gaia_un_cdss.exists() else count_rows(gaia_un)
    n_ps1_unmatched  = count_rows(ps1_un_cdss)  if ps1_un_cdss.exists()  else count_rows(ps1_un)
    n_usno_unmatched = count_rows(usno_un)      if usno_un.exists()      else 0

    print(tile_dir.name)
    print(f" GAIA: matched rows={n_gaia_matched}, unmatched={n_gaia_unmatched}")
    print(f" PS1 : matched rows={n_ps1_matched}, unmatched={n_ps1_unmatched}")
    print(f" USNOB: matched rows={n_usno_matched}, unmatched={n_usno_unmatched}")

def main():
    if len(sys.argv) != 2:
        print("Usage: summarize_xmatch.py <base_dir>", file=sys.stderr)
        print("  <base_dir> can be a run directory (with ./tiles) or ./data", file=sys.stderr)
        sys.exit(2)

    base_dir = Path(sys.argv[1]).resolve()
    if not base_dir.exists():
        print(f"[ERROR] Not found: {base_dir}", file=sys.stderr)
        sys.exit(2)

    tiles = list(iter_tile_dirs_any(base_dir))
    if not tiles:
        run_tiles = base_dir / "tiles"
        if run_tiles.exists():
            tiles = [p for p in sorted(run_tiles.glob("tile-*")) if p.is_dir()]
    if not tiles:
        print(f"[INFO] No tiles found under: {base_dir}")
        sys.exit(0)

    for tile in tiles:
        summarize_one_tile(tile)

if __name__ == "__main__":
    main()
