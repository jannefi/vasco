#!/usr/bin/env python3
"""
Cleanup non-POSS-I tiles and empty tile folders.
Layout-aware: scans flat ./data/tiles and sharded ./data/tiles_by_sky.
"""
import argparse
import csv
import re
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

TILES_GLOB = 'tile-RA*-DEC*'

def is_empty_tile(tile_dir: Path) -> bool:
    raw_dir = tile_dir / 'raw'
    if raw_dir.exists() and any(raw_dir.glob('*.fits')):
        return False
    if (tile_dir / 'pass1.ldac').exists():
        return False
    if (tile_dir / 'pass2.ldac').exists():
        return False
    xdir = tile_dir / 'xmatch'
    if xdir.exists() and any(xdir.iterdir()):
        return False
    if any(tile_dir.glob('RUN_*')):
        return False
    return True

def has_serc_in_log(tile_dir: Path) -> bool:
    log_path = tile_dir / 'logs' / 'download.log'
    if not log_path.exists():
        return False
    try:
        text = log_path.read_text(encoding='utf-8', errors='ignore')
    except Exception:
        return False
    return re.search(r'\bSERC\b', text, flags=re.IGNORECASE) is not None

def iter_tile_dirs_any(tiles_root: Path):
    flat = tiles_root
    if flat.exists():
        for p in sorted(flat.glob(TILES_GLOB)):
            if p.is_dir(): yield p
    sharded = tiles_root.parent / 'tiles_by_sky'
    if sharded.exists():
        for p in sorted(sharded.glob('ra_bin=*/dec_bin=*/' + TILES_GLOB)):
            if p.is_dir(): yield p

def find_tiles(tiles_root: Path):
    return list(iter_tile_dirs_any(tiles_root))

def write_ledger(csv_path: Path, rows):
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ['tile_id', 'reason', 'action', 'timestamp']
    new_file = (not csv_path.exists()) or (csv_path.stat().st_size == 0)
    with csv_path.open('a', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if new_file:
            w.writeheader()
        for r in rows:
            w.writerow(r)

def main(argv=None):
    ap = argparse.ArgumentParser(description='Cleanup non-POSS-I and empty tile folders (layout-aware)')
    ap.add_argument('--tiles-root', default='./data/tiles', help='Root path containing tile folders')
    ap.add_argument('--mode', choices=['logs', 'empty', 'all'], default='logs', help='Detection mode')
    dz = ap.add_mutually_exclusive_group()
    dz.add_argument('--dry-run', action='store_true', default=True, help='List actions only')
    dz.add_argument('--apply', action='store_true', help='Perform deletions')
    ap.add_argument('--verbose', action='store_true', help='Show per-tile details')
    ap.add_argument('--ledger', default='./data/metadata/cleanup_actions.csv', help='CSV ledger path')
    args = ap.parse_args(argv)

    tiles_root = Path(args.tiles_root)
    if not tiles_root.exists():
        print(f"[ERROR] tiles-root not found: {tiles_root}", file=sys.stderr)
        return 2
    tiles = find_tiles(tiles_root)
    if not tiles:
        print(f"[INFO] No tile folders under {tiles_root}")
        return 0

    flagged_logs = []
    flagged_empty = []
    for td in tiles:
        if args.mode in ('logs', 'all') and has_serc_in_log(td):
            flagged_logs.append(td)
        if args.mode in ('empty', 'all') and is_empty_tile(td):
            flagged_empty.append(td)

    union_set = set(flagged_logs) | set(flagged_empty)
    union = sorted(union_set)

    print(f"[SUMMARY] Tiles flagged via logs (SERC): {len(flagged_logs)}")
    print(f"[SUMMARY] Tiles flagged via empty: {len(flagged_empty)}")
    print(f"[SUMMARY] Union (unique tile-ids): {len(union)}")

    if args.verbose:
        for p in union:
            reason = []
            if p in flagged_logs: reason.append('logs')
            if p in flagged_empty: reason.append('empty')
            print(f" - {p.name} (reason={'+' .join(reason)})")

    if args.dry_run and not args.apply:
        print("[DRY-RUN] No changes made. Use --apply to delete.")
        return 0

    rows = []
    now = datetime.now(timezone.utc).isoformat()
    for p in union:
        reason = []
        if p in flagged_logs: reason.append('logs')
        if p in flagged_empty: reason.append('empty')
        r = '+'.join(reason) if reason else 'unknown'
        try:
            shutil.rmtree(p)
            print(f"[DELETE] {p.name} (reason={r})")
            rows.append({'tile_id': p.name, 'reason': r, 'action': 'deleted', 'timestamp': now})
        except Exception as e:
            print(f"[ERROR] Failed to delete {p}: {e}", file=sys.stderr)
            rows.append({'tile_id': p.name, 'reason': r, 'action': f'error:{e}', 'timestamp': now})

    if rows:
        write_ledger(Path(args.ledger), rows)
        print(f"[LEDGER] Recorded {len(rows)} actions to {args.ledger}")
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
