#!/usr/bin/env python3
import sys
from pathlib import Path

def main():
    if len(sys.argv) != 2:
        print("Usage: validate_tile_isolation.py <tile_root>")
        return 2
    tile = Path(sys.argv[1])
    err = tile / 'sex.err'
    ldac = tile / 'pass1.ldac'

    if not ldac.exists():
        print('[FAIL] pass1.ldac missing under', tile)
        return 1
    if not err.exists():
        print('[WARN] sex.err not found (continuing)')
        return 0

    txt = err.read_text(encoding='utf-8', errors='ignore')
    bad = [
        'not found, using internal defaults',
        'cannot open for writing data/tiles/',
    ]
    hits = [b for b in bad if b in txt]
    if hits:
        print('[FAIL] Found pathing errors in sex.err:', ', '.join(hits))
        return 1
    print('[OK] Tile isolation looks good; no pathing errors detected.')
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
