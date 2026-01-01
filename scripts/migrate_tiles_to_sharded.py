
#!/usr/bin/env python3
# Migrate tiles to sharded layout without creating double-nested directories.
# POSIX/WSL paths, symlink ./data -> /mnt/d/vasco/data.

import os, re, json, math, hashlib, shutil, time, argparse

BASE_DIR = os.path.abspath("./data")
SRC_ROOT = os.path.join(BASE_DIR, "tiles")
DST_ROOT = os.path.join(BASE_DIR, "tiles_by_sky")
BIN_DEG  = 5

RA_KEYS  = ["RA_DEG", "CRVAL1", "RA", "OBJ_RA"]
DEC_KEYS = ["DEC_DEG", "CRVAL2", "DEC", "OBJ_DEC"]

PAT_TILE_RADEC_A = re.compile(r"\bRA\s*([0-9]+(?:\.[0-9]+)?)\b.*?\bDEC\s*([+\-][0-9]+(?:\.[0-9]+)?)", re.I)
PAT_TILE_RADEC_B = re.compile(r"\bRA\s*([0-9]+(?:\.[0-9]+)?)\b.*?\bDEC\s*([+\-]?[0-9]+(?:\.[0-9]+)?)", re.I)
PAT_FITS_NAME    = re.compile(r"^.+?_([0-9]+(?:\.[0-9]+)?)_([+\-]?[0-9]+(?:\.[0-9]+)?)_[0-9]+(?:arcmin)?\.fits$", re.I)

def norm_ra_dec(ra, dec):
    ra  = ra % 360.0
    dec = max(-90.0, min(90.0, dec))
    return ra, dec

def bin_val(v, width): return int(math.floor(v / width) * width)
def fmt_ra_bin(ra):     return f"{bin_val(ra, BIN_DEG):03d}"
def fmt_dec_bin(dec):
    b = bin_val(dec, BIN_DEG)
    return f"{'+' if b >= 0 else '-'}{abs(b):02d}"

def scan_tiles(root):
    return sorted([e.path for e in os.scandir(root) if e.is_dir()], key=lambda p: os.path.basename(p))

def header_ra_dec(tile_path, tile_id):
    raw_dir = os.path.join(tile_path, "raw")
    if not os.path.isdir(raw_dir): return None
    preferred = os.path.join(raw_dir, f"{tile_id}.fits.header.json")
    candidates = []
    if os.path.isfile(preferred): candidates.append(preferred)
    for e in os.scandir(raw_dir):
        if e.is_file() and e.name.endswith(".fits.header.json"):
            if e.path not in candidates: candidates.append(e.path)
    for path in candidates:
        try:
            with open(path, "r", encoding="utf-8") as f:
                hdr = json.load(f)
            ra, dec = None, None
            for k in RA_KEYS:
                if k in hdr: ra = float(hdr[k]); break
            for k in DEC_KEYS:
                if k in hdr: dec = float(hdr[k]); break
            if ra is not None and dec is not None:
                return norm_ra_dec(ra, dec)
        except Exception:
            continue
    return None

def tileid_ra_dec(tile_id):
    m = PAT_TILE_RADEC_A.search(tile_id)
    if m: return norm_ra_dec(float(m.group(1)), float(m.group(2)))
    m = PAT_TILE_RADEC_B.search(tile_id)
    if m: return norm_ra_dec(float(m.group(1)), float(m.group(2)))
    return None

def fitsname_ra_dec(tile_path):
    raw_dir = os.path.join(tile_path, "raw")
    if not os.path.isdir(raw_dir): return None
    for e in os.scandir(raw_dir):
        if e.is_file() and e.name.lower().endswith(".fits"):
            m = PAT_FITS_NAME.match(e.name)
            if m: return norm_ra_dec(float(m.group(1)), float(m.group(2)))
    return None

def plan_rows():
    rows = []
    for tile_path in scan_tiles(SRC_ROOT):
        tile_id = os.path.basename(tile_path)
        ra_dec = header_ra_dec(tile_path, tile_id) or tileid_ra_dec(tile_id) or fitsname_ra_dec(tile_path)
        if ra_dec is None:
            h = hashlib.sha1(tile_id.encode("utf-8")).hexdigest()
            dst = os.path.join(DST_ROOT, "fallback_id", h[:2], h[2:4], tile_id)
            rows.append((tile_id, tile_path, dst, "fallback")); continue
        ra, dec = ra_dec
        dst = os.path.join(DST_ROOT, f"ra_bin={fmt_ra_bin(ra)}", f"dec_bin={fmt_dec_bin(dec)}", tile_id)
        rows.append((tile_id, tile_path, dst, "sky"))
    return rows

def safe_move_dir(src, dst, retries=10, sleep=5):
    """Move src->dst without pre-creating dst (avoid double nesting)."""
    parent = os.path.dirname(dst)
    os.makedirs(parent, exist_ok=True)
    for _ in range(retries):
        try:
            # ensure dst does NOT exist; shutil.move will create it
            if os.path.exists(dst):
                # if a previous partial move created nested structure, flatten then continue
                inner = os.path.join(dst, os.path.basename(src))
                if os.path.isdir(inner):
                    # move inner/* up to dst/, then remove inner
                    for entry in os.scandir(inner):
                        shutil.move(entry.path, dst)
                    os.rmdir(inner)
            else:
                shutil.move(src, dst)
            return True
        except Exception as e:
            print(f"[warn] move failed: {e}; retrying in {sleep}s")
            time.sleep(sleep)
    return False

def main(go=False, max_tiles=None):
    os.makedirs(DST_ROOT, exist_ok=True)
    rows = plan_rows()
    count = 0
    for t, s, d, m in rows:
        done_flag = os.path.join(d, "tile_move_done.txt")
        if os.path.exists(done_flag):
            print(f"[skip] {t} (already done)")
            continue
        print(f"[plan] {t} -> {d} ({m})")
        if not go:
            count += 1
            if max_tiles and count >= max_tiles: break
            continue
        ok = safe_move_dir(s, d)
        if ok:
            with open(done_flag, "w", encoding="utf-8") as f:
                f.write("DONE\n")
            print(f"[ok] {t}")
        else:
            print(f"[err] {t} failed; you can re-run later to resume.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Migrate tiles to sharded layout (no double nesting).")
    ap.add_argument("--go", action="store_true", help="Execute moves (default is dry-run).")
    ap.add_argument("--max-tiles", type=int, default=None, help="Limit number of tiles in this run.")
    args = ap.parse_args()
    main(go=args.go, max_tiles=args.max_tiles)

