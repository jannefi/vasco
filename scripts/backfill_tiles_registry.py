
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Backfill tiles_registry.csv from existing tiles under ./data/tiles/.

- Reads each tile's raw FITS + header JSON sidecar if available.
- Infers survey, pixel_scale_arcsec, size_arcmin.
- Appends rows atomically with a simple file lock.

Usage:
  python scripts/backfill_tiles_registry.py \
    --tiles-root ./data/tiles \
    --registry ./data/metadata/tiles_registry.csv \
    --default-survey dss1-red \
    --default-size-arcmin 30 \
    --default-pixel-scale-arcsec 1.7
"""

import csv, json, os, time, argparse, math
from pathlib import Path
from datetime import datetime, timezone

REGISTRY_FIELDS = (
    "tile_id", "ra_deg", "dec_deg", "survey", "size_arcmin", "pixel_scale_arcsec",
    "status", "downloaded_utc", "source", "notes"
)
REGISTRY_KEY_FIELDS = ("tile_id", "survey", "size_arcmin", "pixel_scale_arcsec")

def ensure_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

def load_seen(csv_path: Path, key_fields=REGISTRY_KEY_FIELDS) -> set:
    """Build set of keys already present in registry."""
    seen = set()
    if csv_path.exists() and csv_path.stat().st_size > 0:
        with csv_path.open("r", newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            if r.fieldnames and all(k in r.fieldnames for k in key_fields):
                for row in r:
                    seen.add(tuple(row.get(k, "") for k in key_fields))
    return seen

class FileLock:
    """Cross-platform file lock: exclusive .lock file next to the registry."""
    def __init__(self, target_csv: Path, timeout_ms: int = 5000, poll_ms: int = 50):
        self.lock_path = target_csv.with_suffix(target_csv.suffix + ".lock")
        self.timeout_ms = timeout_ms
        self.poll_ms = poll_ms
    def __enter__(self):
        ensure_dir(self.lock_path)
        start = time.time()
        while True:
            try:
                fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                os.write(fd, str(os.getpid()).encode("utf-8"))
                os.close(fd)
                return self
            except FileExistsError:
                if (time.time() - start) * 1000 > self.timeout_ms:
                    raise TimeoutError(f"Timeout acquiring lock: {self.lock_path}")
                time.sleep(self.poll_ms / 1000.0)
    def __exit__(self, exc_type, exc, tb):
        try: os.unlink(self.lock_path)
        except FileNotFoundError: pass

def append_registry(csv_path: Path, row: dict) -> None:
    """Append one row with header creation + fsync, under lock."""
    ensure_dir(csv_path)
    with FileLock(csv_path):
        new_file = (not csv_path.exists()) or (csv_path.stat().st_size == 0)
        with csv_path.open("a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=REGISTRY_FIELDS)
            if new_file:
                w.writeheader()
            w.writerow(row)
            f.flush()
            os.fsync(f.fileno())

# ---- tile parsing helpers ----

def parse_ra_dec_from_tile_id(tile_id: str):
    """tile-RA{:.3f}-DEC{:+.3f} -> (ra_deg, dec_deg)"""
    try:
        if not tile_id.startswith("tile-RA") or "-DEC" not in tile_id: return None
        ra_str = tile_id[len("tile-RA"): tile_id.index("-DEC")]
        dec_str = tile_id[tile_id.index("-DEC") + len("-DEC") :]
        return float(ra_str), float(dec_str)
    except Exception:
        return None

def find_raw_fits_and_header(raw_dir: Path):
    """Return (fits_path, header_json_path or None)."""
    fits = None; header = None
    # Prefer .fits first; include compressed variants
    for ext in (".fits", ".fit", ".fits.gz", ".fit.gz", ".fz", ".fit.fz", ".fits.fz"):
        candidates = sorted(raw_dir.glob(f"*{ext}"))
        if candidates:
            fits = candidates[0]
            break
    if fits:
        hj = raw_dir / (fits.name + ".header.json")
        if hj.exists(): header = hj
    else:
        # No FITS? try standalone header JSONs
        cand_json = sorted(raw_dir.glob("*.fits.header.json")) + sorted(raw_dir.glob("*.fit.header.json"))
        if cand_json:
            header = cand_json[0]
    return fits, header

def infer_from_header_json(header_json: Path):
    """
    Extract survey, pixel scale (arcsec/pixel), size (arcmin) from header JSON.
    - survey from header['SURVEY'] if present
    - pixel scale from CD matrix or CDELT (degrees/pixel -> arcsec)
    - size from NAXIS1/2 and scale
    """
    try:
        data = json.loads(header_json.read_text(encoding="utf-8"))
        hdr = data.get("header", data)
    except Exception:
        return None

    survey = str(hdr.get("SURVEY", "")).strip()

    # pixel scale: degrees per pixel from CD / CDELT, then -> arcsec
    pix_deg_x = None; pix_deg_y = None
    # CD matrix
    cd11 = hdr.get("CD1_1"); cd12 = hdr.get("CD1_2")
    cd21 = hdr.get("CD2_1"); cd22 = hdr.get("CD2_2")
    if all(v is not None for v in (cd11, cd12)):
        try: pix_deg_x = math.sqrt(float(cd11)**2 + float(cd12)**2)
        except: pass
    if all(v is not None for v in (cd21, cd22)):
        try: pix_deg_y = math.sqrt(float(cd21)**2 + float(cd22)**2)
        except: pass
    # CDELT fallback
    cdelt1 = hdr.get("CDELT1"); cdelt2 = hdr.get("CDELT2")
    if pix_deg_x is None and cdelt1 is not None:
        try: pix_deg_x = abs(float(cdelt1))
        except: pass
    if pix_deg_y is None and cdelt2 is not None:
        try: pix_deg_y = abs(float(cdelt2))
        except: pass

    # Prefer average pixel scale if both axes present
    pixel_scale_arcsec = None
    if pix_deg_x or pix_deg_y:
        vals = [v for v in (pix_deg_x, pix_deg_y) if v is not None]
        pixel_scale_arcsec = (sum(vals)/len(vals)) * 3600.0

    # size: NAXIS1/2 * pix_arcsec -> arcmin
    size_arcmin = None
    n1 = hdr.get("NAXIS1"); n2 = hdr.get("NAXIS2")
    if pixel_scale_arcsec and n1 and n2:
        try:
            w_arcmin = (float(n1) * pixel_scale_arcsec) / 60.0
            h_arcmin = (float(n2) * pixel_scale_arcsec) / 60.0
            # report the larger side for registry visibility (or average; your choice)
            size_arcmin = round(max(w_arcmin, h_arcmin), 3)
        except:
            pass

    return {
        "survey": survey or "",
        "pixel_scale_arcsec": None if pixel_scale_arcsec is None else round(pixel_scale_arcsec, 3),
        "size_arcmin": size_arcmin,
    }

def main():
    ap = argparse.ArgumentParser(description="Backfill tiles_registry.csv from existing tiles.")
    ap.add_argument("--tiles-root", default="./data/tiles")
    ap.add_argument("--registry", default="./data/metadata/tiles_registry.csv")
    ap.add_argument("--default-survey", default="dss1-red")
    ap.add_argument("--default-size-arcmin", type=float, default=30.0)
    ap.add_argument("--default-pixel-scale-arcsec", type=float, default=1.7)
    ap.add_argument("--source-tag", default="backfill")
    args = ap.parse_args()

    tiles_root = Path(args.tiles_root)
    registry = Path(args.registry)

    seen = load_seen(registry)

    tiles = sorted([p for p in tiles_root.glob("tile-RA*-DEC*") if p.is_dir()])
    if not tiles:
        print("[INFO] No tile folders found.")
        return 0

    appended = 0; skipped = 0; missing = 0
    for tile_dir in tiles:
        tile_id = tile_dir.name
        coords = parse_ra_dec_from_tile_id(tile_id)
        if not coords:
            print(f"[WARN] Unexpected tile folder name, skipping: {tile_id}")
            continue
        ra_deg, dec_deg = coords
        raw_dir = tile_dir / "raw"
        fits, header = find_raw_fits_and_header(raw_dir)

        if not fits and not header:
            missing += 1
            continue

        # Infer parameters from header JSON if present; else fallback
        inferred = infer_from_header_json(header) if header else None
        survey = (inferred and inferred.get("survey")) or args.default_survey
        px = (inferred and inferred.get("pixel_scale_arcsec")) or args.default_pixel_scale_arcsec
        size = (inferred and inferred.get("size_arcmin")) or args.default_size_arcmin

        key = (tile_id, survey, str(size), f"{px:.3f}")
        if key in seen:
            skipped += 1
            continue

        row = {
            "tile_id": tile_id,
            "ra_deg": f"{ra_deg:.6f}",
            "dec_deg": f"{dec_deg:.6f}",
            "survey": survey,
            "size_arcmin": str(size),
            "pixel_scale_arcsec": f"{px:.3f}",
            "status": "ok",
            "downloaded_utc": datetime.now(timezone.utc).isoformat(),
            "source": args.source_tag,
            "notes": "",
        }
        try:
            append_registry(registry, row)
            seen.add(key)
            appended += 1
        except Exception as e:
            print(f"[WARN] Append failed for {tile_id}: {e}")

    print({"appended": appended, "skipped_existing": skipped, "tiles_missing_raw": missing,
           "registry": str(registry)})
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

