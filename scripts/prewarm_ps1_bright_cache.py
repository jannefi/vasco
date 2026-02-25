#!/usr/bin/env python3
from __future__ import annotations

import os
import json
import time
import signal
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from vasco.mnras.spikes import fetch_bright_ps1  # uses your current PS1-based bright-star fetcher

TILE_PREFIX = "tile-RA"

def iter_tile_dirs_sharded(tiles_root: Path):
    # tiles_root: ./data/tiles_by_sky
    for root, dirs, _files in os.walk(tiles_root):
        for d in dirs:
            if d.startswith(TILE_PREFIX) and "-DEC" in d:
                yield Path(root) / d

def parse_center_from_tile_name(name: str):
    # tile-RA19.285-DEC+17.868
    try:
        if not name.startswith("tile-RA") or "-DEC" not in name:
            return None
        ra_part = name[len("tile-RA"): name.index("-DEC")]
        dec_part = name[name.index("-DEC") + len("-DEC") :]
        return float(ra_part), float(dec_part)
    except Exception:
        return None

def atomic_write_text(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)

def write_cache_csv(cache_path: Path, stars):
    import csv
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = cache_path.with_suffix(cache_path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["ra", "dec", "rmag"])
        w.writeheader()
        for s in stars:
            w.writerow({"ra": s.ra, "dec": s.dec, "rmag": s.rmag})
    tmp.replace(cache_path)

def cache_exists(cache_path: Path) -> bool:
    try:
        return cache_path.exists() and cache_path.stat().st_size > 0
    except Exception:
        return False

class StopRequested(Exception):
    pass

def main():
    import argparse
    ap = argparse.ArgumentParser(description="Prewarm per-tile PS1 bright-star caches (resumable).")
    ap.add_argument("--tiles-root", default="./data/tiles_by_sky", help="Sharded tiles root (default: ./data/tiles_by_sky)")
    ap.add_argument("--logs-dir", default="./logs", help="Logs directory (default: ./logs)")
    ap.add_argument("--workers", type=int, default=4, help="Parallel workers (default: 4)")
    ap.add_argument("--radius-arcmin", type=float, default=35.0, help="PS1 search radius in arcmin (default: 35)")
    ap.add_argument("--rmag-max", type=float, default=16.0, help="PS1 rMeanPSFMag upper cutoff (default: 16)")
    ap.add_argument("--mindetections", type=int, default=2, help="PS1 nDetections.gte (default: 2)")
    ap.add_argument("--limit", type=int, default=0, help="0=all tiles; else schedule only N tiles")
    ap.add_argument("--retry", type=int, default=3, help="Retries per tile on fetch error (default: 3)")
    ap.add_argument("--progress-every", type=int, default=100, help="Write progress JSON every N results (default: 100)")
    args = ap.parse_args()

    tiles_root = Path(args.tiles_root)
    logs_dir = Path(args.logs_dir)
    logs_dir.mkdir(parents=True, exist_ok=True)

    log_path = logs_dir / "prewarm_ps1_cache.log"
    progress_path = logs_dir / "prewarm_ps1_progress.json"
    stop_file = logs_dir / "PREWARM_STOP"

    # Logger (rotating)
    logger = logging.getLogger("prewarm_ps1")
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(log_path, maxBytes=10_000_000, backupCount=5, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)

    # also echo to stdout (captured by nohup output if used)
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(sh)

    stop = {"flag": False}
    def _sig_handler(_sig, _frame):
        stop["flag"] = True
        logger.warning("Stop signal received; exiting after in-flight tasks complete.")
    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    counters = {
        "tiles_found": 0,
        "tiles_scheduled": 0,
        "tiles_cached_skip": 0,
        "tiles_fetched": 0,
        "tiles_failed": 0,
        "tiles_no_center": 0,
        "total_stars_written": 0,
        "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "last_tile": None,
    }

    def write_progress():
        atomic_write_text(progress_path, json.dumps(counters, indent=2))

    def do_one(tile_dir: Path):
        if stop["flag"] or stop_file.exists():
            raise StopRequested()

        tile_id = tile_dir.name
        counters["last_tile"] = tile_id

        cache_path = tile_dir / "catalogs" / "ps1_bright_stars_r16_rad35.csv"
        if cache_exists(cache_path):
            return ("cached", tile_id, 0)

        ctr = parse_center_from_tile_name(tile_id)
        if not ctr:
            return ("no_center", tile_id, 0)

        ra, dec = ctr

        last_err = None
        for attempt in range(1, args.retry + 1):
            try:
                stars = fetch_bright_ps1(
                    ra, dec,
                    radius_arcmin=args.radius_arcmin,
                    rmag_max=args.rmag_max,
                    mindetections=args.mindetections,
                )
                write_cache_csv(cache_path, stars)
                return ("fetched", tile_id, len(stars))
            except Exception as e:
                last_err = str(e)
                # small backoff
                time.sleep(min(2 * attempt, 10))

        return ("failed", tile_id, last_err or "unknown_error")

    tiles = list(iter_tile_dirs_sharded(tiles_root))
    counters["tiles_found"] = len(tiles)
    logger.info(f"prewarm start: tiles_root={tiles_root} tiles_found={len(tiles)} workers={args.workers}")
    write_progress()

    # Schedule tasks
    to_run = []
    for td in tiles:
        if args.limit and len(to_run) >= args.limit:
            break
        to_run.append(td)
    counters["tiles_scheduled"] = len(to_run)

    logger.info(f"prewarm scheduled: {len(to_run)} tiles")
    write_progress()

    done_count = 0
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futs = {ex.submit(do_one, td): td for td in to_run}

        for fut in as_completed(futs):
            td = futs[fut]
            try:
                status, tile_id, meta = fut.result()
                if status == "cached":
                    counters["tiles_cached_skip"] += 1
                elif status == "fetched":
                    counters["tiles_fetched"] += 1
                    counters["total_stars_written"] += int(meta)
                    logger.info(f"[OK] {tile_id} stars={meta}")
                elif status == "no_center":
                    counters["tiles_no_center"] += 1
                    logger.warning(f"[SKIP] {tile_id} no_center")
                else:
                    counters["tiles_failed"] += 1
                    logger.warning(f"[FAIL] {tile_id} err={meta}")

            except StopRequested:
                logger.warning("StopRequested: exiting loop.")
                stop["flag"] = True
                break
            except Exception as e:
                counters["tiles_failed"] += 1
                logger.warning(f"[FAIL] {td.name} unexpected={e}")

            done_count += 1
            if done_count % args.progress_every == 0:
                write_progress()
                logger.info(
                    f"progress: done={done_count}/{len(to_run)} "
                    f"fetched={counters['tiles_fetched']} cached={counters['tiles_cached_skip']} "
                    f"failed={counters['tiles_failed']} no_center={counters['tiles_no_center']}"
                )

    write_progress()
    logger.info("prewarm done: " + json.dumps(counters))

if __name__ == "__main__":
    main()
