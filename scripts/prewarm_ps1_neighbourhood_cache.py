#!/usr/bin/env python3
from __future__ import annotations

import os
import json
import time
import signal
import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed


def _add_repo_to_syspath() -> Path:
    """Make the 'vasco' package importable without requiring pip install -e."""
    here = Path(__file__).resolve()
    repo_root = None
    add_path = None
    for p in [here] + list(here.parents):
        if (p / "vasco" / "__init__.py").exists():
            repo_root = p
            add_path = p
            break
        if (p / "src" / "vasco" / "__init__.py").exists():
            repo_root = p
            add_path = p / "src"
            break
    if add_path is None:
        cwd = Path.cwd().resolve()
        if str(cwd) not in sys.path:
            sys.path.insert(0, str(cwd))
        raise ModuleNotFoundError(
            "Could not locate 'vasco' package. Looked for vasco/__init__.py or src/vasco/__init__.py "
            f"from {here} upwards. Current working dir is {cwd}."
        )
    if str(add_path) not in sys.path:
        sys.path.insert(0, str(add_path))
    return repo_root or Path.cwd().resolve()


_DETECTED_REPO = _add_repo_to_syspath()

from vasco.external_fetch_online import fetch_ps1_neighbourhood

TILE_PREFIX = "tile-RA"
PS1_DEC_LIMIT = -30.0


def iter_tile_dirs_sharded(tiles_root: Path):
    for root, dirs, _files in os.walk(tiles_root):
        for d in dirs:
            if d.startswith(TILE_PREFIX) and "-DEC" in d:
                yield Path(root) / d


def parse_center_from_tile_name(name: str):
    try:
        if not name.startswith("tile-RA") or "-DEC" not in name:
            return None
        ra_part = name[len("tile-RA"): name.index("-DEC")]
        dec_part = name[name.index("-DEC") + len("-DEC"):]
        return float(ra_part), float(dec_part)
    except Exception:
        return None


def atomic_write_text(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def cache_exists(path: Path) -> bool:
    try:
        return path.exists() and path.stat().st_size > 0
    except Exception:
        return False


def _ps1_radius_deg(radius_arcmin: float) -> float:
    # Keep consistent with fetcher behavior: allow override via env
    env = os.getenv('VASCO_PS1_RADIUS_DEG')
    if env:
        try:
            return float(env)
        except Exception:
            pass
    return float(radius_arcmin) / 60.0


def _outside_ps1_coverage(dec_deg: float, radius_deg: float) -> bool:
    return (float(dec_deg) + float(radius_deg)) < PS1_DEC_LIMIT


def _default_ps1_cols() -> list[str]:
    # Keep aligned with external_fetch_online.py default
    return [
        'objID','raMean','decMean','nDetections','ng','nr','ni','nz','ny',
        'gMeanPSFMag','rMeanPSFMag','iMeanPSFMag','zMeanPSFMag','yMeanPSFMag'
    ]


def _effective_ps1_cols() -> list[str]:
    ov = os.getenv('VASCO_PS1_COLUMNS')
    if ov:
        return [c.strip() for c in ov.split(',') if c.strip()]
    return _default_ps1_cols()


def write_empty_ps1_neighbourhood(path: Path):
    """Write a schema-valid empty PS1 neighbourhood CSV (header only)."""
    cols = _effective_ps1_cols()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(','.join(cols) + '\n', encoding='utf-8')
    tmp.replace(path)


class StopRequested(Exception):
    pass


def main():
    import argparse

    ap = argparse.ArgumentParser(description="Prewarm per-tile PS1 neighbourhood caches (resumable).")
    ap.add_argument("--tiles-root", default="./data/tiles_by_sky")
    ap.add_argument("--logs-dir", default="./logs")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--radius-arcmin", type=float, default=35.0,
                    help="Neighbourhood radius (arcmin). Default 35 to support spike-cache derivation.")
    ap.add_argument("--max-records", type=int, default=50000)
    ap.add_argument("--timeout", type=float, default=60.0)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--retry", type=int, default=3)
    ap.add_argument("--progress-every", type=int, default=100)
    args = ap.parse_args()

    tiles_root = Path(args.tiles_root)
    logs_dir = Path(args.logs_dir)
    logs_dir.mkdir(parents=True, exist_ok=True)

    log_path = logs_dir / "prewarm_ps1_neighbourhood_cache.log"
    progress_path = logs_dir / "prewarm_ps1_neighbourhood_progress.json"
    stop_file = logs_dir / "PREWARM_PS1_NEIGH_STOP"

    logger = logging.getLogger("prewarm_ps1_neigh")
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(log_path, maxBytes=10_000_000, backupCount=5, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
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
        "tiles_ps1_outside_coverage": 0,
        "tiles_zero_rows": 0,
        "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "last_tile": None,
        "radius_arcmin": float(args.radius_arcmin),
        "ps1_radius_deg_effective": _ps1_radius_deg(args.radius_arcmin),
        "ps1_columns": _effective_ps1_cols(),
    }

    def write_progress():
        atomic_write_text(progress_path, json.dumps(counters, indent=2))

    def do_one(tile_dir: Path):
        if stop["flag"] or stop_file.exists():
            raise StopRequested()

        tile_id = tile_dir.name
        counters["last_tile"] = tile_id

        out = tile_dir / "catalogs" / "ps1_neighbourhood.csv"
        if cache_exists(out):
            return ("cached", tile_id, 0)

        ctr = parse_center_from_tile_name(tile_id)
        if not ctr:
            return ("no_center", tile_id, 0)

        ra, dec = ctr
        radius_deg = _ps1_radius_deg(args.radius_arcmin)

        # PS1 DR2 coverage guard
        if _outside_ps1_coverage(dec_deg=dec, radius_deg=radius_deg):
            write_empty_ps1_neighbourhood(out)
            return ("outside", tile_id, 0)

        last_err = None
        for attempt in range(1, args.retry + 1):
            try:
                # external_fetch_online honors env overrides (radius/timeout/attempts/columns)
                # We pass radius_arcmin here as intent; effective radius may be overridden by env.
                fetch_ps1_neighbourhood(
                    tile_dir, ra, dec, float(args.radius_arcmin),
                    max_records=int(args.max_records), timeout=float(args.timeout)
                )
                # Count data rows quickly (excluding header)
                rows = 0
                try:
                    with out.open('r', encoding='utf-8', errors='ignore') as f:
                        next(f, None)
                        for _ in f:
                            rows += 1
                            if rows > 0:
                                break
                except Exception:
                    rows = -1
                return ("fetched", tile_id, rows)
            except Exception as e:
                last_err = str(e)
                time.sleep(min(2 * attempt, 10))

        return ("failed", tile_id, last_err or "unknown_error")

    tiles = list(iter_tile_dirs_sharded(tiles_root))
    counters["tiles_found"] = len(tiles)
    logger.info(f"prewarm start: tiles_root={tiles_root} tiles_found={len(tiles)} workers={args.workers}")
    write_progress()

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
                elif status == "outside":
                    counters["tiles_ps1_outside_coverage"] += 1
                    logger.info(f"[SKIP] {tile_id} ps1_outside_coverage (wrote empty ps1_neighbourhood.csv)")
                elif status == "fetched":
                    counters["tiles_fetched"] += 1
                    if int(meta) == 0:
                        counters["tiles_zero_rows"] += 1
                    logger.info(f"[OK] {tile_id} ps1_neighbourhood ready")
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
                    f"outside={counters['tiles_ps1_outside_coverage']} failed={counters['tiles_failed']}"
                )

    write_progress()
    logger.info("prewarm done: " + json.dumps(counters))


if __name__ == "__main__":
    main()
