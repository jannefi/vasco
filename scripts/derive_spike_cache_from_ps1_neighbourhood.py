#!/usr/bin/env python3
from __future__ import annotations

import os
import csv
import json
import time
import signal
import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

TILE_PREFIX = "tile-RA"
PS1_DEC_LIMIT = -30.0


def _add_repo_to_syspath() -> Path:
    """Make the repo importable if we later want to reuse helpers; safe no-op if already set."""
    here = Path(__file__).resolve()
    for p in [here] + list(here.parents):
        if (p / "vasco" / "__init__.py").exists():
            if str(p) not in sys.path:
                sys.path.insert(0, str(p))
            return p
        if (p / "src" / "vasco" / "__init__.py").exists():
            src = p / "src"
            if str(src) not in sys.path:
                sys.path.insert(0, str(src))
            return p
    return Path.cwd().resolve()


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


def cache_has_data_rows(path: Path) -> bool:
    """Return True if CSV exists and has at least one data row."""
    try:
        if not path.exists() or path.stat().st_size == 0:
            return False
        with path.open('r', encoding='utf-8', errors='ignore', newline='') as f:
            r = csv.reader(f)
            next(r, None)  # header
            return next(r, None) is not None
    except Exception:
        return False


def _outside_ps1_coverage(dec_deg: float, radius_arcmin: float) -> bool:
    radius_deg = float(radius_arcmin) / 60.0
    return (float(dec_deg) + radius_deg) < PS1_DEC_LIMIT


def write_spike_cache(out_path: Path, rows: list[dict]):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    with tmp.open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['ra', 'dec', 'rmag'])
        w.writeheader()
        for row in rows:
            w.writerow(row)
    tmp.replace(out_path)


def write_empty_spike_cache(out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    with tmp.open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['ra', 'dec', 'rmag'])
        w.writeheader()
    tmp.replace(out_path)


class StopRequested(Exception):
    pass


def main():
    import argparse

    _add_repo_to_syspath()

    ap = argparse.ArgumentParser(
        description="Derive per-tile PS1 spike/bright-star cache from existing PS1 neighbourhood cache (no network)."
    )
    ap.add_argument('--tiles-root', default='./data/tiles_by_sky')
    ap.add_argument('--logs-dir', default='./logs')
    ap.add_argument('--workers', type=int, default=6)
    ap.add_argument('--radius-arcmin', type=float, default=35.0,
                    help='Used only for PS1 coverage guard (default 35)')
    ap.add_argument('--rmag-max', type=float, default=16.0)
    ap.add_argument('--mindetections', type=int, default=2)
    ap.add_argument('--overwrite', action='store_true',
                    help='Overwrite spike cache even if it already has data rows')
    ap.add_argument('--limit', type=int, default=0)
    ap.add_argument('--progress-every', type=int, default=200)
    args = ap.parse_args()

    tiles_root = Path(args.tiles_root)
    logs_dir = Path(args.logs_dir)
    logs_dir.mkdir(parents=True, exist_ok=True)

    log_path = logs_dir / 'derive_spike_cache_from_ps1_neighbourhood.log'
    progress_path = logs_dir / 'derive_spike_cache_from_ps1_neighbourhood_progress.json'
    stop_file = logs_dir / 'DERIVE_SPIKE_STOP'

    logger = logging.getLogger('derive_spike')
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(log_path, maxBytes=10_000_000, backupCount=5, encoding='utf-8')
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    logger.addHandler(handler)
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    logger.addHandler(sh)

    stop = {'flag': False}

    def _sig_handler(_sig, _frame):
        stop['flag'] = True
        logger.warning('Stop signal received; exiting after in-flight tasks complete.')

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    counters = {
        'tiles_found': 0,
        'tiles_scheduled': 0,
        'tiles_cached_skip': 0,
        'tiles_written': 0,
        'tiles_failed': 0,
        'tiles_no_center': 0,
        'tiles_missing_ps1_neighbourhood': 0,
        'tiles_ps1_outside_coverage': 0,
        'tiles_zero_stars': 0,
        'total_stars_written': 0,
        'started_at': time.strftime('%Y-%m-%d %H:%M:%S'),
        'last_tile': None,
        'rmag_max': float(args.rmag_max),
        'mindetections': int(args.mindetections),
    }

    def write_progress():
        atomic_write_text(progress_path, json.dumps(counters, indent=2))

    def do_one(tile_dir: Path):
        if stop['flag'] or stop_file.exists():
            raise StopRequested()

        tile_id = tile_dir.name
        counters['last_tile'] = tile_id

        ps1_neigh = tile_dir / 'catalogs' / 'ps1_neighbourhood.csv'
        out = tile_dir / 'catalogs' / 'ps1_bright_stars_r16_rad35.csv'

        if (not args.overwrite) and cache_has_data_rows(out):
            return ('cached', tile_id, 0)

        ctr = parse_center_from_tile_name(tile_id)
        if not ctr:
            return ('no_center', tile_id, 0)
        ra0, dec0 = ctr

        # If entire cone is below PS1 footprint, write empty and mark outside
        if _outside_ps1_coverage(dec0, args.radius_arcmin):
            write_empty_spike_cache(out)
            return ('outside', tile_id, 0)

        if not ps1_neigh.exists() or ps1_neigh.stat().st_size == 0:
            return ('missing_neigh', tile_id, 0)

        # Read PS1 neighbourhood and filter
        try:
            with ps1_neigh.open('r', encoding='utf-8', errors='ignore', newline='') as f:
                rdr = csv.DictReader(f)
                fieldnames = rdr.fieldnames or []
                need = {'raMean', 'decMean', 'rMeanPSFMag'}
                if not need.issubset(set(fieldnames)):
                    # can't derive reliably
                    return ('failed', tile_id, f'missing columns: {sorted(list(need - set(fieldnames)))}')

                use_ndet = 'nDetections' in fieldnames
                out_rows = []
                for row in rdr:
                    try:
                        rmag = float(row.get('rMeanPSFMag', 'nan'))
                        if rmag > float(args.rmag_max):
                            continue
                        if use_ndet:
                            nd = int(float(row.get('nDetections', '0') or 0))
                            if nd < int(args.mindetections):
                                continue
                        ra = float(row.get('raMean', 'nan'))
                        dec = float(row.get('decMean', 'nan'))
                        if not (ra == ra and dec == dec and rmag == rmag):
                            continue
                        out_rows.append({'ra': ra, 'dec': dec, 'rmag': rmag})
                    except Exception:
                        continue

            write_spike_cache(out, out_rows)
            return ('written', tile_id, len(out_rows))
        except Exception as e:
            return ('failed', tile_id, str(e))

    tiles = list(iter_tile_dirs_sharded(tiles_root))
    counters['tiles_found'] = len(tiles)
    logger.info(f"derive start: tiles_root={tiles_root} tiles_found={len(tiles)} workers={args.workers}")
    write_progress()

    to_run = []
    for td in tiles:
        if args.limit and len(to_run) >= args.limit:
            break
        to_run.append(td)
    counters['tiles_scheduled'] = len(to_run)
    logger.info(f"derive scheduled: {len(to_run)} tiles")
    write_progress()

    done = 0
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futs = {ex.submit(do_one, td): td for td in to_run}
        for fut in as_completed(futs):
            td = futs[fut]
            try:
                status, tile_id, meta = fut.result()
                if status == 'cached':
                    counters['tiles_cached_skip'] += 1
                elif status == 'outside':
                    counters['tiles_ps1_outside_coverage'] += 1
                    logger.info(f"[SKIP] {tile_id} ps1_outside_coverage (wrote empty spike cache)")
                elif status == 'missing_neigh':
                    counters['tiles_missing_ps1_neighbourhood'] += 1
                elif status == 'written':
                    counters['tiles_written'] += 1
                    n = int(meta)
                    counters['total_stars_written'] += n
                    if n == 0:
                        counters['tiles_zero_stars'] += 1
                    logger.info(f"[OK] {tile_id} spike_cache stars={n}")
                elif status == 'no_center':
                    counters['tiles_no_center'] += 1
                    logger.warning(f"[SKIP] {tile_id} no_center")
                else:
                    counters['tiles_failed'] += 1
                    logger.warning(f"[FAIL] {tile_id} err={meta}")
            except StopRequested:
                logger.warning('StopRequested: exiting loop.')
                stop['flag'] = True
                break
            except Exception as e:
                counters['tiles_failed'] += 1
                logger.warning(f"[FAIL] {td.name} unexpected={e}")

            done += 1
            if done % int(args.progress_every) == 0:
                write_progress()
                logger.info(
                    f"progress: done={done}/{len(to_run)} "
                    f"written={counters['tiles_written']} cached={counters['tiles_cached_skip']} "
                    f"missing_neigh={counters['tiles_missing_ps1_neighbourhood']} outside={counters['tiles_ps1_outside_coverage']}"
                )

    write_progress()
    logger.info('derive done: ' + json.dumps(counters))


if __name__ == '__main__':
    main()
