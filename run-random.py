#!/usr/bin/env python3
"""
VASCO runner (reworked) — two modes:

1) download_loop
   - Continuously picks random tiles and runs **step1-download** until user interrupts.

2) steps
   - Scans **data/tiles/**; for each requested step, executes it on tiles that
     don't yet have that step's outputs.

Examples:
  # 1) Continuous download
  python run-random.py download_loop --sleep-sec 15 --size-arcmin 30 --survey dss1-red

  # 2) Run pass1 for all tiles that have raw FITS but no pass1 yet
  python run-random.py steps --steps 2 --size-arcmin 30

  # 3) Run PSFEx+pass2 + CDS xmatch + within5 + summarize across all tiles
  python run-random.py steps --steps 3,4,5,6 --xmatch-backend cds     --cds-gaia-table I/355/gaiadr3 --cds-ps1-table II/349/ps1
"""
import os
import sys
import json
import random
import logging
import time
from pathlib import Path
from subprocess import Popen, PIPE
import argparse

# --- DEFAULTS ---
WORKDIR_ROOT = "data/tiles"      # tile-based storage root (data/ is git-ignored)
SURVEY = "dss1-red"
PIXEL_SCALE = 1.7
TILE_SIZE_ARCMIN = 30             # 30×30 arcmin tiles
LOG_FILE = "logs/run_random.log"  # logs outside data/
RA_MIN, RA_MAX = 0, 360
DEC_MIN, DEC_MAX = -90, 90        # default to full sky

# --- logging ---
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, mode='a')
    ]
)
log = logging.getLogger("vasco_random_run")

# --- helpers: tile id & subprocess streaming ---

def tile_id_from_coords(ra_deg: float, dec_deg: float, nd: int = 3) -> str:
    """Return tile-RA<ra>-DEC<dec> with fixed decimals and signed DEC."""
    return f"tile-RA{ra_deg:.{nd}f}-DEC{dec_deg:+.{nd}f}"


def run_and_stream(cmd: list[str]) -> int:
    log.info(f"Running: {' '.join(cmd)}")
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE, text=True, bufsize=1)
    for line in proc.stdout:
        sys.stdout.write(line)
    for line in proc.stderr:
        sys.stderr.write(line)
    proc.wait()
    return proc.returncode

# --- status checks per tile ---

def has_raw_fits(tile_dir: Path) -> bool:
    return any(tile_dir.joinpath('raw').glob('*.fits'))


def has_pass1(tile_dir: Path) -> bool:
    return tile_dir.joinpath('pass1.ldac').exists()


def has_pass2(tile_dir: Path) -> bool:
    return tile_dir.joinpath('pass2.ldac').exists()


def has_xmatch(tile_dir: Path) -> bool:
    xdir = tile_dir.joinpath('xmatch')
    if not xdir.exists():
        return False
    patterns = [
        'sex_*_xmatch.csv',
        'sex_*_xmatch_cdss.csv',
    ]
    for pat in patterns:
        if any(xdir.glob(pat)):
            return True
    return False


def has_within5(tile_dir: Path) -> bool:
    xdir = tile_dir.joinpath('xmatch')
    return any(xdir.glob('*_within5arcsec.csv'))


def has_summary(tile_dir: Path) -> bool:
    return tile_dir.joinpath('RUN_SUMMARY.md').exists()

# --- tessellation (random centers) ---

def random_ra_dec() -> tuple[float, float]:
    ra = random.uniform(RA_MIN, RA_MAX)
    dec = random.uniform(DEC_MIN, DEC_MAX)
    return ra, dec

# --- CLI orchestrations ---

def cmd_download_loop(args: argparse.Namespace) -> int:
    log.info("Starting download loop — CTRL+C to stop.")
    try:
        while True:
            ra, dec = random_ra_dec()
            tid = tile_id_from_coords(ra, dec)
            workdir_tile = os.path.join(WORKDIR_ROOT, tid)
            os.makedirs(workdir_tile, exist_ok=True)
            log.info(f"Selected tile: RA={ra:.5f}, Dec={dec:.5f} -> {tid}")
            cmd = [
                "python", "-u", "-m", "vasco.cli_pipeline", "step1-download",
                "--ra", str(ra),
                "--dec", str(dec),
                "--size-arcmin", str(args.size_arcmin or TILE_SIZE_ARCMIN),
                "--survey", args.survey or SURVEY,
                "--pixel-scale-arcsec", str(args.pixel_scale or PIXEL_SCALE),
                "--workdir", workdir_tile,
            ]
            rc = run_and_stream(cmd)
            if rc != 0:
                log.warning(f"Download failed for {tid} (rc={rc}).")
            time.sleep(float(args.sleep_sec or 15))
    except KeyboardInterrupt:
        log.info("Interrupted by user. Exiting download loop.")
    return 0


def cmd_steps(args: argparse.Namespace) -> int:
    # Normalize steps input; allow "2,3,4,5,6" or names
    raw = (args.steps or '').strip()
    if not raw:
        log.error("No steps specified. Use --steps 2,3,4,5,6 or names like step2-pass1.")
        return 2
    mapping = {
        '2': 'step2-pass1', 'step2': 'step2-pass1', 'step2-pass1': 'step2-pass1',
        '3': 'step3-psf-and-pass2', 'step3': 'step3-psf-and-pass2', 'step3-psf-and-pass2': 'step3-psf-and-pass2',
        '4': 'step4-xmatch', 'step4': 'step4-xmatch', 'step4-xmatch': 'step4-xmatch',
        '5': 'step5-filter-within5', 'step5': 'step5-filter-within5', 'step5-filter-within5': 'step5-filter-within5',
        '6': 'step6-summarize', 'step6': 'step6-summarize', 'step6-summarize': 'step6-summarize',
    }
    steps = []
    for token in raw.split(','):
        t = token.strip()
        if not t:
            continue
        canon = mapping.get(t)
        if not canon:
            log.error(f"Unknown step token: {t}")
            return 2
        steps.append(canon)
    steps = list(dict.fromkeys(steps))  # de-dup preserving order

    # Scan tiles
    root = Path(args.workdir_root or WORKDIR_ROOT)
    if not root.exists():
        log.error(f"Tiles root not found: {root}")
        return 2
    tiles = sorted([p for p in root.glob('tile-RA*-DEC*') if p.is_dir()])
    if not tiles:
        log.info("No tiles under data/tiles. Run download_loop first.")
        return 0

    total_runs = 0
    limit = int(args.limit or 0)

    for tile_dir in tiles:
        # Decide per step whether this tile needs work
        for step in steps:
            if step == 'step2-pass1':
                if not has_raw_fits(tile_dir) or has_pass1(tile_dir):
                    continue
                cmd = ["python", "-u", "-m", "vasco.cli_pipeline", "step2-pass1",
                       "--workdir", str(tile_dir)]
            elif step == 'step3-psf-and-pass2':
                if not has_pass1(tile_dir) or has_pass2(tile_dir):
                    continue
                cmd = ["python", "-u", "-m", "vasco.cli_pipeline", "step3-psf-and-pass2",
                       "--workdir", str(tile_dir)]
            elif step == 'step4-xmatch':
                if not has_pass2(tile_dir) or has_xmatch(tile_dir):
                    continue
                cmd = ["python", "-u", "-m", "vasco.cli_pipeline", "step4-xmatch",
                       "--workdir", str(tile_dir),
                       "--xmatch-backend", args.xmatch_backend or 'cds',
                       "--xmatch-radius-arcsec", str(args.xmatch_radius or 5.0),
                       "--size-arcmin", str(args.size_arcmin or TILE_SIZE_ARCMIN)]
                # Add CDS tables if backend=cds and tables provided
                if (args.xmatch_backend or 'cds') == 'cds':
                    if args.cds_gaia_table:
                        cmd += ["--cds-gaia-table", args.cds_gaia_table]
                    if args.cds_ps1_table:
                        cmd += ["--cds-ps1-table", args.cds_ps1_table]
            elif step == 'step5-filter-within5':
                if not has_xmatch(tile_dir) or has_within5(tile_dir):
                    continue
                cmd = ["python", "-u", "-m", "vasco.cli_pipeline", "step5-filter-within5",
                       "--workdir", str(tile_dir)]
            elif step == 'step6-summarize':
                if not has_pass2(tile_dir) or has_summary(tile_dir):
                    continue
                cmd = ["python", "-u", "-m", "vasco.cli_pipeline", "step6-summarize",
                       "--workdir", str(tile_dir),
                       "--export", args.export or 'csv',
                       "--hist-col", args.hist_col or 'FWHM_IMAGE']
            else:
                continue

            log.info(f"[RUN] {step} -> {tile_dir.name}")
            rc = run_and_stream(cmd)
            if rc != 0:
                log.warning(f"Step {step} failed for {tile_dir.name} (rc={rc}).")
            total_runs += 1
            if limit and total_runs >= limit:
                log.info(f"Reached limit={limit}. Stopping.")
                return 0
    log.info(f"Steps completed. Total step invocations: {total_runs}")
    return 0

# --- argparse entry ---

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description='VASCO runner: download loop and step sweeper')
    sub = ap.add_subparsers(dest='cmd')

    dl = sub.add_parser('download_loop', help='Continuously download tiles (step1) until interrupted')
    dl.add_argument('--sleep-sec', type=float, default=15)
    dl.add_argument('--size-arcmin', type=float, default=TILE_SIZE_ARCMIN)
    dl.add_argument('--survey', default=SURVEY)
    dl.add_argument('--pixel-scale', type=float, default=PIXEL_SCALE)
    dl.set_defaults(func=cmd_download_loop)

    st = sub.add_parser('steps', help='Scan tiles and run requested steps where missing')
    st.add_argument('--steps', required=True, help='Comma list: 2,3,4,5,6 or names like step3-psf-and-pass2')
    st.add_argument('--workdir-root', default=WORKDIR_ROOT)
    st.add_argument('--limit', type=int, default=0, help='Max number of step runs to execute (0=no limit)')
    st.add_argument('--size-arcmin', type=float, default=TILE_SIZE_ARCMIN)
    st.add_argument('--xmatch-backend', choices=['local','cds'], default='cds')
    st.add_argument('--xmatch-radius', type=float, default=5.0)
    st.add_argument('--cds-gaia-table', default=os.getenv('VASCO_CDS_GAIA_TABLE'))
    st.add_argument('--cds-ps1-table',  default=os.getenv('VASCO_CDS_PS1_TABLE'))
    st.add_argument('--export', choices=['none','csv','parquet','both'], default='csv')
    st.add_argument('--hist-col', default='FWHM_IMAGE')
    st.set_defaults(func=cmd_steps)

    args = ap.parse_args(argv)
    if hasattr(args, 'func'):
        return args.func(args)
    ap.print_help()
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
