
#!/usr/bin/env python3
"""
VASCO runner — modes:
 1) download_loop — continuously run step1-download until interrupted
 2) steps — sweep tiles and run requested steps (2..6) where missing
 3) download_from_tiles — scan existing tile folders under data/tiles and invoke step1-download per tile

Fix: --force now overrides --only-missing. Also added --no-only-missing to explicitly process all tiles without deleting them.
"""
import os, sys, json, random, logging, time, argparse
from pathlib import Path
from subprocess import Popen, PIPE

WORKDIR_ROOT = "data/tiles"
SURVEY = "dss1-red"
PIXEL_SCALE = 1.7
TILE_SIZE_ARCMIN = 30
LOG_FILE = "logs/run_random.log"
RA_MIN, RA_MAX = 0, 360
DEC_MIN, DEC_MAX = -90, 90

# logging
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(LOG_FILE, mode='a')])
log = logging.getLogger("vasco_random_run")

# ensure subprocesses import the repo copy
REPO_ROOT = Path(__file__).resolve().parent
BASE_ENV = os.environ.copy(); BASE_ENV['PYTHONPATH'] = str(REPO_ROOT)

# helpers

def tile_id_from_coords(ra_deg: float, dec_deg: float, nd: int = 3) -> str:
    return f"tile-RA{ra_deg:.{nd}f}-DEC{dec_deg:+.{nd}f}"

def parse_ra_dec_from_tile(dirname: str):
    try:
        if not dirname.startswith('tile-RA') or '-DEC' not in dirname:
            return None
        ra_str = dirname[len('tile-RA'): dirname.index('-DEC')]
        dec_str = dirname[dirname.index('-DEC') + len('-DEC') :]
        return float(ra_str), float(dec_str)
    except Exception:
        return None

def run_and_stream(cmd: list[str]) -> int:
    log.info(f"Running: {' '.join(cmd)}")
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE, text=True, bufsize=1, env=BASE_ENV)
    for line in proc.stdout: sys.stdout.write(line)
    for line in proc.stderr: sys.stderr.write(line)
    proc.wait(); return proc.returncode

def has_raw_fits(tile_dir: Path) -> bool:
    return any(tile_dir.joinpath('raw').glob('*.fits'))

# commands

def cmd_download_loop(args: argparse.Namespace) -> int:
    log.info("Starting download loop — CTRL+C to stop.")
    try:
        while True:
            ra = random.uniform(RA_MIN, RA_MAX)
            dec = random.uniform(DEC_MIN, DEC_MAX)
            tid = tile_id_from_coords(ra, dec)
            workdir_tile = os.path.join(WORKDIR_ROOT, tid); os.makedirs(workdir_tile, exist_ok=True)
            log.info(f"Selected tile: RA={ra:.5f}, Dec={dec:.5f} -> {tid}")
            cmd = [
                "python","-u","-m","vasco.cli_pipeline","step1-download",
                "--ra",str(ra),"--dec",str(dec),
                "--size-arcmin",str(args.size_arcmin or TILE_SIZE_ARCMIN),
                "--survey",args.survey or SURVEY,
                "--pixel-scale-arcsec",str(args.pixel_scale_arcsec or PIXEL_SCALE),
                "--workdir",workdir_tile,
            ]
            rc = run_and_stream(cmd)
            if rc != 0: log.warning(f"Download failed for {tid} (rc={rc}).")
            time.sleep(float(args.sleep_sec or 15))
    except KeyboardInterrupt:
        log.info("Interrupted by user. Exiting download loop.")
    return 0


def cmd_steps(args: argparse.Namespace) -> int:
    raw = (args.steps or '').strip()
    mapping = {
        '2':'step2-pass1','step2':'step2-pass1','step2-pass1':'step2-pass1',
        '3':'step3-psf-and-pass2','step3':'step3-psf-and-pass2','step3-psf-and-pass2':'step3-psf-and-pass2',
        '4':'step4-xmatch','step4':'step4-xmatch','step4-xmatch':'step4-xmatch',
        '5':'step5-filter-within5','step5':'step5-filter-within5','step5-filter-within5':'step5-filter-within5',
        '6':'step6-summarize','step6':'step6-summarize','step6-summarize':'step6-summarize',
    }
    steps = [mapping.get(t.strip()) for t in raw.split(',') if t.strip()]
    if not steps or any(s is None for s in steps):
        log.error("Unknown or missing --steps. Use 2,3,4,5,6 or names like step3-psf-and-pass2."); return 2
    steps = list(dict.fromkeys(steps))
    root = Path(args.workdir_root or WORKDIR_ROOT)
    tiles = sorted([p for p in root.glob('tile-RA*-DEC*') if p.is_dir()])
    if not tiles:
        log.info("No tiles under data/tiles. Run download_loop first."); return 0
    total_runs = 0; limit = int(args.limit or 0)
    for tile_dir in tiles:
        for step in steps:
            if step == 'step2-pass1':
                if not has_raw_fits(tile_dir) or (tile_dir / 'pass1.ldac').exists(): continue
                cmd = ["python","-u","-m","vasco.cli_pipeline","step2-pass1","--workdir",str(tile_dir)]
            elif step == 'step3-psf-and-pass2':
                if not (tile_dir / 'pass1.ldac').exists() or (tile_dir / 'pass2.ldac').exists(): continue
                cmd = ["python","-u","-m","vasco.cli_pipeline","step3-psf-and-pass2","--workdir",str(tile_dir)]
            elif step == 'step4-xmatch':
                if not (tile_dir / 'pass2.ldac').exists() or (tile_dir / 'xmatch').exists() and (any((tile_dir / 'xmatch').glob('sex_*_xmatch.csv')) or any((tile_dir / 'xmatch').glob('sex_*_xmatch_cdss.csv'))): continue
                cmd = ["python","-u","-m","vasco.cli_pipeline","step4-xmatch","--workdir",str(tile_dir),
                       "--xmatch-backend",args.xmatch_backend or 'cds',
                       "--xmatch-radius-arcsec",str(args.xmatch_radius or 5.0),
                       "--size-arcmin",str(args.size_arcmin or TILE_SIZE_ARCMIN)]
                if (args.xmatch_backend or 'cds') == 'cds':
                    if args.cds_gaia_table: cmd += ["--cds-gaia-table", args.cds_gaia_table]
                    if args.cds_ps1_table:  cmd += ["--cds-ps1-table",  args.cds_ps1_table]
            elif step == 'step5-filter-within5':
                xdir = tile_dir / 'xmatch'
                if not xdir.exists() or any(xdir.glob('*_within5arcsec.csv')): continue
                cmd = ["python","-u","-m","vasco.cli_pipeline","step5-filter-within5","--workdir",str(tile_dir)]
            elif step == 'step6-summarize':
                if not (tile_dir / 'pass2.ldac').exists() or (tile_dir / 'RUN_SUMMARY.md').exists(): continue
                cmd = ["python","-u","-m","vasco.cli_pipeline","step6-summarize","--workdir",str(tile_dir),
                       "--export",args.export or 'csv',"--hist-col",args.hist_col or 'FWHM_IMAGE']
            else:
                continue
            log.info(f"[RUN] {step} -> {tile_dir.name}"); rc = run_and_stream(cmd)
            if rc != 0: log.warning(f"Step {step} failed for {tile_dir.name} (rc={rc}).")
            total_runs += 1
            if limit and total_runs >= limit:
                log.info(f"Reached limit={limit}. Stopping."); return 0
    log.info(f"Steps completed. Total step invocations: {total_runs}"); return 0


def cmd_download_from_tiles(args: argparse.Namespace) -> int:
    """Scan existing tile folders and invoke step1-download for each.
    --force overrides --only-missing (i.e., process all tiles and delete existing raw FITS+sidecars).
    Use --no-only-missing to process all tiles without deleting existing files.
    """
    root = Path(args.workdir_root or WORKDIR_ROOT)
    tiles = sorted([p for p in root.glob('tile-RA*-DEC*') if p.is_dir()])
    if not tiles:
        log.info("No tiles found under %s", root); return 0
    planned = len(tiles); attempted = 0; downloaded = 0
    for tile_dir in tiles:
        parsed = parse_ra_dec_from_tile(tile_dir.name)
        if not parsed:
            log.warning("Skipping folder with unexpected name: %s", tile_dir.name); continue
        ra, dec = parsed
        raw_dir = tile_dir / 'raw'; raw_dir.mkdir(parents=True, exist_ok=True)
        # decide whether to process
        if args.force:
            # force: remove existing FITS + header sidecars and proceed
            for f in list(raw_dir.glob('*.fits*')) + list(raw_dir.glob('*.fits.header.json')):
                try:
                    f.unlink()
                except Exception:
                    pass
            log.info("[CLEAN] Removed existing FITS+sidecars in %s", tile_dir.name)
        elif args.only_missing and has_raw_fits(tile_dir):
            log.info("[SKIP] %s — raw FITS present; --only-missing", tile_dir.name)
            continue
        # build command
        cmd = [
            "python","-u","-m","vasco.cli_pipeline","step1-download",
            "--ra",str(ra),"--dec",str(dec),
            "--size-arcmin",str(args.size_arcmin or TILE_SIZE_ARCMIN),
            "--survey",args.survey or SURVEY,
            "--pixel-scale-arcsec",str(args.pixel_scale_arcsec or PIXEL_SCALE),
            "--workdir",str(tile_dir),
        ]
        log.info(f"[RUN] step1-download -> {tile_dir.name} (RA={ra:.6f} Dec={dec:.6f})")
        attempted += 1
        rc = run_and_stream(cmd)
        if rc != 0:
            log.warning("Step1 failed for %s (rc=%s)", tile_dir.name, rc)
        if has_raw_fits(tile_dir):
            downloaded += 1
        if args.sleep_sec:
            time.sleep(float(args.sleep_sec))
        if args.limit and attempted >= int(args.limit):
            break
    log.info("[DONE] Tiles planned=%d, attempted=%d, downloaded=%d", planned, attempted, downloaded)
    return 0

# argparse

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description='VASCO runner: download loop, step sweeper, and batch re-download from tile folders')
    sub = ap.add_subparsers(dest='cmd')

    dl = sub.add_parser('download_loop', help='Continuously download tiles (step1) until interrupted')
    dl.add_argument('--sleep-sec', type=float, default=15)
    dl.add_argument('--size-arcmin', type=float, default=TILE_SIZE_ARCMIN)
    dl.add_argument('--survey', default=SURVEY)
    dl.add_argument('--pixel-scale-arcsec', dest='pixel_scale_arcsec', type=float, default=PIXEL_SCALE)
    dl.add_argument('--pixel-scale',       dest='pixel_scale_arcsec', type=float, default=PIXEL_SCALE)
    dl.set_defaults(func=cmd_download_loop)

    st = sub.add_parser('steps', help='Scan tiles and run requested steps where missing')
    st.add_argument('--steps', required=True)
    st.add_argument('--workdir-root', default=WORKDIR_ROOT)
    st.add_argument('--limit', type=int, default=0)
    st.add_argument('--size-arcmin', type=float, default=TILE_SIZE_ARCMIN)
    st.add_argument('--xmatch-backend', choices=['local','cds'], default='cds')
    st.add_argument('--xmatch-radius', type=float, default=5.0)
    st.add_argument('--cds-gaia-table', default=os.getenv('VASCO_CDS_GAIA_TABLE'))
    st.add_argument('--cds-ps1-table',  default=os.getenv('VASCO_CDS_PS1_TABLE'))
    st.add_argument('--export', choices=['none','csv','parquet','both'], default='csv')
    st.add_argument('--hist-col', default='FWHM_IMAGE')
    st.set_defaults(func=cmd_steps)

    bt = sub.add_parser('download_from_tiles', help='Re-download Step 1 for existing tile folders under --workdir-root')
    bt.add_argument('--workdir-root', default=WORKDIR_ROOT)
    only_missing_group = bt.add_mutually_exclusive_group()
    only_missing_group.add_argument('--only-missing', action='store_true', default=True, help='Skip tiles that already have raw/*.fits (default)')
    only_missing_group.add_argument('--no-only-missing', dest='only_missing', action='store_false', help='Process all tiles regardless of raw/*.fits presence')
    bt.add_argument('--force', action='store_true', help='Delete existing raw FITS+sidecars, then re-download')
    bt.add_argument('--size-arcmin', type=float, default=TILE_SIZE_ARCMIN)
    bt.add_argument('--survey', default=SURVEY)
    bt.add_argument('--pixel-scale-arcsec', dest='pixel_scale_arcsec', type=float, default=PIXEL_SCALE)
    bt.add_argument('--pixel-scale',       dest='pixel_scale_arcsec', type=float, default=PIXEL_SCALE)
    bt.add_argument('--sleep-sec', type=float, default=0)
    bt.add_argument('--limit', type=int, default=0)
    bt.set_defaults(func=cmd_download_from_tiles)

    args = ap.parse_args(argv)
    if hasattr(args, 'func'): return args.func(args)
    ap.print_help(); return 0

if __name__ == '__main__':
    raise SystemExit(main())
