
#!/usr/bin/env python3
"""
VASCO runner — modes:
 1) download_loop — continuously run step1-download until interrupted
 2) steps — sweep tiles and run requested steps (2..6) where missing
 3) download_from_tiles — scan existing tile folders under data/tiles and invoke step1-download per tile

This version avoids creating tile folders during step1 failures by relying on the downloader's
repo-local staging and late promotion semantics.
"""
import os, sys, json, random, logging, time, argparse
from pathlib import Path
from subprocess import Popen, PIPE, STDOUT

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

# --- tiles registry helpers ---
import csv
REGISTRY_PATH = Path("./data/metadata/tiles_registry.csv")
REGISTRY_FIELDS = (
    "tile_id", "ra_deg", "dec_deg", "survey", "size_arcmin", "pixel_scale_arcsec",
    "status", "downloaded_utc", "source", "notes"
)
REGISTRY_KEY_FIELDS = ("tile_id", "survey", "size_arcmin", "pixel_scale_arcsec")


def _needs_step5(xdir: Path) -> bool:
    # Source patterns match the CLI (see cli_pipeline.py)
    sources = sorted(list(xdir.glob('sex_*_xmatch.csv')) + list(xdir.glob('sex_*_xmatch_cdss.csv')))
    if not sources:
        # returning False will skip quietly. Choose one behavior:
        return True  # or False if you want to skip silently when there are no sources
    for src in sources:
        dst = src.with_name(src.stem + '_within5arcsec.csv')
        if not dst.exists():
            return True
    return False


def _ensure_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

def load_seen_from_registry(csv_path: Path, key_fields=REGISTRY_KEY_FIELDS) -> set[tuple]:
    seen = set()
    if csv_path.exists() and csv_path.stat().st_size > 0:
        with csv_path.open("r", newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            has_all = all(k in r.fieldnames for k in key_fields)
            if has_all:
                for row in r:
                    seen.add(tuple(row[k] for k in key_fields))
    return seen

class _FileLock:
    def __init__(self, target_csv: Path, timeout_ms: int = 5000, poll_ms: int = 50):
        self.lock_path = target_csv.with_suffix(target_csv.suffix + ".lock")
        self.timeout_ms = timeout_ms
        self.poll_ms = poll_ms
    def __enter__(self):
        _ensure_dir(self.lock_path)
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
        try:
            os.unlink(self.lock_path)
        except FileNotFoundError:
            pass

def append_row_to_registry(
    csv_path: Path,
    row: dict,
    fieldnames: tuple[str, ...] = REGISTRY_FIELDS,
) -> None:
    _ensure_dir(csv_path)
    with _FileLock(csv_path):
        new_file = (not csv_path.exists()) or (csv_path.stat().st_size == 0)
        with csv_path.open("a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            if new_file:
                w.writeheader()
            w.writerow(row)
            f.flush(); os.fsync(f.fileno())

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
    proc = Popen(cmd, stdout=PIPE, stderr=STDOUT, text=True, bufsize=1, env=BASE_ENV)
    for line in proc.stdout:
        sys.stdout.write(line)
    proc.wait()
    return proc.returncode


def has_raw_fits(tile_dir: Path) -> bool:
    return any((tile_dir/'raw').glob('*.fits'))

# commands

def cmd_download_loop(args: argparse.Namespace) -> int:
    log.info("Starting download loop — CTRL+C to stop.")
    seen = load_seen_from_registry(REGISTRY_PATH)
    try:
        while True:
            ra = random.uniform(RA_MIN, RA_MAX)
            dec = random.uniform(DEC_MIN, DEC_MAX)
            tid = tile_id_from_coords(ra, dec)
            size = float(args.size_arcmin or TILE_SIZE_ARCMIN)
            px = float(args.pixel_scale_arcsec or PIXEL_SCALE)
            survey = args.survey or SURVEY
            key = (tid, survey, str(size), str(px))
            workdir_tile = os.path.join(WORKDIR_ROOT, tid)

            # EARLY SKIP using registry
            if key in seen:
                log.info(f"[SKIP] {tid} — seen in registry for {survey}/{size}/{px}")
                time.sleep(float(args.sleep_sec or 15))
                continue

            # Step 1 command build (no upfront mkdir)
            cmd = [
                "python","-u","-m","vasco.cli_pipeline","step1-download",
                "--ra", str(ra), "--dec", str(dec),
                "--size-arcmin", str(size),
                "--survey", survey,
                "--pixel-scale-arcsec", str(px),
                "--workdir", workdir_tile,
            ]
            rc = run_and_stream(cmd)
            if rc != 0:
                log.warning(f"Download failed for {tid} (rc={rc}).")
            else:
                # confirm we got a FITS; only then record registry
                if has_raw_fits(Path(workdir_tile)):
                    from datetime import datetime, timezone
                    row = {
                        "tile_id": tid,
                        "ra_deg": f"{ra:.6f}",
                        "dec_deg": f"{dec:.6f}",
                        "survey": survey,
                        "size_arcmin": str(size),
                        "pixel_scale_arcsec": f"{px:.2f}",
                        "status": "ok",
                        "downloaded_utc": datetime.now(timezone.utc).isoformat(),
                        "source": "download_loop",
                        "notes": "",
                    }
                    try:
                        append_row_to_registry(REGISTRY_PATH, row)
                        seen.add(key)
                        log.info(f"[LEDGER] appended {tid} to tiles_registry.csv")
                    except Exception as e:
                        log.warning(f"[LEDGER] append failed for {tid}: {e}")
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
                if not (tile_dir / 'pass2.ldac').exists() or ((tile_dir / 'xmatch').exists() and (any((tile_dir / 'xmatch').glob('sex_*_xmatch.csv')) or any((tile_dir / 'xmatch').glob('sex_*_xmatch_cdss.csv')))): continue
                cmd = ["python","-u","-m","vasco.cli_pipeline","step4-xmatch","--workdir",str(tile_dir),
                       "--xmatch-backend",args.xmatch_backend or 'cds',
                       "--xmatch-radius-arcsec",str(args.xmatch_radius or 5.0),
                       "--size-arcmin",str(args.size_arcmin or TILE_SIZE_ARCMIN)]
                if (args.xmatch_backend or 'cds') == 'cds':
                    if args.cds_gaia_table: cmd += ["--cds-gaia-table", args.cds_gaia_table]
                    if args.cds_ps1_table: cmd += ["--cds-ps1-table", args.cds_ps1_table]
            elif step == 'step5-filter-within5':
                xdir = tile_dir / 'xmatch'
                if not xdir.exists() or not _needs_step5(xdir):
                    continue
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
        raw_dir = tile_dir / 'raw'
        # decide whether to process
        if args.force:
            for f in list(raw_dir.glob('*.fits*')) + list(raw_dir.glob('*.fits.header.json')):
                try: f.unlink()
                except Exception: pass
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
    ap = argparse.ArgumentParser(description='VASCO runner: download loop, step sweeper, and batch re-download from tile folders (staging-aware)')
    sub = ap.add_subparsers(dest='cmd')

    dl = sub.add_parser('download_loop', help='Continuously download tiles (step1) until interrupted')
    dl.add_argument('--sleep-sec', type=float, default=15)
    dl.add_argument('--size-arcmin', type=float, default=TILE_SIZE_ARCMIN)
    dl.add_argument('--survey', default=SURVEY)
    dl.add_argument('--pixel-scale-arcsec', dest='pixel_scale_arcsec', type=float, default=PIXEL_SCALE)
    dl.add_argument('--pixel-scale', dest='pixel_scale_arcsec', type=float, default=PIXEL_SCALE)
    dl.set_defaults(func=cmd_download_loop)

    st = sub.add_parser('steps', help='Scan tiles and run requested steps where missing')
    st.add_argument('--steps', required=True)
    st.add_argument('--workdir-root', default=WORKDIR_ROOT)
    st.add_argument('--limit', type=int, default=0)
    st.add_argument('--size-arcmin', type=float, default=TILE_SIZE_ARCMIN)
    st.add_argument('--xmatch-backend', choices=['local','cds'], default='cds')
    st.add_argument('--xmatch-radius', type=float, default=5.0)
    st.add_argument('--cds-gaia-table', default=os.getenv('VASCO_CDS_GAIA_TABLE'))
    st.add_argument('--cds-ps1-table', default=os.getenv('VASCO_CDS_PS1_TABLE'))
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
    bt.add_argument('--pixel-scale', dest='pixel_scale_arcsec', type=float, default=PIXEL_SCALE)
    bt.add_argument('--sleep-sec', type=float, default=0)
    bt.add_argument('--limit', type=int, default=0)
    bt.set_defaults(func=cmd_download_from_tiles)

    ins = sub.add_parser('inspect', help='One-off Step 1 download at given RA/Dec')
    ins.add_argument('--ra', required=True, type=float)
    ins.add_argument('--dec', required=True, type=float)
    ins.add_argument('--workdir-root', default=WORKDIR_ROOT)
    ins.add_argument('--size-arcmin', type=float, default=10.0) # default 10′ for inspection
    ins.add_argument('--survey', default=SURVEY) # dss1-red
    ins.add_argument('--pixel-scale-arcsec', dest='pixel_scale_arcsec', type=float, default=PIXEL_SCALE)
    ins.add_argument('--pixel-scale', dest='pixel_scale_arcsec', type=float, default=PIXEL_SCALE)
    ins.set_defaults(func=lambda a: run_and_stream([
        "python","-u","-m","vasco.cli_pipeline","step1-download",
        "--ra", str(a.ra), "--dec", str(a.dec),
        "--size-arcmin", str(a.size_arcmin),
        "--survey", a.survey,
        "--pixel-scale-arcsec", str(a.pixel_scale_arcsec),
        "--workdir", str(Path(a.workdir_root) / tile_id_from_coords(a.ra, a.dec))
    ]))

    args = ap.parse_args(argv)
    if hasattr(args, 'func'): return args.func(args)
    ap.print_help(); return 0

if __name__ == '__main__':
    raise SystemExit(main())
