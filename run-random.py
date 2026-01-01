
#!/usr/bin/env python3
"""
VASCO runner — dual-layout aware (legacy flat OR RA/Dec sharded)

Modes:
  1) download_loop         – continuously run step1-download until interrupted
  2) steps                 – sweep tiles and run requested steps (2..6) where missing
  3) download_from_tiles   – scan existing tiles and re-run step1 per tile
  4) inspect               – one-off step1 download at given RA/Dec

Layout control (works in WSL with repo-root symlink ./data -> /mnt/d/vasco/data):
  * --tiles-root   base dir for datasets (default: env VASCO_TILES_ROOT or ./data)
  * --layout       auto | legacy | sharded (default: auto)
    - auto: if ./data/tiles_by_sky exists => sharded, else legacy
    - legacy:  ./data/tiles/<tileid>/
    - sharded: ./data/tiles_by_sky/ra_bin=RRR/dec_bin=SS/<tileid>/

This keeps old users unbroken while letting NTFS/USB users benefit from sharding.
"""

import os, sys, json, random, logging, time, argparse, math
from pathlib import Path
from subprocess import Popen, PIPE, STDOUT

# ---------- defaults & logging ----------
DEFAULT_TILES_BASE = os.environ.get("VASCO_TILES_ROOT", "./data")
DEFAULT_LAYOUT     = os.environ.get("VASCO_TILES_LAYOUT", "auto")  # auto|legacy|sharded

SURVEY = "dss1-red"
PIXEL_SCALE = 1.7
TILE_SIZE_ARCMIN = 30
LOG_FILE = "logs/run_random.log"
RA_MIN, RA_MAX = 0, 360
DEC_MIN, DEC_MAX = -90, 90

os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(LOG_FILE, mode='a')]
)
log = logging.getLogger("vasco_random_run")

# ensure subprocesses import the repo copy
REPO_ROOT = Path(__file__).resolve().parent
BASE_ENV = os.environ.copy()
BASE_ENV['PYTHONPATH'] = str(REPO_ROOT)

# ---------- tiles registry (unchanged) ----------
import csv
REGISTRY_PATH = Path("./data/metadata/tiles_registry.csv")
REGISTRY_FIELDS = (
    "tile_id", "ra_deg", "dec_deg", "survey", "size_arcmin", "pixel_scale_arcsec",
    "status", "downloaded_utc", "source", "notes"
)
REGISTRY_KEY_FIELDS = ("tile_id", "survey", "size_arcmin", "pixel_scale_arcsec")

def _ensure_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

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
        try: os.unlink(self.lock_path)
        except FileNotFoundError: pass

def append_row_to_registry(csv_path: Path, row: dict, fieldnames=REGISTRY_FIELDS) -> None:
    _ensure_dir(csv_path)
    with _FileLock(csv_path):
        new_file = (not csv_path.exists()) or (csv_path.stat().st_size == 0)
        with csv_path.open("a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            if new_file:
                w.writeheader()
            w.writerow(row)
            f.flush(); os.fsync(f.fileno())

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

# ---------- tile-id & layout helpers ----------
def tile_id_from_coords(ra_deg: float, dec_deg: float, nd: int = 3) -> str:
    return f"tile-RA{ra_deg:.{nd}f}-DEC{dec_deg:+.{nd}f}"

def _parse_ra_dec_from_tile(dirname: str):
    try:
        if not dirname.startswith('tile-RA') or '-DEC' not in dirname:
            return None
        ra_str = dirname[len('tile-RA'): dirname.index('-DEC')]
        dec_str = dirname[dirname.index('-DEC') + len('-DEC') :]
        return float(ra_str), float(dec_str)
    except Exception:
        return None

def _ra_bin_5(ra: float) -> int:
    return int(math.floor((ra % 360.0) / 5.0) * 5)

def _dec_bin_5(dec: float) -> int:
    d = max(-90.0, min(90.0, dec))
    return int(math.floor(d / 5.0) * 5)

def _format_dec_bin(b: int) -> str:
    return f"{'+' if b >= 0 else '-'}{abs(b):02d}"

def compute_workdir_for_tile(base_dir: Path, layout: str, ra: float, dec: float, tile_id: str) -> Path:
    """
    base_dir: path to ./data (symlink to /mnt/d/vasco/data in your WSL setup)
    layout:   'auto'|'legacy'|'sharded'
    """
    base = Path(base_dir)
    if layout == "auto":
        layout = "sharded" if (base / "tiles_by_sky").exists() else "legacy"
    if layout == "legacy":
        return base / "tiles" / tile_id
    # sharded
    rb = _ra_bin_5(ra)
    db = _dec_bin_5(dec)
    return base / "tiles_by_sky" / f"ra_bin={rb:03d}" / f"dec_bin={_format_dec_bin(db)}" / tile_id

# ---------- subprocess runner ----------
def run_and_stream(cmd: list[str]) -> int:
    log.info("Running: %s", ' '.join(cmd))
    proc = Popen(cmd, stdout=PIPE, stderr=STDOUT, text=True, bufsize=1, env=BASE_ENV)
    for line in proc.stdout:
        sys.stdout.write(line)
    proc.wait()
    return proc.returncode

def has_raw_fits(tile_dir: Path) -> bool:
    return any((tile_dir / 'raw').glob('*.fits'))

# ---------- steps logic ----------
def _needs_step5(xdir: Path) -> bool:
    sources = sorted(list(xdir.glob('sex_*_xmatch.csv')) + list(xdir.glob('sex_*_xmatch_cdss.csv')))
    if not sources:
        return True
    for src in sources:
        dst = src.with_name(src.stem + '_within5arcsec.csv')
        if not dst.exists():
            return True
    return False

# ---------- Tiles enumeration (adapter) ----------
# We use the adapter you added earlier so scripts see both layouts transparently.
try:
    from vasco.io.tiles_root_adapter import TilesAdapter
except Exception:
    TilesAdapter = None  # will fallback to legacy-only scan if adapter is missing

def _iter_all_tiles(tiles_base: Path) -> list[Path]:
    """
    Return a list of tile directories across both layouts under tiles_base (./data).
    Requires TilesAdapter; if missing, falls back to legacy ./data/tiles only.
    """
    tiles_base = Path(tiles_base)
    out: list[Path] = []
    if TilesAdapter is not None:
        ta = TilesAdapter(base_dir=str(tiles_base))
        out = [Path(t.path) for t in ta.iter_tiles()]
    else:
        legacy = tiles_base / "tiles"
        if legacy.exists():
            out = sorted([p for p in legacy.glob('tile-RA*-DEC*') if p.is_dir()], key=lambda p: p.name)
    return out

# ---------- commands ----------
def cmd_download_loop(args: argparse.Namespace) -> int:
    log.info("Starting download loop — CTRL+C to stop.")
    tiles_base = Path(args.tiles_root or DEFAULT_TILES_BASE)
    layout = (args.layout or DEFAULT_LAYOUT).strip().lower()
    seen = load_seen_from_registry(REGISTRY_PATH)
    planned = 0; errors = 0
    limit = int(args.limit or 0)
    max_errors = int(args.max_errors or 0)

    try:
        while True:
            if limit and planned >= limit:
                log.info("Reached limit=%s. Stopping.", limit); return 0
            if max_errors and errors >= max_errors:
                log.warning("Reached max_errors=%s. Stopping.", max_errors); return 1

            ra = random.uniform(RA_MIN, RA_MAX)
            dec = random.uniform(DEC_MIN, DEC_MAX)
            tid = tile_id_from_coords(ra, dec)
            size = float(args.size_arcmin or TILE_SIZE_ARCMIN)
            px   = float(args.pixel_scale_arcsec or PIXEL_SCALE)
            survey = args.survey or SURVEY
            key = (tid, survey, str(size), str(px))

            workdir_tile = compute_workdir_for_tile(tiles_base, layout, ra, dec, tid)

            if key in seen:
                log.info("[SKIP] %s — seen in registry for %s/%s/%s", tid, survey, size, px)
                time.sleep(float(args.sleep_sec or 15)); continue

            cmd = [
                "python","-u","-m","vasco.cli_pipeline","step1-download",
                "--ra", str(ra), "--dec", str(dec),
                "--size-arcmin", str(size),
                "--survey", survey,
                "--pixel-scale-arcsec", str(px),
                "--workdir", str(workdir_tile),
            ]
            rc = run_and_stream(cmd)
            if rc != 0:
                errors += 1
                log.warning("Download failed for %s (rc=%s). errors=%s", tid, rc, errors)
            else:
                if has_raw_fits(workdir_tile):
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
                        log.info("[LEDGER] appended %s to tiles_registry.csv", tid)
                    except Exception as e:
                        log.warning("[LEDGER] append failed for %s: %s", tid, e)
                planned += 1
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

    tiles_base = Path(args.tiles_root or DEFAULT_TILES_BASE)
    tiles = _iter_all_tiles(tiles_base)
    if not tiles:
        log.info("No tiles found under %s (legacy or sharded). Run download_loop first.", tiles_base); return 0

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
                xdir = tile_dir / 'xmatch'
                has_any = (xdir.exists() and (any(xdir.glob('sex_*_xmatch.csv')) or any(xdir.glob('sex_*_xmatch_cdss.csv'))))
                if not (tile_dir / 'pass2.ldac').exists() or has_any: continue
                cmd = [
                    "python","-u","-m","vasco.cli_pipeline","step4-xmatch","--workdir",str(tile_dir),
                    "--xmatch-backend", args.xmatch_backend or 'cds',
                    "--xmatch-radius-arcsec", str(args.xmatch_radius or 5.0),
                    "--size-arcmin", str(args.size_arcmin or TILE_SIZE_ARCMIN),
                ]
                if (args.xmatch_backend or 'cds') == 'cds':
                    if args.cds_gaia_table: cmd += ["--cds-gaia-table", args.cds_gaia_table]
                    if args.cds_ps1_table: cmd += ["--cds-ps1-table",  args.cds_ps1_table]
            elif step == 'step5-filter-within5':
                xdir = tile_dir / 'xmatch'
                if not xdir.exists() or not _needs_step5(xdir):
                    continue
                cmd = ["python","-u","-m","vasco.cli_pipeline","step5-filter-within5","--workdir",str(tile_dir)]
            elif step == 'step6-summarize':
                if not (tile_dir / 'pass2.ldac').exists() or (tile_dir / 'RUN_SUMMARY.md').exists(): continue
                cmd = [
                    "python","-u","-m","vasco.cli_pipeline","step6-summarize","--workdir",str(tile_dir),
                    "--export", args.export or 'csv', "--hist-col", args.hist_col or 'FWHM_IMAGE'
                ]
            else:
                continue

            log.info("[RUN] %s -> %s", step, tile_dir.name)
            rc = run_and_stream(cmd)
            if rc != 0:
                log.warning("Step %s failed for %s (rc=%s).", step, tile_dir.name, rc)
            total_runs += 1
            if limit and total_runs >= limit:
                log.info("Reached limit=%s. Stopping.", limit); return 0

    log.info("Steps completed. Total step invocations: %s", total_runs); return 0

def cmd_download_from_tiles(args: argparse.Namespace) -> int:
    """
    Scan existing tiles from BOTH layouts under --tiles-root and re-run Step 1 per tile.
    """
    tiles_base = Path(args.tiles_root or DEFAULT_TILES_BASE)
    tiles = _iter_all_tiles(tiles_base)
    if not tiles:
        log.info("No tiles found under %s", tiles_base); return 0

    planned = len(tiles); attempted = 0; downloaded = 0
    for tile_dir in tiles:
        parsed = _parse_ra_dec_from_tile(tile_dir.name)
        if not parsed:
            log.warning("Skipping folder with unexpected name: %s", tile_dir.name); continue
        ra, dec = parsed
        raw_dir = tile_dir / 'raw'
        # policy
        if args.force:
            for f in list(raw_dir.glob('*.fits*')) + list(raw_dir.glob('*.fits.header.json')):
                try: f.unlink()
                except Exception: pass
            log.info("[CLEAN] Removed existing FITS+sidecars in %s", tile_dir.name)
        elif args.only_missing and has_raw_fits(tile_dir):
            log.info("[SKIP] %s — raw FITS present; --only-missing", tile_dir.name)
            continue

        cmd = [
            "python","-u","-m","vasco.cli_pipeline","step1-download",
            "--ra",str(ra),"--dec",str(dec),
            "--size-arcmin",str(args.size_arcmin or TILE_SIZE_ARCMIN),
            "--survey",args.survey or SURVEY,
            "--pixel-scale-arcsec",str(args.pixel_scale_arcsec or PIXEL_SCALE),
            "--workdir",str(tile_dir),
        ]
        log.info("[RUN] step1-download -> %s (RA=%.6f Dec=%.6f)", tile_dir.name, ra, dec)
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

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description='VASCO runner (dual-layout; staging-aware)')
    sub = ap.add_subparsers(dest='cmd')

    # shared knobs
    def add_shared(p):
        p.add_argument('--tiles-root', default=DEFAULT_TILES_BASE,
                       help='Base data directory (default: env VASCO_TILES_ROOT or ./data)')
        p.add_argument('--layout', choices=['auto','legacy','sharded'], default=DEFAULT_LAYOUT,
                       help='Tile layout to target when creating new tiles (default: auto)')

    dl = sub.add_parser('download_loop', help='Continuously download tiles (step1) until interrupted')
    add_shared(dl)
    dl.add_argument('--sleep-sec', type=float, default=15)
    dl.add_argument('--size-arcmin', type=float, default=TILE_SIZE_ARCMIN)
    dl.add_argument('--survey', default=SURVEY)
    dl.add_argument('--pixel-scale-arcsec', dest='pixel_scale_arcsec', type=float, default=PIXEL_SCALE)
    dl.add_argument('--pixel-scale', dest='pixel_scale_arcsec', type=float, default=PIXEL_SCALE)
    dl.add_argument('--limit', type=int, default=0, help='Max successful downloads before stop (0 = unlimited)')
    dl.add_argument('--max-errors', type=int, default=0, help='Stop after this many step1 failures (0 = unlimited)')
    dl.set_defaults(func=cmd_download_loop)

    st = sub.add_parser('steps', help='Scan tiles (both layouts) and run requested steps where missing')
    add_shared(st)
    st.add_argument('--steps', required=True)
    st.add_argument('--limit', type=int, default=0)
    st.add_argument('--size-arcmin', type=float, default=TILE_SIZE_ARCMIN)
    st.add_argument('--xmatch-backend', choices=['local','cds'], default='cds')
    st.add_argument('--xmatch-radius', type=float, default=5.0)
    st.add_argument('--cds-gaia-table', default=os.getenv('VASCO_CDS_GAIA_TABLE'))
    st.add_argument('--cds-ps1-table', default=os.getenv('VASCO_CDS_PS1_TABLE'))
    st.add_argument('--export', choices=['none','csv','parquet','both'], default='csv')
    st.add_argument('--hist-col', default='FWHM_IMAGE')
    st.set_defaults(func=cmd_steps)

    bt = sub.add_parser('download_from_tiles', help='Re-download Step 1 for existing tiles (both layouts)')
    add_shared(bt)
    group = bt.add_mutually_exclusive_group()
    group.add_argument('--only-missing', action='store_true', default=True, help='Skip tiles that already have raw/*.fits (default)')
    group.add_argument('--no-only-missing', dest='only_missing', action='store_false', help='Process all tiles regardless of raw/*.fits presence')
    bt.add_argument('--force', action='store_true', help='Delete existing raw FITS+sidecars, then re-download')
    bt.add_argument('--size-arcmin', type=float, default=TILE_SIZE_ARCMIN)
    bt.add_argument('--survey', default=SURVEY)
    bt.add_argument('--pixel-scale-arcsec', dest='pixel_scale_arcsec', type=float, default=PIXEL_SCALE)
    bt.add_argument('--pixel-scale', dest='pixel_scale_arcsec', type=float, default=PIXEL_SCALE)
    bt.add_argument('--sleep-sec', type=float, default=0)
    bt.add_argument('--limit', type=int, default=0)
    bt.set_defaults(func=cmd_download_from_tiles)

    ins = sub.add_parser('inspect', help='One-off Step 1 download at given RA/Dec')
    add_shared(ins)
    ins.add_argument('--ra', required=True, type=float)
    ins.add_argument('--dec', required=True, type=float)
    ins.add_argument('--size-arcmin', type=float, default=10.0)  # inspection
    ins.add_argument('--survey', default=SURVEY)                  # dss1-red
    ins.add_argument('--pixel-scale-arcsec', dest='pixel_scale_arcsec', type=float, default=PIXEL_SCALE)
    def _run_inspect(a):
        ra, dec = float(a.ra), float(a.dec)
        tid = tile_id_from_coords(ra, dec)
        tiles_base = Path(a.tiles_root or DEFAULT_TILES_BASE)
        workdir = compute_workdir_for_tile(tiles_base, a.layout, ra, dec, tid)
        return run_and_stream([
            "python","-u","-m","vasco.cli_pipeline","step1-download",
            "--ra", str(ra), "--dec", str(dec),
            "--size-arcmin", str(a.size_arcmin),
            "--survey", a.survey,
            "--pixel-scale-arcsec", str(a.pixel_scale_arcsec),
            "--workdir", str(workdir),
        ])
    ins.set_defaults(func=_run_inspect)

    args = ap.parse_args(argv)
    if hasattr(args, 'func'):
        return args.func(args)
    ap.print_help(); return 0

if __name__ == '__main__':
    raise SystemExit(main())
