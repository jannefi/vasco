#!/usr/bin/env python3
from pathlib import Path
import csv

DATA_ROOT = Path("./data")
OUTPUT_CSV = DATA_ROOT / "tile_status.csv"

def iter_tile_dirs_any(data_root: Path):
    tiles_flat = data_root / "tiles"
    tiles_sharded = data_root / "tiles_by_sky"
    if tiles_flat.exists():
        for p in sorted(tiles_flat.glob("tile-*")):
            if p.is_dir():
                yield p
    if tiles_sharded.exists():
        for p in sorted(tiles_sharded.glob("ra_bin=*/dec_bin=*/tile-*")):
            if p.is_dir():
                yield p

# ---- stage definitions ----
def stage_1_raw_fits(tile: Path):
    raw_dir = tile / "raw"
    if not raw_dir.exists():
        return False, []
    matches = list(raw_dir.glob("*.fits")) + list(raw_dir.glob("*.fit")) + list(raw_dir.glob("*.fits.fz")) + list(raw_dir.glob("*.fit.fz")) + list(raw_dir.glob("*.fz"))
    return bool(matches), []

def stage_2_pass1(tile: Path): return (tile / "pass1.ldac").exists(), []

def stage_3_psf_and_pass2(tile: Path):
    missing=[]
    if not (tile/"pass1.psf").exists(): missing.append("pass1.psf")
    if not (tile/"pass2.ldac").exists(): missing.append("pass2.ldac")
    return not missing, missing

def is_expected_missing(tile: Path) -> bool:
    debug_log = tile / "xmatch" / "STEP4_CDS.log"
    if debug_log.exists():
        log_text = debug_log.read_text()
        return ("outside survey coverage" in log_text or "xmatch failed" in log_text or "skipped" in log_text)
    return False

def add_consistency_warnings(tile: Path, completed: dict[int,bool], warnings: list[str]) -> None:
    present_later = [s for s, ok in completed.items() if ok]
    for later in present_later:
        earlier_missing = [e for e in range(1, later) if not completed.get(e, False)]
        if not earlier_missing: continue
        if (later in (6,7) and any(e in (4,5) for e in earlier_missing) and is_expected_missing(tile)):
            warnings.append(f"stage {later} present while earlier stages {earlier_missing} are missing (expected: outside survey coverage)")
        else:
            warnings.append(f"stage {later} present while earlier stages {earlier_missing} are missing")

def stage_4_xmatch(tile: Path):
    gaia = tile / "xmatch" / "sex_gaia_xmatch_cdss.csv"
    ps1  = tile / "xmatch" / "sex_ps1_xmatch_cdss.csv"
    exists = gaia.exists() or ps1.exists()
    warnings=[]
    if exists and not (gaia.exists() and ps1.exists()):
        warnings.append("partial xmatch (one of gaia/ps1 missing)")
    return exists, warnings

def stage_5_xmatch_within5(tile: Path):
    gaia = tile / "xmatch" / "sex_gaia_xmatch_cdss_within5arcsec.csv"
    ps1  = tile / "xmatch" / "sex_ps1_xmatch_cdss_within5arcsec.csv"
    return (gaia.exists() or ps1.exists()), []

def stage_6_final_catalog(tile: Path): return (tile / "final_catalog.csv").exists(), []

def stage_7_post_processing(tile: Path): return (tile / "final_catalog_wcsfix.csv").exists(), []

STAGES = [
    (1, "raw_fits", stage_1_raw_fits),
    (2, "sextractor_pass1", stage_2_pass1),
    (3, "psf_and_pass2", stage_3_psf_and_pass2),
    (4, "xmatch", stage_4_xmatch),
    (5, "xmatch_within5arcsec", stage_5_xmatch_within5),
    (6, "final_catalog", stage_6_final_catalog),
    (7, "post_processing", stage_7_post_processing),
]

def main():
    rows=[]
    for tile in iter_tile_dirs_any(DATA_ROOT):
        completed={}
        warnings=[]
        for stage_num, stage_name, checker in STAGES:
            ok, stage_warnings = checker(tile)
            completed[stage_num]=ok
            for w in stage_warnings:
                warnings.append(f"stage {stage_num}: {w}")
        done = [s for s,ok in completed.items() if ok]
        if done:
            highest = max(done)
            stage_name = next(name for num,name,_ in STAGES if num==highest)
        else:
            highest=-1; stage_name="none"; warnings.append("no known stage outputs found")
        add_consistency_warnings(tile, completed, warnings)
        rows.append({
            "tile_id": tile.name,
            "stage": highest,
            "stage_name": stage_name,
            "warning": "; ".join(sorted(set(warnings)))
        })
    with OUTPUT_CSV.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["tile_id","stage","stage_name","warning"])
        writer.writeheader()
        writer.writerows(rows)

if __name__ == "__main__":
    main()
