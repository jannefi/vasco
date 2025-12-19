#!/usr/bin/env python3

from pathlib import Path
import csv

TILES_ROOT = Path("./data/tiles")
DATA_ROOT = Path("./data")
OUTPUT_CSV = DATA_ROOT / "tile_status.csv"

# ----------------------------------------------------------------------
# Stage definitions
# Each stage returns (completed: bool, warning_messages: list[str])
# ----------------------------------------------------------------------

def stage_1_raw_fits(tile: Path):
    raw_dir = tile / "raw"
    fits_files = list(raw_dir.glob("dss1-red_*.fits")) if raw_dir.exists() else []
    return bool(fits_files), []


def stage_2_pass1(tile: Path):
    return (tile / "pass1.ldac").exists(), []


def stage_3_psf_and_pass2(tile: Path):
    missing = []
    if not (tile / "pass1.psf").exists():
        missing.append("pass1.psf")
    if not (tile / "pass2.ldac").exists():
        missing.append("pass2.ldac")
    return not missing, missing


def stage_4_xmatch(tile: Path):
    gaia = tile / "xmatch" / "sex_gaia_xmatch_cdss.csv"
    ps1  = tile / "xmatch" / "sex_ps1_xmatch_cdss.csv"

    exists = gaia.exists() or ps1.exists()
    warnings = []
    if exists and not (gaia.exists() and ps1.exists()):
        warnings.append("partial xmatch (one of gaia/ps1 missing)")
    return exists, warnings


def stage_5_xmatch_within5(tile: Path):
    gaia = tile / "xmatch" / "sex_gaia_xmatch_cdss_within5arcsec.csv"
    ps1  = tile / "xmatch" / "sex_ps1_xmatch_cdss_within5arcsec.csv"
    return (gaia.exists() or ps1.exists()), []


def stage_6_final_catalog(tile: Path):
    return (tile / "final_catalog.csv").exists(), []


def stage_7_post_processing(tile: Path):
    return (tile / "final_catalog_wcsfix.csv").exists(), []


STAGES = [
    (1, "raw_fits", stage_1_raw_fits),
    (2, "sextractor_pass1", stage_2_pass1),
    (3, "psf_and_pass2", stage_3_psf_and_pass2),
    (4, "xmatch", stage_4_xmatch),
    (5, "xmatch_within5arcsec", stage_5_xmatch_within5),
    (6, "final_catalog", stage_6_final_catalog),
    (7, "post_processing", stage_7_post_processing),
]

# ----------------------------------------------------------------------

def main():
    rows = []

    for tile in sorted(p for p in TILES_ROOT.iterdir() if p.is_dir() and p.name.startswith("tile-")):
        completed = {}
        warnings = []

        for stage_num, stage_name, checker in STAGES:
            ok, stage_warnings = checker(tile)
            completed[stage_num] = ok
            for w in stage_warnings:
                warnings.append(f"stage {stage_num}: {w}")

        # Determine highest completed stage
        completed_stages = [s for s, ok in completed.items() if ok]
        if completed_stages:
            highest = max(completed_stages)
            stage_name = next(name for num, name, _ in STAGES if num == highest)
        else:
            highest = -1
            stage_name = "none"
            warnings.append("no known stage outputs found")

        # Consistency warnings: later stage exists but earlier missing
        for stage_num, _, _ in STAGES:
            if completed.get(stage_num):
                for earlier in range(1, stage_num):
                    if not completed.get(earlier, False):
                        warnings.append(
                            f"stage {stage_num} present but stage {earlier} missing"
                        )

        rows.append({
            "tile_id": tile.name,
            "stage": highest,
            "stage_name": stage_name,
            "warning": "; ".join(sorted(set(warnings)))
        })

    # Write CSV
    with OUTPUT_CSV.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["tile_id", "stage", "stage_name", "warning"]
        )
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
