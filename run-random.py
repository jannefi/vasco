#!/usr/bin/env python3
import os
import sys
import json
import random
import logging
import time
from pathlib import Path
from subprocess import Popen, PIPE

# --- CONFIGURABLE PARAMETERS ---
RA_MIN, RA_MAX = 0, 360
DEC_MIN, DEC_MAX = 0, 90
TILE_SIZE_ARCMIN = 30
TILE_RADIUS_ARCMIN = 15  # For hex tessellation
WORKDIR = "data/tiles"
PROCESSED_FILE = "data/processed_tiles.json"
LOG_FILE = "data/vasco_random_run.log"
SURVEY = "poss1-e"
PIXEL_SCALE = 1.7

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, mode="a"),
    ],
)
log = logging.getLogger("vasco_random_run")


# --- TILE GRID GENERATION ---
def generate_tile_grid():
    from vasco.downloader import tessellate_centers

    width = (RA_MAX - RA_MIN) * 60
    height = (DEC_MAX - DEC_MIN) * 60
    centers = tessellate_centers(
        center_ra=(RA_MAX + RA_MIN) / 2,
        center_dec=(DEC_MAX + DEC_MIN) / 2,
        width_arcmin=width,
        height_arcmin=height,
        tile_radius_arcmin=TILE_RADIUS_ARCMIN,
        overlap_arcmin=0,
    )
    # Normalize RA to [0, 360)
    centers = [((ra % 360), dec) for (ra, dec) in centers]
    return centers


# --- LOAD/STORE PROCESSED TILES ---
def load_processed_tiles():
    if Path(PROCESSED_FILE).exists():
        with open(PROCESSED_FILE, "r") as f:
            return set(tuple(x) for x in json.load(f))
    return set()


def save_processed_tiles(processed):
    with open(PROCESSED_FILE, "w") as f:
        json.dump([list(x) for x in processed], f)


# --- STREAMING SUBPROCESS ---
def run_and_stream(cmd):
    log.info(f"Running: {' '.join(cmd)}")
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE, text=True, bufsize=1)
    # Stream stdout
    for line in proc.stdout:
        sys.stdout.write(line)
        log.info(f"VASCO: {line.rstrip()}")
    # Stream stderr
    for line in proc.stderr:
        sys.stderr.write(line)
        log.warning(f"VASCO STDERR: {line.rstrip()}")
    proc.wait()
    return proc.returncode


# --- MAIN LOOP ---
def main():
    log.info("Starting VASCO random tile science run.")
    all_tiles = generate_tile_grid()
    processed = load_processed_tiles()
    log.info(
        f"Total tiles in grid: {len(all_tiles)}. Already processed: {len(processed)}."
    )

    try:
        while True:
            unprocessed = [tile for tile in all_tiles if tuple(tile) not in processed]
            if not unprocessed:
                log.info("All tiles processed! Exiting.")
                break

            tile = random.choice(unprocessed)
            ra, dec = tile
            log.info(f"Selected tile: RA={ra:.5f}, Dec={dec:.5f}")

            cmd = [
                "python",
                "-m",
                "vasco.cli_pipeline",
                "one2pass",
                "--ra",
                str(ra),
                "--dec",
                str(dec),
                "--size-arcmin",
                str(TILE_SIZE_ARCMIN),
                "--survey",
                SURVEY,
                "--pixel-scale-arcsec",
                str(PIXEL_SCALE),
                "--export",
                "csv",
                "--hist-col",
                "FWHM_IMAGE",
                "--xmatch-backend",
                "cds",
                "--xmatch-radius-arcsec",
                "5.0",
                "--workdir",
                str(tile_dir),
            ]
            try:
                rc = run_and_stream(cmd)
                if rc == 0:
                    processed.add(tuple(tile))
                    save_processed_tiles(processed)
                    log.info(
                        f"Tile processed and recorded. Total done: {len(processed)}"
                    )
                else:
                    log.error(
                        f"VASCO pipeline failed for tile RA={ra}, Dec={dec}. Skipping."
                    )
            except Exception as e:
                log.error(f"Exception running VASCO for tile RA={ra}, Dec={dec}: {e}")

            log.info("Sleeping 15 seconds before next tile...")
            time.sleep(15)

    except KeyboardInterrupt:
        log.info("Interrupted by user. Saving progress and exiting.")
        save_processed_tiles(processed)


if __name__ == "__main__":
    main()
