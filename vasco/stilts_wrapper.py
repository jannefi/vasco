#!/usr/bin/env python3
import subprocess
import logging
from typing import Optional

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")


def run_tskymatch(
    in1: str,
    ra1_col: str,
    dec1_col: str,
    in2: str,
    ra2_col: str,
    dec2_col: str,
    radius_arcsec: float,
    out: str,
    ofmt: str = "votable",
    ifmt1: Optional[str] = None,
    ifmt2: Optional[str] = None,
) -> None:
    error_deg = radius_arcsec / 3600.0
    cmd = [
        "stilts",
        "tskymatch",
        f"in1={in1}",
        f"ra1={ra1_col}",
        f"dec1={dec1_col}",
        f"in2={in2}",
        f"ra2={ra2_col}",
        f"dec2={dec2_col}",
        "join=1and2",
        "find=best",
        f"error={error_deg}",
        f"ofmt={ofmt}",
        f"out={out}",
    ]
    if ifmt1:
        cmd.insert(2, f"ifmt1={ifmt1}")
    if ifmt2:
        insert_at = 2 + (3 if ifmt1 else 1) + 3
        cmd.insert(insert_at, f"ifmt2={ifmt2}")

    logging.info("Running STILTS: %s", " ".join(cmd))
    subprocess.run(cmd, check=True)
