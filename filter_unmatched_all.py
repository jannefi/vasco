
import pandas as pd
from pathlib import Path
import sys
import subprocess

RUN_ROOT = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data/runs")

def detect_cols(cols, candidates):
    for pair in candidates:
        if set(pair).issubset(cols):
            return pair
    return None


def detect_radec_columns(csv_path, candidates):
    import csv
    with open(csv_path, newline='') as f:
        header = next(csv.reader(f))
        cols = set(header)
        for ra, dec in candidates:
            if ra in cols and dec in cols:
                return ra, dec
    return None, None


def filter_unmatched(xmatch_csv, candidates, out_name):
    if not xmatch_csv.exists():
        return
    df = pd.read_csv(xmatch_csv)
    pair = detect_cols(df.columns, candidates)
    if not pair:
        print("[WARN] RA/Dec columns not found in:", xmatch_csv)
        return
    ra, de = pair
    # NOTE: with join=1and2 xmatch files contain matched rows only.
    # The following will usually be empty unless your xmatch file includes unmatched rows.
    unmatched = df[df[ra].isna() | df[de].isna()]
    out = xmatch_csv.parent / out_name
    unmatched.to_csv(out, index=False)
    print("[INFO] Unmatched ->", out)

def make_unmatched_with_stilts(sex_csv, neigh_csv, out_csv, ra1, dec1, ra2, dec2, radius_arcsec=2.0):
    if not (sex_csv.exists() and neigh_csv.exists()):
        return
    cmd = [
        "stilts", "tskymatch2",
        f"in1={str(sex_csv)}", f"in2={str(neigh_csv)}",
        f"ra1={ra1}", f"dec1={dec1}", f"ra2={ra2}", f"dec2={dec2}",
        f"error={radius_arcsec}", "join=1not2",
        f"out={str(out_csv)}", "ofmt=csv"
    ]
    try:
        subprocess.run(cmd, check=True)
        print("[INFO] STILTS unmatched ->", out_csv)
    except Exception as e:
        print("[WARN] STILTS unmatched failed:", e)

# Gaia

for xmatch in RUN_ROOT.glob("run-*/tiles/*/xmatch/sex_gaia_xmatch.csv"):
    # Optional: pure filter (likely empty for join=1and2)
    filter_unmatched(
        xmatch,
        candidates=[("RA_ICRS","DE_ICRS"), ("RAJ2000","DEJ2000"), ("ra","dec"), ("RA","DEC")],
        out_name="sex_gaia_unmatched.csv"
    )
    # Recommended: build unmatched explicitly via STILTS
    tile_dir = xmatch.parent.parent
    sex_csv = tile_dir / "catalogs" / "sextractor_pass2.csv"
    gaia_csv = tile_dir / "catalogs" / "gaia_neighbourhood.csv"
    out_csv = xmatch.parent / "sex_gaia_unmatched.csv"
    # Detect actual RA/Dec columns in Gaia CSV
    ra2, dec2 = detect_radec_columns(gaia_csv, [("RA_ICRS","DE_ICRS"), ("RAJ2000","DEJ2000"), ("ra","dec"), ("RA","DEC")])
    if ra2 and dec2:
        make_unmatched_with_stilts(sex_csv, gaia_csv, out_csv,
                                   ra1="ALPHA_J2000", dec1="DELTA_J2000",
                                   ra2=ra2, dec2=dec2)
    else:
        print(f"[WARN] Could not find RA/Dec columns in {gaia_csv}; skipping STILTS unmatched.")

# PS1
for xmatch in RUN_ROOT.glob("run-*/tiles/*/xmatch/sex_ps1_xmatch.csv"):
    filter_unmatched(
        xmatch,
        candidates=[("raMean","decMean"), ("RAMean","DecMean"), ("ra","dec"), ("RA","DEC")],
        out_name="sex_ps1_unmatched.csv"
    )
    tile_dir = xmatch.parent.parent
    sex_csv = tile_dir / "catalogs" / "sextractor_pass2.csv"
    ps1_csv = tile_dir / "catalogs" / "ps1_neighbourhood.csv"
    out_csv = xmatch.parent / "sex_ps1_unmatched.csv"
    make_unmatched_with_stilts(sex_csv, ps1_csv, out_csv,
                               ra1="ALPHA_J2000", dec1="DELTA_J2000",
                               ra2="raMean", dec2="decMean")

# USNO-B
for xmatch in RUN_ROOT.glob("run-*/tiles/*/xmatch/sex_usnob_xmatch.csv"):
    # Minimal alignment fix for detection:
    filter_unmatched(
        xmatch,
        candidates=[("RAJ2000","DEJ2000"), ("RA","DEC"), ("ra","dec"), ("RA_ICRS","DE_ICRS")],
        out_name="sex_usnob_unmatched.csv"
    )
    # Recommended STILTS unmatched
    tile_dir = xmatch.parent.parent
    sex_csv = tile_dir / "catalogs" / "sextractor_pass2.csv"
    usnob_csv = tile_dir / "catalogs" / "usnob_neighbourhood.csv"
    out_csv = xmatch.parent / "sex_usnob_unmatched.csv"
    make_unmatched_with_stilts(sex_csv, usnob_csv, out_csv,
                               ra1="ALPHA_J2000", dec1="DELTA_J2000",
                               ra2="RAJ2000", dec2="DEJ2000")
