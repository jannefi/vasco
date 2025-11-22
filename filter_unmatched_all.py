
import pandas as pd
from pathlib import Path
import sys

RUN_ROOT = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data/runs")

def detect_cols(cols, candidates):
    for pair in candidates:
        if set(pair).issubset(cols):
            return pair
    return None

def filter_unmatched(xmatch_csv, candidates, out_name):
    if not xmatch_csv.exists():
        return
    df = pd.read_csv(xmatch_csv)
    pair = detect_cols(df.columns, candidates)
    if not pair:
        print("[WARN] RA/Dec columns not found in:", xmatch_csv)
        return
    ra, de = pair
    # unmatched = missing/non-numeric RA/Dec
    # pandas: isna() handles nulls; to catch blanks, add .str.strip()=='' for object dtype
    unmatched = df[df[ra].isna() | df[de].isna()]
    out = xmatch_csv.parent / out_name
    unmatched.to_csv(out, index=False)
    print("[INFO] Unmatched ->", out)

for xmatch in RUN_ROOT.glob("run-*/tiles/*/xmatch/sex_gaia_xmatch.csv"):
    filter_unmatched(xmatch,
        candidates=[("RA_ICRS","DE_ICRS"), ("RAJ2000","DEJ2000"),
                    ("ra","dec"), ("RA","DEC")],
        out_name="sex_gaia_unmatched.csv")

for xmatch in RUN_ROOT.glob("run-*/tiles/*/xmatch/sex_ps1_xmatch.csv"):
    filter_unmatched(xmatch,
        candidates=[("raMean","decMean"), ("RAMean","DecMean"),
                    ("ra","dec"), ("RA","DEC")],
        out_name="sex_ps1_unmatched.csv")

