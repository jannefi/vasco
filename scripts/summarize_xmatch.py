import pandas as pd
from pathlib import Path
import sys

RUN_DIR = Path(sys.argv[1])


def count_rows(path):
    try:
        return sum(1 for _ in open(path)) - 1
    except Exception:
        return 0


for tile_dir in RUN_DIR.glob("tiles/*"):
    p2 = tile_dir / "pass2.ldac"
    gaia_x = tile_dir / "xmatch" / "sex_gaia_xmatch.csv"
    ps1_x = tile_dir / "xmatch" / "sex_ps1_xmatch.csv"
    usno_x = tile_dir / "xmatch" / "sex_usnob_xmatch.csv"
    gaia_un = tile_dir / "xmatch" / "sex_gaia_unmatched.csv"
    ps1_un = tile_dir / "xmatch" / "sex_ps1_unmatched.csv"
    usno_un = tile_dir / "xmatch" / "sex_usnob_unmatched.csv"

    # pass-2 count (LDAC is FITS; we’ll just report xmatch sizes here)
    n_gaia = count_rows(gaia_x) if gaia_x.exists() else 0
    n_ps1 = count_rows(ps1_x) if ps1_x.exists() else 0
    n_usno = count_rows(usno_x) if usno_x.exists() else 0
    n_gaia_un = count_rows(gaia_un) if gaia_un.exists() else 0
    n_usno_un = count_rows(usno_un) if usno_un.exists() else 0
    n_ps1_un = count_rows(ps1_un) if ps1_un.exists() else 0

    print(tile_dir.name)
    print(f"  Gaia: matched rows={n_gaia}, unmatched={n_gaia_un}")
    print(f"  PS1 : matched rows={n_ps1},  unmatched={n_ps1_un}")
    print(f"  USNO-B : matched rows={n_usno},  unmatched={n_usno_un}")
