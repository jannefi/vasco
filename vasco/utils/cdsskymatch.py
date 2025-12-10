from __future__ import annotations
import subprocess
from pathlib import Path


class StiltsNotFound(RuntimeError):
    pass


def _ensure_tool(tool: str) -> None:
    import shutil

    if shutil.which(tool) is None:
        raise StiltsNotFound(f"Required tool '{tool}' not found in PATH.")


# vasco/utils/cdsskymatch.py
def cdsskymatch(
    in_table,
    out_table,
    *,
    ra,
    dec,
    cdstable,
    radius_arcsec: float = 5.0,
    find: str = "best",
    ofmt: str = "csv",
    omode: str = "out",
):
    _ensure_tool("stilts")
    cmd = [
        "stilts",
        "cdsskymatch",
        f"in={str(in_table)}",
        f"ra={ra}",
        f"dec={dec}",
        f"cdstable={cdstable}",
        f"radius={radius_arcsec}",
        f"find={find}",
        f"omode={omode}",
        f"out={str(out_table)}",
        f"ofmt={ofmt}",
    ]
    subprocess.run(cmd, check=True)
