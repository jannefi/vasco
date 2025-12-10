"""
Post-run STILTS cross-match helper.

Usage examples:
  # Use latest run under data/runs and create external demo catalogs if missing
  python -m vasco.cli_post_xmatch --demo-external --radius 2

  # Target a specific run directory
  python -m vasco.cli_post_xmatch --run data/runs/run-YYYYMMDD_HHMMSS --radius 1.5

This module will:
  1) Locate the run directory (latest by default).
  2) Optionally create tiny Gaia/PS1 demo catalogs (and SExtractor CSV if missing).
  3) Perform STILTS sky cross-match producing two CSVs under <run>/xmatch/ .

It tries to use `stilts_post_xmatch(run_dir)` if it exists in `vasco.cli_pipeline`.
If not, it falls back to a local routine that calls the STILTS wrapper directly.
"""

from __future__ import annotations

from pathlib import Path
import argparse
import csv

# Optional imports; handled lazily in functions


def _latest_run() -> Path:
    runs = Path("data/runs")
    cands = sorted(
        (d for d in runs.glob("run-*") if d.is_dir()),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not cands:
        raise SystemExit("[ERROR] No runs found under data/runs")
    return cands[0]


def _ensure_demo_catalogs(run_dir: Path) -> None:
    cat_dir = run_dir / "catalogs"
    cat_dir.mkdir(parents=True, exist_ok=True)

    sex_csv = cat_dir / "sextractor_pass2.csv"
    gaia_csv = cat_dir / "gaia_neighbourhood.csv"
    ps1_csv = cat_dir / "ps1_neighbourhood.csv"

    # Create a tiny SExtractor-like CSV if missing
    if not sex_csv.exists():
        with sex_csv.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["ID", "ALPHA_J2000", "DELTA_J2000"])
            w.writeheader()
            w.writerow({"ID": "S1", "ALPHA_J2000": 10.0100, "DELTA_J2000": -5.0005})
            w.writerow({"ID": "S2", "ALPHA_J2000": 150.1234, "DELTA_J2000": 2.3456})
            w.writerow({"ID": "S3", "ALPHA_J2000": 222.0000, "DELTA_J2000": 33.0000})

    # Create external demo catalogs if missing
    if not gaia_csv.exists():
        with gaia_csv.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["source_id", "ra", "dec"])
            w.writeheader()
            w.writerow({"source_id": "G1", "ra": 10.0105, "dec": -5.0002})
            w.writerow({"source_id": "G2", "ra": 150.1240, "dec": 2.3459})

    if not ps1_csv.exists():
        with ps1_csv.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["objID", "ra", "dec"])
            w.writeheader()
            w.writerow({"objID": "P1", "ra": 10.0104, "dec": -5.0004})
            w.writerow({"objID": "P2", "ra": 150.1243, "dec": 2.3454})


def _fallback_post_xmatch(run_dir: Path, radius_arcsec: float) -> None:
    """Perform STILTS cross-matching without relying on cli_pipeline helpers."""
    from vasco.mnras.xmatch_stilts import (
        xmatch_sextractor_with_gaia,
        xmatch_sextractor_with_ps1,
    )

    sex_csv = run_dir / "catalogs" / "sextractor_pass2.csv"
    gaia_csv = run_dir / "catalogs" / "gaia_neighbourhood.csv"
    ps1_csv = run_dir / "catalogs" / "ps1_neighbourhood.csv"

    for p in (sex_csv, gaia_csv, ps1_csv):
        if not p.exists():
            raise SystemExit(f"[ERROR] Missing required catalog: {p}")

    xdir = run_dir / "xmatch"
    xdir.mkdir(parents=True, exist_ok=True)
    x_gaia = xdir / "sex_gaia_xmatch.csv"
    x_ps1 = xdir / "sex_ps1_xmatch.csv"

    xmatch_sextractor_with_gaia(sex_csv, gaia_csv, x_gaia, radius_arcsec=radius_arcsec)
    xmatch_sextractor_with_ps1(sex_csv, ps1_csv, x_ps1, radius_arcsec=radius_arcsec)

    print("[OK] STILTS post-xmatch on:", run_dir)
    print("     Gaia ->", x_gaia)
    print("     PS1  ->", x_ps1)


def main():
    ap = argparse.ArgumentParser(description="Run STILTS post-run cross-match")
    ap.add_argument(
        "--run",
        type=str,
        default=None,
        help="Explicit run directory; default: latest under data/runs",
    )
    ap.add_argument(
        "--radius",
        type=float,
        default=2.0,
        help="Match radius in arcseconds (default 2.0)",
    )
    ap.add_argument(
        "--demo-external",
        action="store_true",
        help="Create tiny demo external catalogs (and SExtractor CSV if missing)",
    )
    args = ap.parse_args()

    run_dir = Path(args.run) if args.run else _latest_run()

    if args.demo_external:
        _ensure_demo_catalogs(run_dir)

    # Try the helper in cli_pipeline if available
    try:
        from vasco.cli_pipeline import stilts_post_xmatch as _helper  # type: ignore

        # If helper exists but doesn't accept radius, fall back to local
        try:
            _helper(run_dir)
            print("[INFO] Used stilts_post_xmatch from cli_pipeline")
            return 0
        except TypeError:
            pass
    except Exception:
        pass

    # Fallback path uses our local routine with radius support
    _fallback_post_xmatch(run_dir, radius_arcsec=args.radius)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
