from __future__ import annotations
import argparse, json, csv
from pathlib import Path
from typing import List, Dict, Any, Tuple
import numpy as np
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from .exporter3 import _read_ldac_table as _read_ldac_table

QA_FILES = [
    "qa_fwhm_image.png",
    "qa_mag_auto_hist.png",
    "qa_class_star_hist.png",
    "qa_snr_win_hist.png",
    "qa_mag_vs_snr.png",
    "qa_fwhm_vs_mag.png",
    "qa_ellipticity_vs_mag.png",
    "qa_class_star_vs_mag.png",
]


def _estimate_tile_center(tab) -> Tuple[float, float]:
    ra = float("nan")
    dec = float("nan")
    if tab is None:
        return ra, dec
    cols = set([c.upper() for c in tab.colnames])

    def medpair(rname, dname):
        try:
            return float(np.nanmedian(np.asarray(tab[rname], dtype=float))), float(
                np.nanmedian(np.asarray(tab[dname], dtype=float))
            )
        except Exception:
            return float("nan"), float("nan")

    if "ALPHA_J2000" in cols and "DELTA_J2000" in cols:
        ra, dec = medpair("ALPHA_J2000", "DELTA_J2000")
    elif "X_WORLD" in cols and "Y_WORLD" in cols:
        ra, dec = medpair("X_WORLD", "Y_WORLD")
    elif "RA" in cols and "DEC" in cols:
        ra, dec = medpair("RA", "DEC")
    return ra, dec


def _load_counts(run_dir: Path) -> Dict[str, int]:
    p = run_dir / "RUN_COUNTS.json"
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"planned": 0, "downloaded": 0, "processed": 0}


def _iter_tiles(run_dir: Path) -> List[Path]:
    tiles_root = run_dir / "tiles"
    if not tiles_root.exists():
        return []
    return sorted([p for p in tiles_root.iterdir() if p.is_dir()])


def _tile_stats(tile_dir: Path) -> Dict[str, Any]:
    ecsv = tile_dir / "final_catalog.ecsv"
    ldac = tile_dir / "pass2.ldac"
    tab = None
    has_ecsv = ecsv.exists()
    has_ldac = ldac.exists()
    if has_ecsv:
        try:
            from astropy.table import Table

            tab = Table.read(ecsv, format="ascii.ecsv")
        except Exception:
            tab = None
    if tab is None and has_ldac:
        try:
            tab = _read_ldac_table(ldac)
        except Exception:
            tab = None
    n = int(len(tab)) if tab is not None else 0
    med_fwhm = float("nan")
    if tab is not None and "FWHM_IMAGE" in tab.colnames:
        try:
            med_fwhm = float(np.nanmedian(np.asarray(tab["FWHM_IMAGE"], dtype=float)))
        except Exception:
            med_fwhm = float("nan")
    ra_c, dec_c = _estimate_tile_center(tab)
    existing_qas = [name for name in QA_FILES if (tile_dir / name).exists()]
    return {
        "n_sources": n,
        "median_fwhm": med_fwhm,
        "qa": existing_qas,
        "has_ecsv": has_ecsv,
        "has_ldac": has_ldac,
        "ra_center": ra_c,
        "dec_center": dec_c,
    }


def _write_csv_index(run_dir: Path, rows: List[Dict[str, Any]]) -> Path:
    out = run_dir / "tiles_index.csv"
    cols = [
        "tile",
        "ra_center",
        "dec_center",
        "n_sources",
        "median_fwhm",
        "has_ecsv",
        "has_ldac",
    ] + QA_FILES
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            qa_set = set(r.get("qa", []))
            med = r.get("median_fwhm", float("nan"))
            med_str = ("%0.6f" % med) if np.isfinite(med) else ""
            ra_val = r.get("ra_center", float("nan"))
            dec_val = r.get("dec_center", float("nan"))
            ra_str = ("%0.6f" % ra_val) if np.isfinite(ra_val) else ""
            dec_str = ("%0.6f" % dec_val) if np.isfinite(dec_val) else ""
            w.writerow(
                [
                    r["tile"],
                    ra_str,
                    dec_str,
                    r["n_sources"],
                    med_str,
                    r["has_ecsv"],
                    r["has_ldac"],
                ]
                + [(q in qa_set) for q in QA_FILES]
            )
    return out


def build_dashboard(run_dir: Path, *, max_gallery: int = 50, top_n: int = 5) -> Path:
    run_dir = run_dir.resolve()
    tiles = _iter_tiles(run_dir)
    counts = _load_counts(run_dir)
    rows: List[Dict[str, Any]] = []
    for td in tiles:
        st = _tile_stats(td)
        rows.append({"tile": td.name, **st})

    csv_idx = _write_csv_index(run_dir, rows)

    total_sources = int(sum(r["n_sources"] for r in rows))
    medians = np.array(
        [r["median_fwhm"] for r in rows if np.isfinite(r["median_fwhm"])], dtype=float
    )

    hist_png = run_dir / "RUN_QA_fwhm_median_hist.png"
    if medians.size:
        plt.figure(figsize=(6, 4))
        plt.hist(medians, bins=20, color="#2f5597", edgecolor="white")
        plt.xlabel("median FWHM_IMAGE per tile [pix]")
        plt.ylabel("Tiles")
        plt.title("Median FWHM per tile (N=%d)" % medians.size)
        plt.tight_layout()
        plt.savefig(hist_png, dpi=120)
        plt.close()

    md = run_dir / "RUN_DASHBOARD.md"
    md_lines: List[str] = []
    md_lines += [
        "# Run Dashboard",
        "",
        "- **Run directory**: `%s`" % run_dir.name,
        "- **Planned**: %d  **Downloaded**: %d  **Processed**: %d"
        % (
            counts.get("planned", 0),
            counts.get("downloaded", 0),
            counts.get("processed", 0),
        ),
        "- **Total sources (sum over tiles)**: %d" % total_sources,
        (
            ("- **Median FWHM (tile medians)**: %0.3f" % float(np.nanmedian(medians)))
            if medians.size
            else "- **Median FWHM (tile medians)**: n/a"
        ),
        "- **CSV index**: `%s`" % csv_idx.name,
        "",
    ]
    if hist_png.exists():
        md_lines += ["![Median FWHM per tile](%s)" % hist_png.name, ""]

    high_blur = sorted(
        [r for r in rows if np.isfinite(r["median_fwhm"])],
        key=lambda r: r["median_fwhm"],
        reverse=True,
    )[:top_n]
    low_count = sorted(rows, key=lambda r: r["n_sources"])[:top_n]
    md_lines += ["## Top-%d anomalies" % top_n, ""]
    md_lines += ["**Highest median FWHM (blurriest)**", ""]
    for r in high_blur:
        ra = r.get("ra_center", float("nan"))
        dec = r.get("dec_center", float("nan"))
        loc = (
            (" RA=%0.5f Dec=%0.5f" % (ra, dec))
            if (np.isfinite(ra) and np.isfinite(dec))
            else ""
        )
        md_lines += [
            "- `%s` — N=%d  median FWHM=%0.3f%s  ([open](tiles/%s))"
            % (r["tile"], r["n_sources"], r["median_fwhm"], loc, r["tile"])
        ]
    md_lines += ["", "**Lowest source counts**", ""]
    for r in low_count:
        ra = r.get("ra_center", float("nan"))
        dec = r.get("dec_center", float("nan"))
        loc = (
            ("  RA=%0.5f Dec=%0.5f" % (ra, dec))
            if (np.isfinite(ra) and np.isfinite(dec))
            else ""
        )
        line = "- `%s` — N=%d%s" % (r["tile"], r["n_sources"], loc)
        if np.isfinite(r["median_fwhm"]):
            line += "  median FWHM=%0.3f" % r["median_fwhm"]
        line += "  ([open](tiles/%s))" % r["tile"]
        md_lines += [line]
    md_lines += [""]

    show = rows[:max_gallery]
    md_lines += ["## Tile Gallery (first %d tiles)" % len(show), ""]
    for r in show:
        ra = r.get("ra_center", float("nan"))
        dec = r.get("dec_center", float("nan"))
        loc = (
            ("  (RA=%0.5f Dec=%0.5f)" % (ra, dec))
            if (np.isfinite(ra) and np.isfinite(dec))
            else ""
        )
        head = "### `%s` — N=%d%s" % (r["tile"], r["n_sources"], loc)
        if np.isfinite(r["median_fwhm"]):
            head += "  median FWHM=%0.3f" % r["median_fwhm"]
        md_lines += [head]
        imgs = r["qa"][:3]
        if imgs:
            md_lines += [""]
            for im in imgs:
                rel = "tiles/%s/%s" % (r["tile"], im)
                md_lines += ["![](%s)" % rel]
            md_lines += [""]
        md_lines += ["[Open tile folder](tiles/%s)" % r["tile"], ""]

    md.write_text(chr(10).join(md_lines) + chr(10), encoding="utf-8")

    return md


def cmd_build(args: argparse.Namespace) -> int:
    run_dir = Path(args.run_dir)
    max_gallery = int(args.max_tiles)
    top_n = int(args.top_n)
    out = build_dashboard(run_dir, max_gallery=max_gallery, top_n=top_n)
    print("Dashboard:", out)
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        prog="vasco.cli_dashboard",
        description="Build run-level dashboard (Markdown) for a VASCO run",
    )
    sub = p.add_subparsers(dest="cmd")
    b = sub.add_parser("build", help="Build dashboard for a run directory")
    b.add_argument("--run-dir", required=True)
    b.add_argument("--max-tiles", default=50)
    b.add_argument("--top-n", default=5)
    b.set_defaults(func=cmd_build)
    args = p.parse_args(argv)
    if hasattr(args, "func"):
        return args.func(args)
    p.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
