from __future__ import annotations
import logging
from pathlib import Path
from typing import Literal, Tuple, List
import numpy as np
from astropy.io import fits
from astropy.table import Table
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

__all__ = ["export_and_summarize"]
_logger = logging.getLogger("vasco")
ExportMode = Literal["none", "csv", "parquet", "both"]


# ---- LDAC reader ----
def _read_ldac_table(ldac_path: str | Path) -> Table:
    p = Path(ldac_path)
    with fits.open(p, memmap=False) as hdul:
        for hdu in hdul:
            if (
                isinstance(hdu, fits.BinTableHDU)
                and hdu.header.get("EXTNAME", "").upper() == "LDAC_OBJECTS"
            ):
                return Table(hdu.data)
        for hdu in hdul[1:]:
            if isinstance(hdu, fits.BinTableHDU):
                return Table(hdu.data)
    raise RuntimeError("No table found in LDAC catalog: " + str(p))


# ---- Helpers ----
def _to_dataframe(tab: Table):
    try:
        import pandas as pd

        return tab.to_pandas(), None
    except Exception as exc:
        return None, str(exc)


def _one_d_columns(tab: Table):
    names, skipped = [], []
    for name in tab.colnames:
        try:
            shape = np.asarray(tab[name]).shape
        except Exception:
            shape = ()
        if len(shape) <= 1:
            names.append(name)
        else:
            skipped.append(name)
    return names, skipped


# ---- Writers ----
def _write_csv_and_ecsv(tab: Table, out_dir: Path) -> tuple[Path, list[str]]:
    out_dir.mkdir(parents=True, exist_ok=True)
    # ECSV (full fidelity)
    ecsv_path = out_dir / "final_catalog.ecsv"
    try:
        tab.write(ecsv_path, format="ascii.ecsv", overwrite=True)
        _logger.info("[INFO] Wrote ECSV (all columns): %s", ecsv_path)
    except Exception as exc:
        _logger.warning("[WARN] Could not write ECSV: %s", exc)
    # CSV (1-D subset only)
    csv_path = out_dir / "final_catalog.csv"
    names_1d, skipped = _one_d_columns(tab)
    tab_csv = tab[names_1d]
    df, err = _to_dataframe(tab_csv)
    if df is not None:
        df.to_csv(csv_path, index=False)
    else:
        tab_csv.write(csv_path, format="ascii.csv", overwrite=True)
        _logger.warning("[WARN] CSV via Astropy (pandas unavailable: %s)", err)
    if skipped:
        _logger.info(
            "[INFO] CSV omitted multidimensional columns: %s", ",".join(skipped)
        )
    return csv_path, skipped


def _write_parquet(tab: Table, out_dir: Path) -> Path | None:
    pq_path = out_dir / "final_catalog.parquet"
    names_1d, _ = _one_d_columns(tab)
    tab_1d = tab[names_1d]
    df, err = _to_dataframe(tab_1d)
    if df is not None:
        try:
            df.to_parquet(pq_path, index=False)
            _logger.info("[INFO] Wrote Parquet via pandas: %s", pq_path)
            return pq_path
        except Exception as exc:
            _logger.warning(
                "[WARN] Parquet via pandas failed (%s); trying pyarrow.", exc
            )
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        arrays = {name: np.asarray(tab_1d[name]) for name in tab_1d.colnames}
        table = pa.table(arrays)
        pq.write_table(table, pq_path)
        _logger.info("[INFO] Wrote Parquet via pyarrow (1-D subset): %s", pq_path)
        return pq_path
    except Exception as exc:
        _logger.warning("[WARN] Parquet not available (%s); skipping.", exc)
        return None


# ---- QA utilities ----
def _finite2(x, y):
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    m = np.isfinite(x) & np.isfinite(y)
    return x[m], y[m]


def _finite1(x):
    x = np.asarray(x, dtype=float)
    m = np.isfinite(x)
    return x[m]


def _scatter(
    tab: Table,
    xname: str,
    yname: str,
    out: Path,
    *,
    xlog: bool = False,
    ylog: bool = False,
    xlabel: str | None = None,
    ylabel: str | None = None,
    title: str | None = None,
    s: int = 6,
    alpha: float = 0.5,
    color: str = "#4472c4",
) -> Path | None:
    if xname not in tab.colnames or yname not in tab.colnames:
        return None
    x, y = _finite2(tab[xname], tab[yname])
    if x.size == 0:
        return None
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.scatter(x, y, s=s, alpha=alpha, c=color, edgecolors="none")
    ax.set_xlabel(xlabel or xname)
    ax.set_ylabel(ylabel or yname)
    if title:
        ax.set_title(title)
    if xlog:
        ax.set_xscale("log")
    if ylog:
        ax.set_yscale("log")
    # Astronomy convention: magnitudes decrease to the right
    try:
        if (xlabel and "MAG" in str(xlabel)) or ("MAG" in xname):
            ax.invert_xaxis()
    except Exception:
        pass
    fig.tight_layout()
    fig.savefig(out, dpi=120)
    plt.close(fig)
    return out


def _hist(
    tab: Table,
    name: str,
    out: Path,
    bins: int = 30,
    color: str = "#4472c4",
    title: str | None = None,
) -> Path | None:
    if name not in tab.colnames:
        return None
    vals = _finite1(tab[name])
    if vals.size == 0:
        return None
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.hist(vals, bins=bins, color=color, edgecolor="white")
    ax.set_xlabel(name)
    ax.set_ylabel("Count")
    if title:
        ax.set_title(title)
    else:
        ax.set_title(f"{name} histogram (N={vals.size})")
    fig.tight_layout()
    fig.savefig(out, dpi=120)
    plt.close(fig)
    return out


# ---- Main API ----
def export_and_summarize(
    ldac_path: str | Path,
    run_dir: str | Path,
    export: ExportMode = "csv",
    histogram_col: str = "FWHM_IMAGE",
    histogram_png: str = "qa_fwhm_image.png",
) -> Tuple[Path | None, Path | None]:
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    tab = _read_ldac_table(ldac_path)
    csv_path: Path | None = None
    pq_path: Path | None = None
    if export in ("csv", "both"):
        csv_path, _ = _write_csv_and_ecsv(tab, run_dir)
        _logger.info("[INFO] Wrote CSV: %s", csv_path)
    if export in ("parquet", "both"):
        pq_path = _write_parquet(tab, run_dir)
    # Basic stats
    n_sources = len(tab)
    median_val = float("nan")
    if histogram_col in tab.colnames:
        try:
            median_val = float(
                np.nanmedian(np.asarray(tab[histogram_col], dtype=float))
            )
        except Exception:
            median_val = float("nan")
    # QA plots set
    qa_files: List[str] = []
    # 1) Keep original single histogram (FWHM_IMAGE by default)
    hist_path = run_dir / histogram_png
    hp = _hist(tab, histogram_col, hist_path)
    if hp is not None:
        qa_files.append(hp.name)
    # 2) Additional histograms (if present)
    for name, fname in [
        ("MAG_AUTO", "qa_mag_auto_hist.png"),
        ("CLASS_STAR", "qa_class_star_hist.png"),
        ("SNR_WIN", "qa_snr_win_hist.png"),
    ]:
        p = _hist(tab, name, run_dir / fname)
        if p is not None:
            qa_files.append(p.name)
    # 3) Scatter plots (if columns exist)
    scat_specs = [
        (
            "MAG_AUTO",
            "SNR_WIN",
            "qa_mag_vs_snr.png",
            False,
            True,
            "MAG_AUTO [mag]",
            "SNR_WIN",
            "SNR vs MAG",
        ),
        (
            "MAG_AUTO",
            "FWHM_IMAGE",
            "qa_fwhm_vs_mag.png",
            False,
            False,
            "MAG_AUTO [mag]",
            "FWHM_IMAGE [pix]",
            "FWHM vs MAG",
        ),
        (
            "MAG_AUTO",
            "ELLIPTICITY",
            "qa_ellipticity_vs_mag.png",
            False,
            False,
            "MAG_AUTO [mag]",
            "ELLIPTICITY",
            "ELLIPTICITY vs MAG",
        ),
        (
            "CLASS_STAR",
            "MAG_AUTO",
            "qa_class_star_vs_mag.png",
            False,
            False,
            "CLASS_STAR",
            "MAG_AUTO [mag]",
            "MAG vs CLASS_STAR",
        ),
    ]
    for xname, yname, fname, xlog, ylog, xlabel, ylabel, title in scat_specs:
        p = _scatter(
            tab,
            xname,
            yname,
            run_dir / fname,
            xlog=xlog,
            ylog=ylog,
            xlabel=xlabel,
            ylabel=ylabel,
            title=title,
        )
        if p is not None:
            qa_files.append(p.name)
    # Write summary markdown with gallery
    summary_md = run_dir / "RUN_SUMMARY.md"
    nl = ""
    lines = [
        "# Run Summary",
        "",
        f"- **N_SOURCES**: {n_sources}",
        (
            f"- **median {histogram_col}**: {median_val:.3f}"
            if np.isfinite(median_val)
            else f"- **median {histogram_col}**: n/a"
        ),
    ]
    # CSV omissions note (optional)
    try:
        names_1d, skipped_md = _one_d_columns(tab)
        if skipped_md:
            lines.append(
                f"- **CSV omitted multidimensional columns**: {', '.join(skipped_md)}"
            )
    except Exception:
        pass
    if qa_files:
        lines += ["", "## QA Plots"]
        for f in qa_files:
            lines.append(f"![ ]({f})")
    summary_md.write_text(nl.join(lines) + nl, encoding="utf-8")
    _logger.info("[INFO] Wrote summary: %s", summary_md)
    return csv_path, pq_path
