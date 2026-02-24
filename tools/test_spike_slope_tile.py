# tools/test_spike_slope_tile.py
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple



def _add_repo_to_syspath() -> Path:
    """
    Make the 'vasco' package importable without requiring pip install -e.
    Supports both layouts:
      - <repo>/vasco/__init__.py
      - <repo>/src/vasco/__init__.py
    Returns the detected repo root for diagnostics.
    """
    here = Path(__file__).resolve()
    repo_root = None
    add_path = None

    for p in [here] + list(here.parents):
        # flat layout
        if (p / "vasco" / "__init__.py").exists():
            repo_root = p
            add_path = p
            break
        # src layout
        if (p / "src" / "vasco" / "__init__.py").exists():
            repo_root = p
            add_path = p / "src"
            break

    if add_path is None:
        # Fallback: add CWD to sys.path (sometimes enough), but also raise a helpful error.
        cwd = Path.cwd().resolve()
        if str(cwd) not in sys.path:
            sys.path.insert(0, str(cwd))
        raise ModuleNotFoundError(
            "Could not locate 'vasco' package. Looked for vasco/__init__.py or src/vasco/__init__.py "
            f"from {here} upwards. Current working dir is {cwd}."
        )

    if str(add_path) not in sys.path:
        sys.path.insert(0, str(add_path))
    return repo_root or Path.cwd().resolve()


# --- ensure imports work before importing vasco.*
_DETECTED_REPO = _add_repo_to_syspath()

from astropy.table import Table  # type: ignore
#from vasco.mnras.spikes import SpikeRuleConst, SpikeRuleLine, SpikeConfig, BrightStar
from vasco.mnras.apply_spike_cuts_vectorized  import apply_spike_cuts_vectorized

from vasco.mnras.filters_mnras import apply_extract_filters, apply_morphology_filters
from vasco.mnras.spikes import (
    BrightStar,
    SpikeConfig,
    SpikeRuleConst,
    SpikeRuleLine,
    apply_spike_cuts,
    fetch_bright_ps1,
)

# Optional xmatch delta (only if wrapper imports in your environment)
try:
    from vasco.mnras.xmatch_stilts import xmatch_sextractor_with_gaia, xmatch_sextractor_with_ps1

    HAVE_XMATCH = True
except Exception:
    HAVE_XMATCH = False


def read_csv_rows(path: Path) -> List[Dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def write_csv_rows(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def table_to_rows(tab: Table) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in tab:
        d: Dict[str, Any] = {}
        for col in tab.colnames:
            v = r[col]
            try:
                v = v.item()
            except Exception:
                pass
            d[col] = v
        out.append(d)
    return out


def tile_center_from_name_or_index(tile_dir: Path) -> Optional[Tuple[float, float]]:
    # Try directory name: tile-RA<ra>-DEC<dec>
    name = tile_dir.name
    try:
        if name.startswith("tile-RA") and "-DEC" in name:
            ra_part = name[len("tile-RA") : name.index("-DEC")]
            dec_part = name[name.index("-DEC") + len("-DEC") :]
            return float(ra_part), float(dec_part)
    except Exception:
        pass

    # Try RUN_INDEX.json if present
    try:
        idx = tile_dir / "RUN_INDEX.json"
        if idx.exists():
            recs = json.loads(idx.read_text(encoding="utf-8"))
            if recs:
                stem = Path(recs[0].get("tile", "")).name
                parts = stem.split("_")
                return float(parts[1]), float(parts[2])
    except Exception:
        pass

    return None


def numbers_set(rows: List[Dict[str, Any]]) -> Set[str]:
    out: Set[str] = set()
    for r in rows:
        if "NUMBER" in r and r["NUMBER"] not in (None, ""):
            out.add(str(r["NUMBER"]))
    return out


def load_or_fetch_bright(out_dir: Path, center: Tuple[float, float]) -> List[BrightStar]:
    cache = out_dir / "bright_ps1.csv"
    if cache.exists() and cache.stat().st_size > 0:
        bright: List[BrightStar] = []
        with cache.open(newline="") as f:
            rdr = csv.DictReader(f)
            for row in rdr:
                try:
                    bright.append(
                        BrightStar(ra=float(row["ra"]), dec=float(row["dec"]), rmag=float(row["rmag"]))
                    )
                except Exception:
                    continue
        return bright

    bright = fetch_bright_ps1(
        center[0],
        center[1],
        radius_arcmin=35.0,
        rmag_max=16.0,
        mindetections=2,
    )
    write_csv_rows(cache, [{"ra": b.ra, "dec": b.dec, "rmag": b.rmag} for b in bright])
    return bright


def main(tile_path: str) -> int:
    tile_dir = Path(tile_path).resolve()
    cat_dir = tile_dir / "catalogs"
    sex_csv = cat_dir / "sextractor_pass2.csv"

    if not sex_csv.exists():
        print(f"[ERROR] Missing: {sex_csv}")
        return 2

    out_dir = tile_dir / "test_spike_slope"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load raw SExtractor catalog
    tab = Table.read(str(sex_csv), format="ascii.csv")

    # Apply extract + morphology filters (matching your filters_mnras.py behavior)
    tab2 = apply_extract_filters(tab, cfg={"flags_equal": 0, "snr_win_min": 30.0})
    tab3 = apply_morphology_filters(
        tab2,
        cfg={
            "sigma_clip": True,
            "sigma_k": 2.0,
            "spread_model_min": -0.002,
            "fwhm_lower": 2.0,
            "fwhm_upper": 7.0,
            "elongation_lt": 1.3,
            "extent_delta_lt": 2.0,
            "extent_min": 1.0,
        },
    )

    morph_rows = table_to_rows(tab3)
    write_csv_rows(out_dir / "sex_morph.csv", morph_rows)

    center = tile_center_from_name_or_index(tile_dir)
    if center is None:
        bright: List[BrightStar] = []
        (out_dir / "bright_ps1.csv").write_text("", encoding="utf-8")
        print("[WARN] Could not determine tile center -> bright-star list empty (spike stage will have no effect).")
    else:
        bright = load_or_fetch_bright(out_dir, center)

    # Spike rules: old (bug) vs new (fixed)
    cfg_old = SpikeConfig(rules=[SpikeRuleConst(const_max_mag=12.4), SpikeRuleLine(a=-0.09 / 60.0, b=15.3)])
    cfg_new = SpikeConfig(rules=[SpikeRuleConst(const_max_mag=12.4), SpikeRuleLine(a=-0.09, b=15.3)])

    kept_old, rej_old = apply_spike_cuts(
        morph_rows, bright, cfg_old, src_ra_key="ALPHA_J2000", src_dec_key="DELTA_J2000"
    )
    kept_new, rej_new = apply_spike_cuts(
        morph_rows, bright, cfg_new, src_ra_key="ALPHA_J2000", src_dec_key="DELTA_J2000"
    )

    kept_vec, rej_vec = apply_spike_cuts_vectorized(
       morph_rows, bright, cfg_new,
       src_ra_key="ALPHA_J2000",
       src_dec_key="DELTA_J2000",
    )
    numbers_old = numbers_set(kept_new)
    numbers_vec = numbers_set(kept_vec)
    print("delta old vs vec:",
       len(numbers_vec - numbers_old),
       len(numbers_old - numbers_vec))

    write_csv_rows(out_dir / "sex_spikes_old.csv", kept_old)
    write_csv_rows(out_dir / "rejected_old.csv", rej_old)
    write_csv_rows(out_dir / "sex_spikes_new.csv", kept_new)
    write_csv_rows(out_dir / "rejected_new.csv", rej_new)

    s_old = numbers_set(kept_old)
    s_new = numbers_set(kept_new)
    gained = sorted(s_new - s_old)  # kept in new, not in old
    lost = sorted(s_old - s_new)    # kept in old, not in new

    summary = {
        "repo_root_detected": str(_DETECTED_REPO),
        "tile": str(tile_dir),
        "morph_rows": len(morph_rows),
        "bright_stars": len(bright),
        "kept_old": len(kept_old),
        "rejected_old": len(rej_old),
        "kept_new": len(kept_new),
        "rejected_new": len(rej_new),
        "gained_in_new": len(gained),
        "lost_in_new": len(lost),
        "gained_NUMBER_sample": gained[:50],
        "lost_NUMBER_sample": lost[:50],
    }
    (out_dir / "delta_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))

    # Optional: xmatch delta using existing neighborhood catalogs (no downloads)
    if HAVE_XMATCH:
        gaia_csv = cat_dir / "gaia_neighbourhood.csv"
        ps1_csv = cat_dir / "ps1_neighbourhood.csv"
        xdir = out_dir / "xmatch"
        xdir.mkdir(parents=True, exist_ok=True)

        if gaia_csv.exists() and gaia_csv.stat().st_size > 0:
            xmatch_sextractor_with_gaia(out_dir / "sex_spikes_old.csv", gaia_csv, xdir / "sex_gaia_xmatch_old.csv", radius_arcsec=5.0)
            xmatch_sextractor_with_gaia(out_dir / "sex_spikes_new.csv", gaia_csv, xdir / "sex_gaia_xmatch_new.csv", radius_arcsec=5.0)

        if ps1_csv.exists() and ps1_csv.stat().st_size > 0:
            xmatch_sextractor_with_ps1(out_dir / "sex_spikes_old.csv", ps1_csv, xdir / "sex_ps1_xmatch_old.csv", radius_arcsec=5.0)
            xmatch_sextractor_with_ps1(out_dir / "sex_spikes_new.csv", ps1_csv, xdir / "sex_ps1_xmatch_new.csv", radius_arcsec=5.0)

    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python tools/test_spike_slope_tile.py <tile_dir>")
        raise SystemExit(2)
    raise SystemExit(main(sys.argv[1]))
