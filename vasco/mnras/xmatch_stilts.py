from __future__ import annotations
from pathlib import Path
from typing import Optional, Tuple, Iterable
import csv
from vasco.utils.stilts_wrapper import stilts_xmatch

# ---------------------------------------------------------------------
# Generic RA/Dec detector for any CSV table (external catalogs)
# ---------------------------------------------------------------------
def _guess_radec_any(csv_path, preference=("ra","dec")) -> Tuple[str, str]:
    import csv as _csv
    from pathlib import Path as _Path
    cand = [
        preference,
        ("RA_ICRS","DE_ICRS"), ("RAJ2000","DEJ2000"),
        ("RA","DEC"), ("lon","lat"), ("ra","dec"),
        # PS1 mean / TAP variants
        ("raMean","decMean"), ("RAMean","DecMean"),
    ]
    with _Path(csv_path).open(newline='') as f:
        r = _csv.reader(f); hdr = next(r)
    cols = set(h.strip().lstrip('﻿') for h in hdr)
    for ra, dec in cand:
        if ra in cols and dec in cols:
            return ra, dec
    raise ValueError(f'RA/Dec not found in {csv_path}; header={hdr[:12]}')

# SExtractor candidates
# NOTE: We prefer canonical WCS-fixed coordinates when present.
_SEX_CANDIDATES: Iterable[Tuple[str,str]] = (
    ("RA_corr", "Dec_corr"),                 # NEW: canonical coords (preferred)
    ("ALPHAWIN_J2000", "DELTAWIN_J2000"),    # windowed sky coords
    ("ALPHA_J2000", "DELTA_J2000"),          # raw sky coords
    ("X_WORLD", "Y_WORLD"),
    ("ra", "dec"),
    ("RA", "DEC"),
    ("RAJ2000", "DEJ2000"),
)

def _guess_sextractor_radec(csv_path: Path | str) -> Tuple[str,str]:
    p = Path(csv_path)
    with p.open(newline='') as f:
        r = csv.reader(f)
        try:
            header = next(r)
        except StopIteration:
            raise ValueError(f"Empty CSV: {p}")
    header = [h.strip().lstrip('﻿') for h in header]
    cols = set(header)
    for ra_col, dec_col in _SEX_CANDIDATES:
        if ra_col in cols and dec_col in cols:
            return ra_col, dec_col
    raise ValueError(
        f"Could not find RA/Dec columns in {p}. Tried: " +
        ", ".join(f"({a},{b})" for a,b in _SEX_CANDIDATES)
    )

# Wrapper around STILTS call
def xmatch_catalogs_stilts(
    table1: Path | str,
    table2: Path | str,
    out_table: Path | str,
    *,
    ra1: str, dec1: str,
    ra2: str, dec2: str,
    radius_arcsec: float = 2.0,
    join_type: str = '1and2',
    ofmt: Optional[str] = None,
    find: Optional[str] = None,
) -> Path:
    t1 = str(table1); t2 = str(table2); out = str(out_table)
    stilts_xmatch(
        t1, t2, out,
        ra1=ra1, dec1=dec1,
        ra2=ra2, dec2=dec2,
        radius_arcsec=radius_arcsec,
        join_type=join_type,
        ofmt=ofmt,
        find=find,
    )
    return Path(out)

# Gaia fallback (Astropy) with numeric coercion
def _gaia_fallback_match(sex_catalog_csv: Path | str,
                         gaia_catalog_csv: Path | str,
                         out_csv: Path | str,
                         *,
                         radius_arcsec: float) -> Path:
    import pandas as pd
    from astropy.coordinates import SkyCoord
    import astropy.units as u
    sex_p = Path(sex_catalog_csv)
    gaia_p = Path(gaia_catalog_csv)
    out_p = Path(out_csv)
    out_p.parent.mkdir(parents=True, exist_ok=True)

    df1 = pd.read_csv(sex_p)
    df2 = pd.read_csv(gaia_p)

    # SExtractor columns (preference order)
    pref1 = [
        ('RA_corr','Dec_corr'),
        ('ALPHAWIN_J2000','DELTAWIN_J2000'),
        ('ALPHA_J2000','DELTA_J2000'),
        ('X_WORLD','Y_WORLD'),
        ('ra','dec'), ('RA','DEC'), ('RAJ2000','DEJ2000')
    ]
    for a,b in pref1:
        if a in df1.columns and b in df1.columns:
            ra1, dec1 = a, b
            break
    else:
        raise RuntimeError('Fallback: cannot find RA/Dec in SExtractor CSV')

    # Gaia columns (normalized to ra/dec by our fetcher; accept variants)
    pref2 = [('ra','dec'), ('RA_ICRS','DE_ICRS'), ('RAJ2000','DEJ2000'), ('RA','DEC')]
    for a,b in pref2:
        if a in df2.columns and b in df2.columns:
            ra2, dec2 = a, b
            break
    else:
        raise RuntimeError('Fallback: cannot find RA/Dec in Gaia CSV')

    # Coerce to numeric (drop any non-numeric rows)
    df1['_ra'] = pd.to_numeric(df1[ra1], errors='coerce')
    df1['_dec'] = pd.to_numeric(df1[dec1], errors='coerce')
    df2['_ra'] = pd.to_numeric(df2[ra2], errors='coerce')
    df2['_dec'] = pd.to_numeric(df2[dec2], errors='coerce')

    df1v = df1.dropna(subset=['_ra','_dec']).reset_index(drop=True)
    df2v = df2.dropna(subset=['_ra','_dec']).reset_index(drop=True)

    c1 = SkyCoord(df1v['_ra'].to_numpy()*u.deg, df1v['_dec'].to_numpy()*u.deg)
    c2 = SkyCoord(df2v['_ra'].to_numpy()*u.deg, df2v['_dec'].to_numpy()*u.deg)
    idx1, idx2, sep2d, _ = c2.search_around_sky(c1, radius_arcsec*u.arcsec)

    matched = (df1v.iloc[idx2].reset_index(drop=True)
               .join(df2v.iloc[idx1].reset_index(drop=True), lsuffix='_sex', rsuffix='_gaia'))
    matched['sep_arcsec'] = sep2d.arcsec
    matched.to_csv(out_p, index=False)
    return out_p

def xmatch_sextractor_with_gaia(sex_catalog_csv: Path | str,
                                gaia_catalog_csv: Path | str,
                                out_csv: Path | str,
                                *,
                                radius_arcsec: float = 2.0,
                                join_type: str = '1and2') -> Path:
    ra1, dec1 = _guess_sextractor_radec(sex_catalog_csv)
    try:
        ra2, dec2 = _guess_radec_any(gaia_catalog_csv, preference=("ra","dec"))
    except Exception:
        ra2, dec2 = ("ra","dec")

    # Try STILTS first
    try:
        return xmatch_catalogs_stilts(
            sex_catalog_csv, gaia_catalog_csv, out_csv,
            ra1=ra1, dec1=dec1,
            ra2=ra2, dec2=dec2,
            radius_arcsec=radius_arcsec,
            join_type=join_type,
            ofmt='csv',
        )
    except Exception as e:
        print('[POST][INFO] Gaia STILTS failed (', e, ') — using Astropy fallback')
        return _gaia_fallback_match(sex_catalog_csv, gaia_catalog_csv, out_csv,
                                    radius_arcsec=radius_arcsec)

def xmatch_sextractor_with_ps1(sex_catalog_csv: Path | str,
                               ps1_catalog_csv: Path | str,
                               out_csv: Path | str,
                               *,
                               radius_arcsec: float = 2.0,
                               join_type: str = '1and2') -> Path:
    ra1, dec1 = _guess_sextractor_radec(sex_catalog_csv)
    ra2, dec2 = _guess_radec_any(ps1_catalog_csv, preference=("ra","dec"))
    return xmatch_catalogs_stilts(
        sex_catalog_csv, ps1_catalog_csv, out_csv,
        ra1=ra1, dec1=dec1,
        ra2=ra2, dec2=dec2,
        radius_arcsec=radius_arcsec,
        join_type=join_type,
        ofmt='csv',
    )