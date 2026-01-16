#!/usr/bin/env python3
from __future__ import annotations
from datetime import datetime, timezone
import re, json
from pathlib import Path
from typing import Optional, Tuple

ISO_EPOCH0 = datetime(1858, 11, 17, tzinfo=timezone.utc)

def to_mjd(dt: datetime) -> float:
    return (dt - ISO_EPOCH0).total_seconds() / 86400.0

def parse_dateobs_with_sanitize(dateobs: Optional[str]) -> Optional[Tuple[str, float]]:
    """
    Returns (iso_utc, mjd) or None.
    Fixes malformed HH:MM:SS (e.g. '06:77:00') by normalizing overflow minutes/seconds.
    Accepts 'Z' or explicit offset.
    """
    if not dateobs:
        return None
    t = dateobs.strip().replace('Z', '+00:00')
    # Fast path
    try:
        dt = datetime.fromisoformat(t).astimezone(timezone.utc)
        return dt.isoformat(), to_mjd(dt)
    except Exception:
        pass
    m = re.match(r'^(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2}):(\d{2})([+-]\d{2}:\d{2})?$', t)
    if not m:
        return None
    ymd, hh, mm, ss, off = m.group(1), int(m.group(2)), int(m.group(3)), int(m.group(4)), m.group(5)
    # Normalize overflow
    hh_extra, mm = divmod(mm, 60)
    hh = (hh + hh_extra) % 24
    fixed = f"{ymd}T{hh:02d}:{mm:02d}:{ss:02d}{off or '+00:00'}"
    try:
        dt = datetime.fromisoformat(fixed).astimezone(timezone.utc)
        return dt.isoformat(), to_mjd(dt)
    except Exception:
        return None

def get_epoch_from_tile_or_plate(tile_json: dict, region: Optional[str]) -> Optional[Tuple[str, float, str]]:
    """Return (iso_utc, mjd, provenance). Try tile header, then tile 'selected', then full plate header by REGION."""
    hdr = tile_json.get('header', {})
    sel = tile_json.get('selected', {})
    for part, tag in ((hdr, 'tile_header'), (sel, 'tile_header_selected')):
        iso_mjd = parse_dateobs_with_sanitize(part.get('DATE-OBS'))
        if iso_mjd:
            return iso_mjd[0], iso_mjd[1], tag
    if region:
        plate_path = Path('./data/dss1red-headers')/f'dss1red_{region}.fits.header.json'
        if plate_path.exists():
            try:
                j = json.loads(plate_path.read_text(encoding='utf-8'))
                iso_mjd = parse_dateobs_with_sanitize(j.get('header',{}).get('DATE-OBS'))
                if iso_mjd:
                    return iso_mjd[0], iso_mjd[1], 'plate_header'
            except Exception:
                return None
    return None