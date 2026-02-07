#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple
import json
import re

ISO_EPOCH0 = datetime(1858, 11, 17, tzinfo=timezone.utc)

# New canonical location in the repository (public, committed)
PLATE_HEADERS_DIR = Path("metadata/plates/headers")

# Legacy fallbacks kept for resilience (no indexing, just direct files)
# - Original util used "./data/dss1red-headers" (dash) 
LEGACY_HEADER_DIRS = [
    Path("./data/dss1red_headers"),   # underscore variant (older setups)
    Path("./data/dss1red-headers"),   # dash variant used in older code
]

def to_mjd(dt: datetime) -> float:
    return (dt - ISO_EPOCH0).total_seconds() / 86400.0

def _normalize_overflow(hh: int, mm: int, ss: int) -> tuple[int, int, int]:
    """Normalize seconds -> minutes and minutes -> hours; wrap hour to 0..23."""
    mm_add, ss = divmod(ss, 60)
    mm += mm_add
    hh_add, mm = divmod(mm, 60)
    hh = (hh + hh_add) % 24
    return hh, mm, ss

def parse_dateobs_with_sanitize(dateobs: Optional[str]) -> Optional[Tuple[str, float]]:
    """
    Return (iso_utc, mjd) or None.

    Robustness improvements:
    - Accept 'Z' or explicit offset (±HH:MM). Assume UTC if missing.
    - Accept either 'T' or ' ' as date/time separator and normalize to 'T'.
    - Tolerate overflow in minutes/seconds (e.g., '06:77:90').
    - Accept optional fractional seconds (e.g., '12:34:56.789').
    """
    if not dateobs:
        return None

    t = dateobs.strip().replace("Z", "+00:00")
    # Normalize space separator to 'T'
    if " " in t and "T" not in t:
        t = t.replace(" ", "T")

    # If no timezone provided, assume UTC
    if re.match(r"^\d{4}-\d{2}-\d{2}T", t) and not re.search(r"[+-]\d{2}:\d{2}$", t):
        t = f"{t}+00:00"

    # First try Python's ISO parser
    try:
        dt = datetime.fromisoformat(t).astimezone(timezone.utc)
        return dt.isoformat(), to_mjd(dt)
    except Exception:
        pass

    # Manual parse with overflow + fractional seconds
    m = re.match(
        r"^(\d{4}-\d{2}-\d{2})T"          # date
        r"(\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?"  # time with optional .fff
        r"([+\-]\d{2}:\d{2})$",           # offset (guaranteed after normalization)
        t
    )
    if not m:
        return None

    ymd = m.group(1)
    hh, mm, ss = int(m.group(2)), int(m.group(3)), int(m.group(4))
    frac = m.group(5)
    off = m.group(6)

    # Overflow-normalize
    hh, mm, ss = _normalize_overflow(hh, mm, ss)
    # Rebuild with (optional) fractional seconds
    if frac is not None:
        fixed = f"{ymd}T{hh:02d}:{mm:02d}:{ss:02d}.{frac}{off}"
    else:
        fixed = f"{ymd}T{hh:02d}:{mm:02d}:{ss:02d}{off}"

    try:
        dt = datetime.fromisoformat(fixed).astimezone(timezone.utc)
        return dt.isoformat(), to_mjd(dt)
    except Exception:
        return None

def _candidate_header_paths(plate_id: str):
    """
    Generate plausible header JSON paths for a DSS1-red plate (REGION).
    We do not consult any index; we look only on disk.

    Preferred (repo):
      metadata/plates/headers/dss1red_{REGION}.fits.header.json
      metadata/plates/headers/{REGION}.fits.header.json
      metadata/plates/headers/{REGION}.header.json

    Legacy fallbacks (older local trees):
      ./data/dss1red_headers/dss1red_{REGION}.fits.header.json
      ./data/dss1red-headers/dss1red_{REGION}.fits.header.json
    """
    names = [
        f"dss1red_{plate_id}.fits.header.json",
        f"{plate_id}.fits.header.json",
        f"{plate_id}.header.json",
    ]

    for n in names:
        yield PLATE_HEADERS_DIR / n
    for legacy_dir in LEGACY_HEADER_DIRS:
        yield legacy_dir / names[0]  # the canonical legacy filename

def _load_plate_header_json(plate_id: str) -> Optional[dict]:
    """Return header JSON dict if a known-named file exists; else None."""
    for p in _candidate_header_paths(plate_id):
        if p.exists():
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                # Try next candidate on parse error
                continue
    return None

def get_epoch_from_tile_or_plate(tile_json: dict, plate_id: Optional[str]) -> Optional[Tuple[str, float, str]]:
    """
    Return (iso_utc, mjd, provenance).

    Order:
      1) tile_json['header']['DATE-OBS']
      2) tile_json['selected']['DATE-OBS']
      3) plate header JSON from metadata/plates/headers (by plate_id/REGION)
    """
    hdr = (tile_json or {}).get("header", {}) or {}
    sel = (tile_json or {}).get("selected", {}) or {}

    for part, tag in ((hdr, "tile_header"), (sel, "tile_header_selected")):
        iso_mjd = parse_dateobs_with_sanitize(part.get("DATE-OBS"))
        if iso_mjd:
            return iso_mjd[0], iso_mjd[1], tag

    if plate_id:
        j = _load_plate_header_json(plate_id)
        if j:
            # Expect the verbatim FITS header under 'header'
            iso_mjd = parse_dateobs_with_sanitize((j.get("header") or {}).get("DATE-OBS"))
            if iso_mjd:
                return iso_mjd[0], iso_mjd[1], "plate_header"

    return None

# Backward-compat alias for older callers that pass 'region'
get_epoch_from_tile_or_region = get_epoch_from_tile_or_plate