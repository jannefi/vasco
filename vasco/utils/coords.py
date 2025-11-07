from __future__ import annotations
from astropy.coordinates import Angle

def _looks_like_ra_sexagesimal(s: str) -> bool:
    s = s.strip()
    return any(c in s for c in (':','h','m','s','H','M','S'))

def _looks_like_dec_sexagesimal(s: str) -> bool:
    s = s.strip()
    return any(c in s for c in (':','d','D','m','\'', '"'))

def parse_ra(value: str | float | int) -> float:
    s = str(value).strip()
    if _looks_like_ra_sexagesimal(s):
        return Angle(s, unit='hourangle').degree
    return float(s)

def parse_dec(value: str | float | int) -> float:
    s = str(value).strip()
    if _looks_like_dec_sexagesimal(s):
        return Angle(s, unit='deg').degree
    return float(s)
