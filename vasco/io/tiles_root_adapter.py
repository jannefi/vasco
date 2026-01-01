
# -*- coding: utf-8 -*-
"""
tiles_root_adapter.py — Dual-layout compatibility for VASCO tile trees (WSL/Unix paths).

Supports:
  Legacy:   ./data/tiles/<tileid>/
  Sharded:  ./data/tiles_by_sky/ra_bin=RRR/dec_bin=SS/<tileid>/

Usage in scripts:
  from vasco.io.tiles_root_adapter import TilesAdapter
  ta = TilesAdapter(base_dir="./data", env_var="VASCO_TILES_ROOT")  # default uses symlink ./data

  for tile in ta.iter_tiles():                # iterates both layouts
      tile_id   = tile.tile_id
      tile_path = tile.path
      ra, dec   = ta.read_ra_dec(tile)        # normalized [0,360), [-90,+90]
      # ... per-tile processing ...
"""

import os, re, json, math, hashlib
from dataclasses import dataclass
from typing import Iterator, Optional, Tuple, List

RA_KEYS  = ["RA_DEG", "CRVAL1", "RA", "OBJ_RA"]
DEC_KEYS = ["DEC_DEG", "CRVAL2", "DEC", "OBJ_DEC"]

# tileid like "tile-RA202.172-DEC+33.589"
PAT_TILE_RADEC_A = re.compile(r"\bRA\s*([0-9]+(?:\.[0-9]+)?)\b.*?\bDEC\s*([+\-][0-9]+(?:\.[0-9]+)?)", re.I)
# tileid like "RA150.000_dec+20.000" (order may vary)
PAT_TILE_RADEC_B = re.compile(r"\bRA\s*([0-9]+(?:\.[0-9]+)?)\b.*?\bDEC\s*([+\-]?[0-9]+(?:\.[0-9]+)?)", re.I)
# FITS like "dss1-red_110.135_-24.656_30arcmin.fits"
PAT_FITS_NAME    = re.compile(r"^.+?_([0-9]+(?:\.[0-9]+)?)_([+\-]?[0-9]+(?:\.[0-9]+)?)_[0-9]+(?:arcmin)?\.fits$", re.I)

@dataclass
class Tile:
    tile_id: str
    path: str         # absolute path to the tile directory
    layout: str       # "legacy" or "sharded"
    ra: Optional[float] = None
    dec: Optional[float] = None

class TilesAdapter:
    def __init__(self, base_dir: str = "./data", env_var: Optional[str] = "VASCO_TILES_ROOT", bin_deg: int = 5):
        if env_var and os.getenv(env_var):
            base_dir = os.getenv(env_var)
        self.base_dir = os.path.abspath(base_dir)
        self.bin_deg  = bin_deg
        self.legacy_root  = os.path.join(self.base_dir, "tiles")
        self.sharded_root = os.path.join(self.base_dir, "tiles_by_sky")

    # -------- public API --------
    def iter_tiles(self) -> Iterator[Tile]:
        """Iterate tiles from whichever layout(s) exist."""
        if os.path.isdir(self.sharded_root):
            yield from self._iter_sharded()
        if os.path.isdir(self.legacy_root):
            yield from self._iter_legacy()

    def iter_legacy(self) -> Iterator[Tile]:
        if os.path.isdir(self.legacy_root):
            yield from self._iter_legacy()

    def iter_sharded(self) -> Iterator[Tile]:
        if os.path.isdir(self.sharded_root):
            yield from self._iter_sharded()

    def read_ra_dec(self, tile: Tile) -> Tuple[Optional[float], Optional[float]]:
        if tile.ra is not None and tile.dec is not None:
            return tile.ra, tile.dec
        ra_dec = self._read_header_ra_dec(tile.path, tile.tile_id) \
                 or self._parse_tileid_ra_dec(tile.tile_id) \
                 or self._parse_fitsname_ra_dec(tile.path)
        tile.ra, tile.dec = ra_dec if ra_dec else (None, None)
        return tile.ra, tile.dec

    def sharded_dest_for(self, tile: Tile) -> str:
        ra, dec = self.read_ra_dec(tile)
        if ra is None or dec is None:
            h = hashlib.sha1(tile.tile_id.encode("utf-8")).hexdigest()
            return os.path.join(self.sharded_root, "fallback_id", h[:2], h[2:4], tile.tile_id)
        return os.path.join(self.sharded_root,
                            f"ra_bin={self._fmt_ra_bin(ra)}",
                            f"dec_bin={self._fmt_dec_bin(dec)}",
                            tile.tile_id)

    # -------- private helpers --------
    def _iter_legacy(self) -> Iterator[Tile]:
        for e in sorted(os.scandir(self.legacy_root), key=lambda x: x.name):
            if e.is_dir():
                yield Tile(tile_id=e.name, path=e.path, layout="legacy")

    def _iter_sharded(self) -> Iterator[Tile]:
        for ra_dir in sorted(os.scandir(self.sharded_root), key=lambda x: x.name):
            if not ra_dir.is_dir() or not ra_dir.name.startswith("ra_bin="): continue
            for dec_dir in sorted(os.scandir(ra_dir.path), key=lambda x: x.name):
                if not dec_dir.is_dir() or not dec_dir.name.startswith("dec_bin="): continue
                for tile_dir in sorted(os.scandir(dec_dir.path), key=lambda x: x.name):
                    if tile_dir.is_dir():
                        yield Tile(tile_id=tile_dir.name, path=tile_dir.path, layout="sharded")

    def _read_header_ra_dec(self, tile_path: str, tile_id: str):
        raw_dir = os.path.join(tile_path, "raw")
        if not os.path.isdir(raw_dir): return None
        preferred = os.path.join(raw_dir, f"{tile_id}.fits.header.json")
        candidates: List[str] = []
        if os.path.isfile(preferred): candidates.append(preferred)
        for e in os.scandir(raw_dir):
            if e.is_file() and e.name.endswith(".fits.header.json"):
                if e.path not in candidates: candidates.append(e.path)
        for path in candidates:
            try:
                with open(path, "r", encoding="utf-8") as f:
                    hdr = json.load(f)
                ra, dec = None, None
                for k in RA_KEYS:
                    if k in hdr: ra = float(hdr[k]); break
                for k in DEC_KEYS:
                    if k in hdr: dec = float(hdr[k]); break
                if ra is not None and dec is not None:
                    return self._norm_ra_dec(ra, dec)
            except Exception:
                continue
        return None

    def _parse_tileid_ra_dec(self, tile_id: str):
        m = PAT_TILE_RADEC_A.search(tile_id)
        if m: return self._norm_ra_dec(float(m.group(1)), float(m.group(2)))
        m = PAT_TILE_RADEC_B.search(tile_id)
        if m: return self._norm_ra_dec(float(m.group(1)), float(m.group(2)))
        return None

    def _parse_fitsname_ra_dec(self, tile_path: str):
        raw_dir = os.path.join(tile_path, "raw")
        if not os.path.isdir(raw_dir): return None
        for e in os.scandir(raw_dir):
            if e.is_file() and e.name.lower().endswith(".fits"):
                m = PAT_FITS_NAME.match(e.name)
                if m: return self._norm_ra_dec(float(m.group(1)), float(m.group(2)))
        return None

    def _norm_ra_dec(self, ra: float, dec: float):
        ra  = ra % 360.0
        dec = max(-90.0, min(90.0, dec))
        return ra, dec

    def _bin_val(self, v: float) -> int:
        return int(math.floor(v / self.bin_deg) * self.bin_deg)
    def _fmt_ra_bin(self, ra: float) -> str:
        return f"{self._bin_val(ra):03d}"
    def _fmt_dec_bin(self, dec: float) -> str:
        b = self._bin_val(dec)
        return f"{'+' if b >= 0 else '-'}{abs(b):02d}"

