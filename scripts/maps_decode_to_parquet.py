#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Decode APS/MAPS P###.dat binary files into Parquet.

Spec basis: README.data_format.txt
- Each file: 156-byte records = 39 x 4-byte integers (int32) (big-endian). 
- First record: header (39 int32) with sentinel -1 in slots 34..38 to endian-check. 
- Then NOBJ object records, each 39 int32 with documented scaling. 

This implementation is robust for:
- gzip-compressed .dat (common in online mirrors)
- little-endian hosts (byteswap to native to avoid pandas/arrow endian errors)

Outputs:
- By default writes a compact parquet per plate with RA/Dec in degrees (B1950 frame).
- Use --full to write all 39 object ints plus derived ra/dec degrees and plate metadata.
"""

from __future__ import annotations

import argparse
import gzip
from pathlib import Path
from typing import BinaryIO, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


REC_INTS = 39
REC_BYTES = 156

# Object record column names (39 int32 slots) per README
OBJ_COLS = [
    "starnumO",
    "ra_b1950_sec_x1000",
    "dec_b1950_arcsec_x1000",
    "XsctO_ere",
    "YsctO_ere",
    "diaO_arcsec_x1000",
    "magiO_x1000",
    "magdO_x1000",
    "colori_x1000",
    "colord_x1000",
    "mean_sbO_x1000",
    "thetaO_deg_x1000",
    "ellO_x1000",
    "galnodO_x1000",
    "PsatO_x1000",
    "TavgO_x1000",
    "TskyO_x1000",
    "ReffO_x1000",
    "C42O_x1000",
    "C32O_x1000",
    "Mir1O_x1000",
    "Mir2O_x1000",
    "starnumE",
    "XsctE_ere",
    "YsctE_ere",
    "diaE_arcsec_x1000",
    "mean_sbE_x1000",
    "thetaE_deg_x1000",
    "ellE_x1000",
    "galnodE_x1000",
    "PsatE_x1000",
    "TavgE_x1000",
    "TskyE_x1000",   # README has a typo label; treat as E sky transmittance
    "ReffE_x1000",
    "C42E_x1000",
    "C32E_x1000",
    "Mir1E_x1000",
    "Mir2E_x1000",
    "flag",
]

# Header field indices (subset)
HDR_IDX = {
    "POSS_field": 0,
    "Emulsion": 1,
    "EpochDay": 2,
    "EpochMon": 3,
    "EpochYear": 4,
    "NOBJ": 5,
    "NSTARS": 6,
    "NGALAX": 7,
    "ramin_b1950_sec_x100": 8,
    "ramax_b1950_sec_x100": 9,
    "decmin_b1950_arcsec_x100": 10,
    "decmax_b1950_arcsec_x100": 11,
    "ra_rms_arcsec_x1000": 12,
    "dec_rms_arcsec_x1000": 13,
    "Omaglim_x100": 14,
    "Emaglim_x100": 15,
}

def is_gzip(path: Path) -> bool:
    with path.open("rb") as f:
        return f.read(2) == b"\x1f\x8b"

def open_maybe_gzip(path: Path) -> BinaryIO:
    return gzip.open(path, "rb") if is_gzip(path) else path.open("rb")

def parse_header_bytes(hdr_bytes: bytes) -> Tuple[str, np.ndarray]:
    """
    Interpret header bytes as both BE and LE int32 arrays and choose the one
    matching sentinel: header[34..38] == -1. 
    Returns (endian_char, header_int32_array)
    endian_char is '>' for big-endian or '<' for little-endian.
    """
    if len(hdr_bytes) != REC_BYTES:
        raise RuntimeError(f"Header read length {len(hdr_bytes)} != {REC_BYTES}")

    be = np.frombuffer(hdr_bytes, dtype=">i4", count=REC_INTS)
    le = np.frombuffer(hdr_bytes, dtype="<i4", count=REC_INTS)

    def ok(h: np.ndarray) -> bool:
        return h.size == REC_INTS and np.all(h[34:39] == -1)

    if ok(be):
        return ">", be
    if ok(le):
        return "<", le

    raise RuntimeError(
        "Header sentinel check failed for both endians. "
        f"BE[34:39]={be[34:39].tolist()} LE[34:39]={le[34:39].tolist()}"
    )

def header_dict(hdr: np.ndarray) -> dict:
    d = {k: int(hdr[i]) for k, i in HDR_IDX.items()}
    d["epoch_ymd"] = f"{d['EpochYear']:04d}-{d['EpochMon']:02d}-{d['EpochDay']:02d}"
    return d

def to_native_endian_int32(arr: np.ndarray) -> np.ndarray:
    """
    Convert to native-endian int32 to avoid pandas/arrow issues on LE systems.
    """
    # arr is int32 but may be byte-swapped (dtype.byteorder '>' or '<')
    native = np.dtype("=i4")
    if arr.dtype.byteorder in (">", "<") and arr.dtype.byteorder != native.byteorder:
        arr = arr.byteswap().view(arr.dtype.newbyteorder("="))
    return arr

def ra_dec_deg_from_scaled(ra_sec_x1000: np.ndarray, dec_arcsec_x1000: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """
    Convert:
      ra seconds *1000 (B1950) -> degrees
      dec arcseconds *1000 (B1950) -> degrees
    """
    ra_sec = ra_sec_x1000.astype(np.float64) / 1000.0
    dec_arcsec = dec_arcsec_x1000.astype(np.float64) / 1000.0
    ra_deg = ra_sec / 240.0
    dec_deg = dec_arcsec / 3600.0
    return ra_deg, dec_deg

def write_plate_parquet(
    path: Path,
    out_dir: Path,
    chunk_records: int = 500_000,
    full: bool = False,
) -> Path:
    """
    Decode one plate file to Parquet, streaming in chunks (works for gzip too).
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{path.stem}.parquet"

    with open_maybe_gzip(path) as fh:
        hdr_bytes = fh.read(REC_BYTES)
        endian, hdr = parse_header_bytes(hdr_bytes)
        hd = header_dict(hdr)
        nobj = hd["NOBJ"]

        # Prepare Parquet writer schema
        if full:
            # All 39 int32 + derived floats + plate metadata
            fields = [
                pa.field("POSS_field", pa.int32()),
                pa.field("Emulsion", pa.int32()),
                pa.field("epoch_ymd", pa.string()),
                pa.field("ra_b1950_deg", pa.float64()),
                pa.field("dec_b1950_deg", pa.float64()),
            ] + [pa.field(c, pa.int32()) for c in OBJ_COLS]
        else:
            # Compact schema for crossmatch/sieve work
            fields = [
                pa.field("POSS_field", pa.int32()),
                pa.field("Emulsion", pa.int32()),
                pa.field("epoch_ymd", pa.string()),
                pa.field("starnumO", pa.int32()),
                pa.field("starnumE", pa.int32()),
                pa.field("ra_b1950_deg", pa.float64()),
                pa.field("dec_b1950_deg", pa.float64()),
                pa.field("magdO_x1000", pa.int32()),
                pa.field("magiO_x1000", pa.int32()),
                pa.field("colori_x1000", pa.int32()),
                pa.field("galnodO_x1000", pa.int32()),
                pa.field("diaO_arcsec_x1000", pa.int32()),
                pa.field("diaE_arcsec_x1000", pa.int32()),
                pa.field("flag", pa.int32()),
            ]

        schema = pa.schema(fields)
        writer = pq.ParquetWriter(out_path, schema, compression="zstd")

        remaining = nobj
        plate_field = np.int32(hd["POSS_field"])
        emulsion = np.int32(hd["Emulsion"])
        epoch_ymd = hd["epoch_ymd"]

        try:
            while remaining > 0:
                take = min(chunk_records, remaining)
                need_bytes = take * REC_BYTES
                buf = fh.read(need_bytes)
                if len(buf) != need_bytes:
                    raise RuntimeError(
                        f"{path}: truncated while reading objects. "
                        f"Expected {need_bytes} bytes for {take} records, got {len(buf)}."
                    )

                arr = np.frombuffer(buf, dtype=f"{endian}i4")
                arr = to_native_endian_int32(arr)

                # reshape into (take, 39)
                arr = arr.reshape((take, REC_INTS))

                # Extract scaled RA/Dec cols (1 and 2)
                ra_deg, dec_deg = ra_dec_deg_from_scaled(arr[:, 1], arr[:, 2])

                if full:
                    # Build arrow columns
                    cols = {
                        "POSS_field": pa.array(np.full(take, plate_field, dtype=np.int32)),
                        "Emulsion": pa.array(np.full(take, emulsion, dtype=np.int32)),
                        "epoch_ymd": pa.array([epoch_ymd] * take),
                        "ra_b1950_deg": pa.array(ra_deg),
                        "dec_b1950_deg": pa.array(dec_deg),
                    }
                    for i, name in enumerate(OBJ_COLS):
                        cols[name] = pa.array(arr[:, i].astype(np.int32, copy=False))
                    table = pa.Table.from_pydict(cols, schema=schema)
                else:
                    # Compact selection by index
                    cols = {
                        "POSS_field": pa.array(np.full(take, plate_field, dtype=np.int32)),
                        "Emulsion": pa.array(np.full(take, emulsion, dtype=np.int32)),
                        "epoch_ymd": pa.array([epoch_ymd] * take),
                        "starnumO": pa.array(arr[:, 0].astype(np.int32, copy=False)),
                        "starnumE": pa.array(arr[:, 22].astype(np.int32, copy=False)),
                        "ra_b1950_deg": pa.array(ra_deg),
                        "dec_b1950_deg": pa.array(dec_deg),
                        "magdO_x1000": pa.array(arr[:, 7].astype(np.int32, copy=False)),
                        "magiO_x1000": pa.array(arr[:, 6].astype(np.int32, copy=False)),
                        "colori_x1000": pa.array(arr[:, 8].astype(np.int32, copy=False)),
                        "galnodO_x1000": pa.array(arr[:, 13].astype(np.int32, copy=False)),
                        "diaO_arcsec_x1000": pa.array(arr[:, 5].astype(np.int32, copy=False)),
                        "diaE_arcsec_x1000": pa.array(arr[:, 25].astype(np.int32, copy=False)),
                        "flag": pa.array(arr[:, 38].astype(np.int32, copy=False)),
                    }
                    table = pa.Table.from_pydict(cols, schema=schema)

                writer.write_table(table)
                remaining -= take

        finally:
            writer.close()

    return out_path

def main():
    ap = argparse.ArgumentParser(description="Decode MAPS P###.dat binary records to Parquet (handles gzip + endian).")
    ap.add_argument("--in", dest="inp", required=True, help="Input P###.dat file OR directory containing P*.dat")
    ap.add_argument("--out-dir", required=True, help="Output directory for Parquet")
    ap.add_argument("--chunk-records", type=int, default=500_000, help="Records per chunk (streaming). Default=500000")
    ap.add_argument("--full", action="store_true", help="Write full 39-int schema (default writes compact subset)")
    args = ap.parse_args()

    inp = Path(args.inp)
    out_dir = Path(args.out_dir)

    files = sorted(inp.glob("P*.dat")) if inp.is_dir() else [inp]
    if not files:
        raise SystemExit(f"No P*.dat files found under {inp}")

    for f in files:
        out = write_plate_parquet(f, out_dir, chunk_records=args.chunk_records, full=args.full)
        print(f"[OK] {f.name} -> {out}")

if __name__ == "__main__":
    main()
