#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import argparse
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.compute as pc

from astropy.coordinates import SkyCoord
import astropy.units as u


def convert_one(in_parquet: Path, out_parquet: Path, ra_col="ra_b1950_deg", dec_col="dec_b1950_deg", chunk_rows=500_000):
    pf = pq.ParquetFile(in_parquet)
    out_parquet.parent.mkdir(parents=True, exist_ok=True)

    writer = None
    try:
        for batch in pf.iter_batches(batch_size=chunk_rows):
            tab = pa.Table.from_batches([batch])

            ra = tab[ra_col].to_numpy(zero_copy_only=False)
            dec = tab[dec_col].to_numpy(zero_copy_only=False)

            # FK4 B1950 -> ICRS
            c = SkyCoord(ra=ra*u.deg, dec=dec*u.deg, frame="fk4", equinox="B1950")
            icrs = c.icrs

            tab = tab.append_column("ra_icrs_deg", pa.array(icrs.ra.deg.astype(np.float64)))
            tab = tab.append_column("dec_icrs_deg", pa.array(icrs.dec.deg.astype(np.float64)))

            if writer is None:
                writer = pq.ParquetWriter(out_parquet, tab.schema, compression="zstd")
            writer.write_table(tab)
    finally:
        if writer is not None:
            writer.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-dir", required=True, help="Input MAPS parquet_by_plate directory")
    ap.add_argument("--out-dir", required=True, help="Output directory for ICRS parquets")
    ap.add_argument("--chunk-rows", type=int, default=500_000)
    args = ap.parse_args()

    in_dir = Path(args.in_dir)
    out_dir = Path(args.out_dir)

    files = sorted(in_dir.glob("P*.parquet"))
    if not files:
        raise SystemExit(f"No P*.parquet files found under {in_dir}")

    for f in files:
        out = out_dir / f.name
        convert_one(f, out, chunk_rows=args.chunk_rows)
        print(f"[OK] {f.name} -> {out}")

if __name__ == "__main__":
    main()
