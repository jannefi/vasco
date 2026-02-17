#!/usr/bin/env python3
from __future__ import annotations
import argparse, csv
from pathlib import Path

PRED_COLS = [
    "row_id","NUMBER","tile_id","plate_id","date_obs_iso",
    "has_vosa_like_match","is_supercosmos_artifact","ptf_match_ngood",
    "is_known_variable_or_transient","skybot_strict","skybot_wide",
]
COORD_PREF = [
    ("RA_row","Dec_row"),
    ("RA","Dec"),
    ("ra","dec"),
]

def pick_pair(cols: set[str]):
    for ra, dec in COORD_PREF:
        if ra in cols and dec in cols:
            return ra, dec
    return None, None

def truthy(v):
    if v is None: return False
    try:
        # pandas/pyarrow may give numpy bool, int, etc.
        if isinstance(v, bool): return v
        if isinstance(v, (int, float)): return v != 0
        s = str(v).strip().lower()
        return s in ("true","1","t","yes","y")
    except Exception:
        return False

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--masked-root", default="./work/survivors_masked_union")
    ap.add_argument("--out-prefix", default="./data/vasco-candidates/post16/survivors_R_like_inclusive.provisional")
    ap.add_argument("--require-skybot", action="store_true", help="Fail if skybot_strict missing")
    args = ap.parse_args()

    root = Path(args.masked_root)
    out_prefix = Path(args.out_prefix)
    out_prefix.parent.mkdir(parents=True, exist_ok=True)

    files = sorted(root.rglob("*.parquet"))
    if not files:
        raise SystemExit(f"[ERROR] no parquet files under {root}")

    import pyarrow.parquet as pq
    import pyarrow as pa

    # Determine schema & coord columns from first good file
    good0 = None
    for f in files:
        try:
            sch = pq.ParquetFile(f).schema_arrow
            if len(sch) > 0:
                good0 = f
                cols0 = set([x.name for x in sch])
                break
        except Exception:
            continue
    if good0 is None:
        raise SystemExit("[ERROR] no readable parquet with columns found")

    ra_col, dec_col = pick_pair(cols0)
    if not ra_col:
        raise SystemExit(f"[ERROR] no usable RA/Dec columns found; available: {sorted(cols0)[:40]}...")

    if args.require_skybot and "skybot_strict" not in cols0:
        raise SystemExit("[ERROR] skybot_strict missing but --require-skybot set")

    keep_cols = [c for c in PRED_COLS if c in cols0] + [ra_col, dec_col]
    # ensure unique
    keep_cols = list(dict.fromkeys(keep_cols))

    out_csv = out_prefix.with_suffix(".csv")
    out_parq = out_prefix.with_suffix(".parquet")

    # Stream write
    writer = None
    wrote_header = False
    n_out = 0

    with out_csv.open("w", newline="") as fcsv:
        wcsv = None

        for fp in files:
            # Skip bad/empty-schema parquet files safely
            try:
                pf = pq.ParquetFile(fp)
                if len(pf.schema_arrow) <= 0:
                    continue
            except Exception:
                continue

            # iterate batches to keep memory bounded
            for batch in pf.iter_batches(batch_size=50000, columns=keep_cols):
                tbl = pa.Table.from_batches([batch])
                df = tbl.to_pandas()

                # remainder predicate
                hv = df.get("has_vosa_like_match", False)
                hs = df.get("is_supercosmos_artifact", False)
                ptf = df.get("ptf_match_ngood", 0)
                vx = df.get("is_known_variable_or_transient", False)
                sb = df.get("skybot_strict", False)

                # normalize to booleans
                mask = []
                for i in range(len(df)):
                    ok = True
                    if truthy(hv.iloc[i] if hasattr(hv, "iloc") else hv): ok = False
                    if truthy(hs.iloc[i] if hasattr(hs, "iloc") else hs): ok = False
                    # ptf: accept 0/False as pass
                    vptf = ptf.iloc[i] if hasattr(ptf, "iloc") else ptf
                    if truthy(vptf): ok = False
                    if truthy(vx.iloc[i] if hasattr(vx, "iloc") else vx): ok = False
                    if truthy(sb.iloc[i] if hasattr(sb, "iloc") else sb): ok = False
                    mask.append(ok)

                df2 = df.loc[mask].copy()
                if df2.empty:
                    continue

                # Standardize coordinate column names to RA/Dec in outputs
                df2 = df2.rename(columns={ra_col: "RA", dec_col: "Dec"})

                # CSV header once
                if not wrote_header:
                    wcsv = csv.writer(fcsv)
                    wcsv.writerow(list(df2.columns))
                    wrote_header = True

                # Write CSV rows
                for row in df2.itertuples(index=False, name=None):
                    wcsv.writerow(row)

                # Write Parquet incrementally
                t2 = pa.Table.from_pandas(df2, preserve_index=False)
                if writer is None:
                    writer = pq.ParquetWriter(out_parq, t2.schema, compression="zstd")
                writer.write_table(t2)

                n_out += len(df2)

    if writer:
        writer.close()

    print(f"[OK] wrote {n_out} rows")
    print(f"[OK] CSV: {out_csv}")
    print(f"[OK] Parquet: {out_parq}")

if __name__ == "__main__":
    main()
