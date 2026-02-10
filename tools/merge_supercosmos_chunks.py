#!/usr/bin/env python3
import sys, glob, os
import pandas as pd
import pyarrow as pa, pyarrow.parquet as pq

if len(sys.argv) != 2:
    print("usage: merge_supercosmos_chunks.py <OUTROOT>", file=sys.stderr)
    sys.exit(2)

outroot = sys.argv[1]
pattern = os.path.join(outroot, "flags_supercosmos__*.parquet")
paths = sorted(glob.glob(pattern))
if not paths:
    print(f"[ERR] no per-chunk files match {pattern}", file=sys.stderr)
    sys.exit(3)

dfs = []
for p in paths:
    try:
        dfs.append(pd.read_parquet(p))
    except Exception as e:
        print(f"[WARN] skip {p}: {e}", file=sys.stderr)

if not dfs:
    print("[ERR] no readable chunk files", file=sys.stderr); sys.exit(4)

df = pd.concat(dfs, ignore_index=True)
if "row_id" not in df.columns:
    print("[ERR] expected 'row_id' in chunk schema; got:", list(df.columns), file=sys.stderr)
    sys.exit(5)

# keep only the columns we promise downstream and de-dup by row_id
cols = [c for c in ["row_id", "is_supercosmos_artifact"] if c in df.columns]
df = df[cols].drop_duplicates(subset="row_id")

outp = os.path.join(outroot, "flags_supercosmos.parquet")
pq.write_table(pa.Table.from_pandas(df, preserve_index=False), outp)
print(f"[DONE] merged -> {outp} rows={len(df)} from chunks={len(paths)}")

