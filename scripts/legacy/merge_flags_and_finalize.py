#!/usr/bin/env python3
"""
Merge flags from _master_optical_parquet_flags into master; write a temporary annotated Parquet.
Then reuse existing Post 1.6 steps (post16_counts / post16_strict).
"""
import argparse
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd

FLAG_FILES = (
    'flags_vsx.parquet',
    'flags_supercosmos.parquet',
    'flags_ptf_objects.parquet',
    'flags_skybot.parquet',
)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--master', required=True)
    ap.add_argument('--flags-root', required=True)
    ap.add_argument('--irflags', required=True)
    ap.add_argument('--out', required=True)
    args = ap.parse_args()
    ds_master = ds.dataset(args.master, format='parquet')
    master_df = pd.concat([frag.to_table().to_pandas() for frag in ds_master.get_fragments()], ignore_index=True)
    # Join IR flags
    ir = pq.read_table(args.irflags).to_pandas()
    key = next((c for c in ('NUMBER','row_id','source_id') if c in master_df.columns and c in ir.columns), 'NUMBER')
    out_df = master_df.merge(ir[[key,'has_ir_match']], on=key, how='left')
    out_df['has_ir_match'] = out_df['has_ir_match'].fillna(False)
    # Join other flags if present
    for fname in FLAG_FILES:
        fpath = pathlib.Path(args.flags_root)/fname
        if fpath.exists():
            ff = pq.read_table(fpath).to_pandas()
            out_df = out_df.merge(ff, on=key, how='left')
    for col in ('is_supercosmos_artifact','is_skybot','is_known_variable_or_transient'):
        if col not in out_df.columns:
            out_df[col] = False
        else:
            out_df[col] = out_df[col].fillna(False)
    pq.write_table(pa.Table.from_pandas(out_df, preserve_index=False), args.out)
    print('[OK] Annotated written:', args.out, 'rows=', len(out_df))

if __name__ == '__main__':
    main()