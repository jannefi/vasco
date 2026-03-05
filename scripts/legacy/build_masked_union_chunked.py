
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_masked_union_chunked.py

Chunked builder for the masked survivors dataset.

Why this script?
  - The strict survivors are partitioned (e.g., ra_bin=*/dec_bin=*), and loading
    them all at once can OOM. This tool processes one partition file at a time,
    writing a mirrored partitioned output tree.
  - Flag tables (VOSA-like, SCOS, PTF ngood, VSX, SkyBoT) are reduced to
    compact in-memory key indices so per-partition lookups are O(1) and memory
    stays bounded (≈ a few million keys at most).

Inputs
  --survivors-root DIR
      Partitioned Parquet dataset root, e.g.:
      ./data/vasco-candidates/post16/candidates_final_core_dataset_20260205_170455

  --out-root DIR
      Output root directory for the masked survivors (mirrors partitioning).

Flag inputs (optional; pass any that exist)
  --vosa-like PATH          Parquet file/dir with ≤5" matches (drop)
  --scos PATH               Parquet file/dir with cross-digitization artefacts (drop)
  --ptf-ngood PATH          Parquet file/dir with ngood>0 rows (drop)
  --vsx PATH                Parquet file/dir with VSX matches (drop)
  --skybot PATH             Parquet file/dir OR a directory containing parts/*.parquet
                            Only strict/5" matches should cause drop; wide labels are ignored here.

Other options
  --ra-bin LIST             Comma-separated subset of ra_bin values to process (optional)
  --dec-bin LIST            Comma-separated subset of dec_bin values to process (optional)
  --dry-run                 Plan only; don’t write outputs
  --max-rows INT            For testing: stop after processing this many rows total

Output
  A partitioned Parquet dataset at --out-root, mirroring ra_bin/dec_bin layout.
  A summary JSON file (masked_union_metrics.json) with totals per gate/overall.

"""
import argparse
import json
from pathlib import Path
from typing import Dict, Set, Tuple, Optional
import pandas as pd

# ------------------------ utilities ---------------------------------------

def list_partitions(root: Path, ra_subset=None, dec_subset=None):
    # Expect files at root/ra_bin=*/dec_bin=*/part-*.parquet
    for ra_dir in sorted(root.glob('ra_bin=*')):
        ra_val = ra_dir.name.split('=',1)[1]
        if ra_subset and ra_val not in ra_subset:
            continue
        for dec_dir in sorted(ra_dir.glob('dec_bin=*')):
            dec_val = dec_dir.name.split('=',1)[1]
            if dec_subset and dec_val not in dec_subset:
                continue
            for f in sorted(dec_dir.glob('*.parquet')):
                yield ra_val, dec_val, f


def read_parquet_any(path: Path, columns=None) -> pd.DataFrame:
    if path.is_dir():
        try:
            return pd.read_parquet(path, columns=columns, engine='pyarrow')
        except Exception:
            parts = sorted(path.glob('**/*.parquet'))
            dfs = [pd.read_parquet(p, columns=columns) for p in parts]
            return pd.concat(dfs, ignore_index=True)
    else:
        return pd.read_parquet(path, columns=columns)

# ------------------------ flag index builders -----------------------------

def _keys_from_df(df: pd.DataFrame) -> Tuple[Dict[str, Set[int]], Set[int]]:
    """Return (tile_to_numbers, numbers_only) where tile_to_numbers maps tile_id->set(NUMBER)
    and numbers_only is used when tile_id is absent in survivors.
    NUMBER is assumed integer-like where possible; we store as Python int to lower memory.
    """
    tile_map: Dict[str, Set[int]] = {}
    num_set: Set[int] = set()
    has_tile = 'tile_id' in df.columns
    has_num  = 'NUMBER' in df.columns
    if not has_num:
        return tile_map, num_set
    if has_tile:
        for tile, sub in df[['tile_id','NUMBER']].dropna().astype({'NUMBER':'int64'}).groupby('tile_id'):
            tile_map.setdefault(str(tile), set()).update(sub['NUMBER'].tolist())
    else:
        num_set.update(df['NUMBER'].dropna().astype('int64').tolist())
    return tile_map, num_set


def build_flag_index(path: Optional[str], label: str, columns_hint=None) -> Tuple[Dict[str, Set[int]], Set[int]]:
    if not path:
        return {}, set()
    p = Path(path)
    if not p.exists():
        print(f"[WARN] {label}: path not found {p} — skipping")
        return {}, set()
    cols = ['tile_id','NUMBER']
    if columns_hint:
        cols = sorted(set(cols + columns_hint))
    try:
        df = read_parquet_any(p, columns=cols)
    except Exception as e:
        print(f"[WARN] {label}: read failed at {p}: {e} — trying without column pruning")
        df = read_parquet_any(p)
    print(f"[INFO] {label}: loaded {len(df):,} rows for key index")
    return _keys_from_df(df)


# ----------------------------- per-chunk filter ---------------------------

def in_flag(tile: Optional[str], num: Optional[int], tmap: Dict[str, Set[int]], nset: Set[int]) -> bool:
    if num is None:
        return False
    if tmap and tile is not None and tile in tmap:
        return num in tmap[tile]
    if nset:
        return num in nset
    return False


def process_partition(src_file: Path,
                      out_root: Path,
                      vosa_idx, scos_idx, ptf_idx, vsx_idx, sky_idx,
                      max_rows_left: Optional[int]=None,
                      dry_run=False) -> Tuple[int,int,dict]:
    # read minimal columns; allow extra commonly used identifiers
    cols = ['tile_id','NUMBER','plate_id','RA','Dec','RA_corr','Dec_corr','ALPHAWIN_J2000','DELTAWIN_J2000']
    df = pd.read_parquet(src_file, columns=[c for c in cols if c in pd.read_parquet(src_file, columns=[]).columns])

    # Early exit for test limit
    if max_rows_left is not None and len(df) > max_rows_left:
        df = df.iloc[:max_rows_left].copy()

    # Normalize NUMBER dtype
    if 'NUMBER' in df.columns:
        df['NUMBER'] = pd.to_numeric(df['NUMBER'], errors='coerce').astype('Int64')

    # compute drops
    def hit(label, idx_pair):
        tmap, nset = idx_pair
        return df.apply(lambda r: in_flag(str(r['tile_id']) if 'tile_id' in df.columns else None,
                                          int(r['NUMBER']) if pd.notna(r['NUMBER']) else None,
                                          tmap, nset), axis=1)

    h_vosa = hit('vosa_like', vosa_idx) if any(vosa_idx) else pd.Series(False, index=df.index)
    h_scos = hit('scos',      scos_idx) if any(scos_idx) else pd.Series(False, index=df.index)
    h_ptf  = hit('ptf_ngood', ptf_idx)  if any(ptf_idx)  else pd.Series(False, index=df.index)
    h_vsx  = hit('vsx',       vsx_idx)  if any(vsx_idx)  else pd.Series(False, index=df.index)
    h_sky  = hit('skybot',    sky_idx)  if any(sky_idx)  else pd.Series(False, index=df.index)

    drop_any = (h_vosa | h_scos | h_ptf | h_vsx | h_sky)
    kept = df.loc[~drop_any].copy()

    # write
    written = 0
    if not dry_run:
        # mirror partition path
        ra_dir = src_file.parent.parent.name  # ra_bin=*
        dec_dir = src_file.parent.name        # dec_bin=*
        out_dir = out_root / ra_dir / dec_dir
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / src_file.name
        kept.to_parquet(out_path, index=False)
        written = len(kept)
    stats = {
        'src_rows': int(len(df)),
        'kept_rows': int(len(kept)),
        'drop_vosa_like': int(h_vosa.sum()),
        'drop_scos': int(h_scos.sum()),
        'drop_ptf_ngood': int(h_ptf.sum()),
        'drop_vsx': int(h_vsx.sum()),
        'drop_skybot': int(h_sky.sum()),
    }
    return len(df), written, stats


# ----------------------------- main ---------------------------------------

def main():
    ap = argparse.ArgumentParser(description='Chunked masked survivors builder (partition-safe)')
    ap.add_argument('--survivors-root', required=True)
    ap.add_argument('--out-root', required=True)
    ap.add_argument('--vosa-like', default='')
    ap.add_argument('--scos', default='')
    ap.add_argument('--ptf-ngood', default='')
    ap.add_argument('--vsx', default='')
    ap.add_argument('--skybot', default='')
    ap.add_argument('--ra-bin', default='')
    ap.add_argument('--dec-bin', default='')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--max-rows', type=int, default=0)
    args = ap.parse_args()

    survivors_root = Path(args.survivors_root)
    out_root = Path(args.out_root)

    ra_subset = set(x.strip() for x in args.ra_bin.split(',') if x.strip()) or None
    dec_subset = set(x.strip() for x in args.dec_bin.split(',') if x.strip()) or None

    # Build compact indices for flag datasets (tile_id -> set(NUMBER))
    print('[INFO] Building flag indices...')
    vosa_idx = build_flag_index(args.vosa_like, 'VOSA-like')
    scos_idx = build_flag_index(args.scos, 'SCOS')
    ptf_idx  = build_flag_index(args.ptf_ngood, 'PTF ngood')
    vsx_idx  = build_flag_index(args.vsx, 'VSX')
    sky_idx  = build_flag_index(args.skybot, 'SkyBoT')

    total_src = total_kept = 0
    agg = {'drop_vosa_like':0,'drop_scos':0,'drop_ptf_ngood':0,'drop_vsx':0,'drop_skybot':0}

    max_rows_left = args.max_rows if args.max_rows>0 else None

    for ra, dec, f in list_partitions(survivors_root, ra_subset, dec_subset):
        if max_rows_left is not None and max_rows_left <= 0:
            break
        src_n, kept_n, st = process_partition(
            f, out_root, vosa_idx, scos_idx, ptf_idx, vsx_idx, sky_idx,
            max_rows_left=max_rows_left, dry_run=args.dry_run
        )
        total_src += src_n; total_kept += kept_n
        for k in agg: agg[k] += st[k]
        if max_rows_left is not None:
            max_rows_left -= src_n
        print(f"[OK] {f} -> kept={kept_n}/{src_n}; drops: VOSA={st['drop_vosa_like']}, SCOS={st['drop_scos']}, PTF={st['drop_ptf_ngood']}, VSX={st['drop_vsx']}, SKY={st['drop_skybot']}")

    # write summary
    summary = {
        'survivors_root': str(survivors_root.resolve()),
        'out_root': str(out_root.resolve()),
        'total_src_rows': int(total_src),
        'total_kept_rows': int(total_kept),
        **agg
    }
    if not args.dry_run:
        with open(out_root / 'masked_union_metrics.json', 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2)
    print('[SUMMARY]', json.dumps(summary, indent=2))

if __name__ == '__main__':
    main()
