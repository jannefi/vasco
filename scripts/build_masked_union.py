
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_masked_union.py

Purpose
    Build the "masked survivors" dataset (survivors after applying union gates)
    to feed export_r_like.py. Gates supported: VOSA-like (IR/UV), SCOS, PTF ngood,
    VSX, and optional SkyBoT. Each gate is optional; when absent, it's skipped.

Inputs
    --survivors PATH
        Parquet dataset/dir with strict survivors BEFORE union gating (e.g.,
        ./data/vasco-candidates/post16/candidates_final_core_dataset_*/)

    --out PATH
        Output Parquet path/dir to write the masked survivors (kept rows).

Flag inputs (optional; pass any that exist)
    --vosa-like PATH          Parquet file/dir with matches (≤5")
    --scos PATH               Parquet file/dir with cross-digitization artefact hits
    --ptf-ngood PATH          Parquet file/dir with PTF rows deemed good (ngood>0)
    --vsx PATH                Parquet file/dir with VSX matches
    --skybot PATH             Parquet file/dir OR a directory containing parts/*.parquet

Conventions & heuristics
    - Joins are done on (tile_id, NUMBER) when both columns exist; else we fall
      back to NUMBER alone. Presence in a flag table implies a hit unless a more
      explicit column exists (e.g., has_ir_match, ngood, matched_5as).
    - VOSA-like: columns {has_ir_match|has_match|flag} → drop when True or when
      row is present.
    - SCOS: presence implies artefact → drop.
    - PTF: columns {ngood, ngoodobs} > 0 → drop; presence alone => drop.
    - VSX: presence → drop.
    - SkyBoT: columns {matched_5as|is_skybot_strict} True → drop. If only
      wide/proximity is present, we do NOT drop (label-only behavior).

Outputs
    - A Parquet dataset with the subset of survivors that remain after all gates.
    - Adds convenience booleans for each gate (e.g., _hit_vosa_like, _hit_scos, ...)
      so downstream reports can tally drops.

"""
import argparse
from pathlib import Path
import sys
import pandas as pd

# -------------------------- IO helpers ------------------------------------

def read_parquet_any(path: str) -> pd.DataFrame:
    p = Path(path)
    if p.is_dir():
        # try directory read
        try:
            return pd.read_parquet(p, engine='pyarrow')
        except Exception:
            # concat *.parquet
            parts = sorted(p.glob('**/*.parquet'))
            if not parts:
                raise FileNotFoundError(f'No parquet files under directory: {p}')
            dfs = [pd.read_parquet(pp) for pp in parts]
            return pd.concat(dfs, ignore_index=True)
    return pd.read_parquet(p)


def read_optional(path: str, label: str) -> pd.DataFrame | None:
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        print(f"[WARN] {label} path not found: {p} — skipping", file=sys.stderr)
        return None
    try:
        df = read_parquet_any(str(p))
        print(f"[INFO] Loaded {label}: {len(df):,} rows from {p}")
        return df
    except Exception as e:
        print(f"[WARN] Failed to read {label} at {p}: {e} — skipping", file=sys.stderr)
        return None


# ------------------------ join key selection -------------------------------

def pick_keys(df: pd.DataFrame) -> list[str]:
    keys = []
    if 'tile_id' in df.columns: keys.append('tile_id')
    if 'NUMBER' in df.columns: keys.append('NUMBER')
    # common fallback
    if not keys and 'row_id' in df.columns:
        keys.append('row_id')
    return keys


def join_flags(base: pd.DataFrame, flags: pd.DataFrame, label: str) -> pd.DataFrame:
    if flags is None or flags.empty:
        base[label] = False
        return base
    lk = pick_keys(base)
    rk = [k for k in lk if k in flags.columns]
    if not rk:
        # fall back to NUMBER only if present in both
        if 'NUMBER' in base.columns and 'NUMBER' in flags.columns:
            lk, rk = ['NUMBER'], ['NUMBER']
        else:
            print(f"[WARN] No compatible join keys for {label}; skipping merge", file=sys.stderr)
            base[label] = False
            return base
    merged = base.merge(flags, left_on=lk, right_on=rk, how='left', suffixes=('', f'__{label}'))

    # infer hit boolean from typical columns or presence
    hit = None
    for cand in ['has_ir_match', 'has_match', 'flag', 'is_match', 'is_scos', 'is_vsx', 'is_ptf', 'matched_5as', 'is_skybot_strict']:
        col = f'{cand}'
        if col in merged.columns:
            hit = merged[col]
            break
    if hit is None:
        # PTF special: ngood / ngoodobs
        for cand in ['ngood', 'ngoodobs']:
            if cand in merged.columns:
                hit = merged[cand].fillna(0).astype('float') > 0
                break
    if hit is None:
        # presence heuristic: any non-null in right side columns → hit
        rcols = [c for c in merged.columns if c.endswith(f'__{label}')]
        if rcols:
            any_nonnull = merged[rcols].notna().any(axis=1)
            hit = any_nonnull
        else:
            hit = pd.Series(False, index=merged.index)

    merged[f'_hit_{label}'] = hit.fillna(False).astype(bool)

    # drop right columns to keep base schema lean
    rcols = [c for c in merged.columns if c.endswith(f'__{label}')]
    merged = merged.drop(columns=rcols, errors='ignore')
    return merged


def main():
    ap = argparse.ArgumentParser(description='Build masked survivors (union gates applied)')
    ap.add_argument('--survivors', required=True, help='Path to strict survivors parquet (dir/file)')
    ap.add_argument('--out', required=True, help='Output Parquet path for masked survivors')
    ap.add_argument('--vosa-like', default='')
    ap.add_argument('--scos', default='')
    ap.add_argument('--ptf-ngood', default='')
    ap.add_argument('--vsx', default='')
    ap.add_argument('--skybot', default='')

    args = ap.parse_args()

    base = read_parquet_any(args.survivors)
    print(f"[INFO] Survivors (input): {len(base):,} rows")

    vosa = read_optional(args.vosa_like, 'VOSA-like')
    scos = read_optional(args.scos, 'SCOS')
    ptf  = read_optional(args.ptf_ngood, 'PTF ngood')
    vsx  = read_optional(args.vsx, 'VSX')
    sky  = read_optional(args.skybot, 'SkyBoT')

    df = base.copy()
    df = join_flags(df, vosa, 'vosa_like')
    df = join_flags(df, scos, 'scos')
    df = join_flags(df, ptf, 'ptf_ngood')
    df = join_flags(df, vsx, 'vsx')
    df = join_flags(df, sky, 'skybot')

    # keep rows that are NOT hits in any gate (note: SkyBoT wide/proximity not handled as drop here)
    drop_cols = [c for c in df.columns if c.startswith('_hit_')]
    keep_mask = ~df[drop_cols].any(axis=1) if drop_cols else pd.Series(True, index=df.index)
    kept = df.loc[keep_mask].copy()
    print(f"[INFO] After union gating: kept {len(kept):,} / {len(df):,}")

    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)
    kept.to_parquet(outp, index=False)
    print(f"[OK] Wrote masked survivors to {outp}")

if __name__ == '__main__':
    main()
