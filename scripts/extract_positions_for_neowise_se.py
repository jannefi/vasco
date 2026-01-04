
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Incremental extractor for (row_id, ra, dec) from a partitioned Parquet master dataset.
Writes only NEW/CHANGED parts into a subfolder (default: ./new) using a small manifest.
Optionally fans out NEOWISE-SE per-chunk runs just like the original script.

Compared to the original, key additions are:
- Manifest gating on Parquet part (mtime + size) -> skip unchanged parts
- New-only chunk folder (out_dir/<write_subdir>/positions_chunk_*.csv)
- Larger default chunk size (20000)
"""
import argparse, os, sys, hashlib, json
from pathlib import Path
from typing import List, Dict
import pandas as pd

DEFAULT_MANIFEST = './data/local-cats/tmp/positions_manifest.json'

def find_parquet_parts(root: Path):
    return sorted(root.rglob('*.parquet'))

def file_sig(p: Path) -> Dict:
    st = p.stat()
    return { 'size': st.st_size, 'mtime_ns': st.st_mtime_ns }

def load_manifest(path: Path) -> Dict[str, Dict]:
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            return {}
    return {}

def save_manifest(path: Path, data: Dict[str, Dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(path)

def autodetect_columns(df: pd.DataFrame):
    ra_cands = ["ALPHAWIN_J2000","ALPHA_J2000","X_WORLD","alpha","ra"]
    de_cands = ["DELTAWIN_J2000","DELTA_J2000","Y_WORLD","delta","dec"]
    ra_col = next((c for c in ra_cands if c in df.columns), None)
    de_col = next((c for c in de_cands if c in df.columns), None)
    has_row_id = 'row_id' in df.columns
    tile_col = next((c for c in ("tile_id","tile","tile_name") if c in df.columns), None)
    return ra_col, de_col, has_row_id, tile_col

import hashlib

def stable_row_id(tile_id: str, local_index: int) -> int:
    h = hashlib.sha1(f"{tile_id}:{local_index}".encode('utf-8')).digest()
    return int.from_bytes(h[:8], byteorder='big', signed=False)

def load_positions_from_part(part_path: Path) -> pd.DataFrame:
    df = pd.read_parquet(part_path)
    ra_col, de_col, has_row_id, tile_col = autodetect_columns(df)
    if ra_col is None or de_col is None:
        raise RuntimeError(f"Could not find RA/Dec columns in {part_path}")
    out = pd.DataFrame({ 'ra': pd.to_numeric(df[ra_col], errors='coerce').astype('float64'),
                         'dec': pd.to_numeric(df[de_col], errors='coerce').astype('float64') })
    if has_row_id:
        out['row_id'] = pd.to_numeric(df['row_id'], errors='coerce').astype('Int64').astype('int64')
    else:
        if tile_col is not None:
            local_idx = pd.RangeIndex(start=0, stop=len(out), step=1)
            tiles = df[tile_col].astype(str).fillna('unknown')
            if tiles.nunique() == 1:
                tconst = tiles.iloc[0]
                out['row_id'] = [stable_row_id(tconst, i) for i in local_idx]
            else:
                out['row_id'] = [stable_row_id(tiles.iloc[i], i) for i in local_idx]
        else:
            out['row_id'] = pd.RangeIndex(start=0, stop=len(out), step=1).astype('int64')
    out = out.dropna(subset=['ra','dec']).reset_index(drop=True)
    return out[['row_id','ra','dec']]

def write_chunks(df_all: pd.DataFrame, out_dir: Path, chunk_size: int, subdir: str) -> List[Path]:
    target = out_dir / subdir
    target.mkdir(parents=True, exist_ok=True)
    chunks = []
    counter = 1
    for start in range(0, len(df_all), chunk_size):
        chunk = df_all.iloc[start:start+chunk_size]
        fname = target / f"positions_chunk_{counter:05d}.csv"
        chunk[['row_id','ra','dec']].to_csv(fname, index=False)
        chunks.append(fname)
        counter += 1
    return chunks

def run_neowise_per_chunk(neowise_script: Path, chunk_path: Path, out_dir: Path,
                          radius_arcsec=5.0, mjd_cap=59198, snr=5.0, chunk_size=20000, sleep=1.0):
    import subprocess, sys
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = chunk_path.stem.replace('positions_chunk_', 'neowise_se_matches_')
    out_csv = out_dir / f"{stem}.csv"
    cmd = [sys.executable, str(neowise_script),
           '--in-csv', str(chunk_path), '--out-csv', str(out_csv),
           '--radius-arcsec', str(radius_arcsec), '--mjd-cap', str(mjd_cap),
           '--snr', str(snr), '--chunk-size', str(chunk_size), '--sleep', str(sleep)]
    subprocess.run(cmd, check=True)
    return out_csv

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--parquet-root', required=True)
    ap.add_argument('--out-dir', required=True)
    ap.add_argument('--chunk-size', type=int, default=20000)
    ap.add_argument('--write-subdir', default='new')
    ap.add_argument('--manifest', default=DEFAULT_MANIFEST)
    ap.add_argument('--full-rescan', action='store_true', help='Ignore manifest and write all chunks into write-subdir')
    ap.add_argument('--run-neowise', action='store_true')
    ap.add_argument('--neowise-script', default='./scripts/xmatch_neowise_single_exposure.py')
    ap.add_argument('--neowise-out-dir', default='./data/local-cats/out/neowise_se')
    ap.add_argument('--radius-arcsec', type=float, default=5.0)
    ap.add_argument('--mjd-cap', type=int, default=59198)
    ap.add_argument('--snr', type=float, default=5.0)
    ap.add_argument('--sleep', type=float, default=1.0)
    args = ap.parse_args()

    root = Path(args.parquet_root)
    out_dir = Path(args.out_dir)
    man_path = Path(args.manifest)
    manifest = {} if args.full_rescan else load_manifest(man_path)

    parts = find_parquet_parts(root)
    if not parts:
        raise SystemExit(f"No parquet files found under {root}")

    changed_parts = []
    for p in parts:
        key = str(p.relative_to(root))
        sig = file_sig(p)
        if key not in manifest or manifest[key].get('size') != sig['size'] or manifest[key].get('mtime_ns') != sig['mtime_ns']:
            changed_parts.append(p)
    if args.full_rescan:
        changed_parts = parts

    if not changed_parts:
        print('[INFO] No changes detected; nothing to write.')
        return

    frames = []
    for p in changed_parts:
        try:
            frames.append(load_positions_from_part(p))
        except Exception as e:
            print(f"[WARN] Skipping {p}: {e}", file=sys.stderr)
    if not frames:
        raise SystemExit('No positions could be extracted from changed parts')

    df_all = pd.concat(frames, ignore_index=True)
    if 'row_id' in df_all.columns:
        df_all = df_all.drop_duplicates(subset=['row_id']).reset_index(drop=True)

    chunks = write_chunks(df_all, out_dir, args.chunk_size, args.write_subdir)
    print(f"[INFO] Wrote {len(chunks)} positions chunk(s) to {out_dir / args.write_subdir}")

    # Update manifest entries only for changed parts
    for p in changed_parts:
        key = str(p.relative_to(root))
        manifest[key] = file_sig(p)
    save_manifest(man_path, manifest)

    if args.run_neowise:
        neo_out = Path(args.neowise_out_dir)
        produced = []
        for c in chunks:
            out_csv = run_neowise_per_chunk(Path(args.neowise_script), Path(c), neo_out,
                                            radius_arcsec=args.radius_arcsec,
                                            mjd_cap=args.mjd_cap, snr=args.snr,
                                            chunk_size=args.chunk_size, sleep=args.sleep)
            produced.append(out_csv)
            print(f"[INFO] TAP results -> {out_csv}")
        print(f"[INFO] Completed NEOWISE-SE runs for {len(produced)} chunk(s).")

if __name__ == '__main__':
    main()
