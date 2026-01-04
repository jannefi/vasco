
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build NEOWISE-SE IR sidecar and global flags parquet from per-chunk '*_closest.csv'.
Adds optional incremental mode with an ingest manifest. Default remains full rebuild.
"""
import argparse, glob, math, hashlib, json
from pathlib import Path
from typing import List, Tuple, Dict
import numpy as np
import pandas as pd

DEF_MANIFEST = './data/local-cats/_master_optical_parquet_irflags/_closest_manifest.json'

def to_float32(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors='coerce').astype('float32')

def to_int16(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors='coerce').astype('Int16')

def to_int64_nullable(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors='coerce').astype('Int64')

def compute_bins(in_ra: pd.Series, in_dec: pd.Series, bin_deg: int) -> Tuple[pd.Series,pd.Series]:
    ra = pd.to_numeric(in_ra, errors='coerce')
    dec = pd.to_numeric(in_dec, errors='coerce')
    valid = ra.notna() & dec.notna()
    ra_bin = pd.Series([pd.NA]*len(ra), dtype='Int32')
    dec_bin = pd.Series([pd.NA]*len(dec), dtype='Int32')
    if valid.any():
        ra_v = (ra[valid].to_numpy() % 360.0).astype(np.float64)
        dec_v = dec[valid].to_numpy(dtype=np.float64)
        ra_bin_vals = (np.floor_divide(ra_v, bin_deg) * bin_deg).astype(np.int32)
        dec_bin_vals = (np.floor_divide(dec_v + 90.0, bin_deg) * bin_deg - 90).astype(np.int32)
        ra_bin.loc[valid] = ra_bin_vals
        dec_bin.loc[valid] = dec_bin_vals
    return ra_bin, dec_bin

def collect_closest_paths(closest_dir: Path) -> List[Path]:
    files = sorted(glob.glob(str(closest_dir / '*_closest.csv')))
    return [Path(f) for f in files]

def sha1_file(p: Path, block=65536) -> str:
    h = hashlib.sha1()
    with p.open('rb') as f:
        while True:
            b = f.read(block)
            if not b: break
            h.update(b)
    return h.hexdigest()

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

def load_and_normalize(f: Path) -> pd.DataFrame:
    df = pd.read_csv(f, dtype={'row_id':'string'})
    for col in ("in_ra","in_dec","ra","dec","mjd","w1snr","w2snr","qi_fact","saa_sep","sep_deg","sep_arcsec"):
        if col in df.columns:
            df[col] = to_float32(df[col])
    if 'qual_frame' in df.columns:
        df['qual_frame'] = to_int16(df['qual_frame'])
    if 'cntr' in df.columns:
        df['cntr'] = to_int64_nullable(df['cntr'])
    if 'moon_masked' in df.columns:
        df['moon_masked'] = df['moon_masked'].astype('string')
    if 'sep_arcsec' not in df.columns or df['sep_arcsec'].isna().all():
        if 'sep_deg' in df.columns:
            df['sep_arcsec'] = df['sep_deg'] * 3600.0
        elif {'in_ra','in_dec','ra','dec'}.issubset(df.columns):
            ra1 = np.deg2rad(df['in_ra'].astype('float64').to_numpy())
            dec1 = np.deg2rad(df['in_dec'].astype('float64').to_numpy())
            ra2 = np.deg2rad(df['ra'].astype('float64').to_numpy())
            dec2 = np.deg2rad(df['dec'].astype('float64').to_numpy())
            cos_s = np.sin(dec1)*np.sin(dec2) + np.cos(dec1)*np.cos(dec2)*np.cos(ra1-ra2)
            cos_s = np.clip(cos_s, -1.0, 1.0)
            df['sep_arcsec'] = (np.arccos(cos_s) * (180.0/ math.pi) * 3600.0).astype('float32')
        else:
            df['sep_arcsec'] = np.float32(np.nan)
    return df

def build_flags(all_df: pd.DataFrame, radius_arcsec: float, bin_deg: int) -> pd.DataFrame:
    if 'row_id' not in all_df.columns:
        raise SystemExit("Missing 'row_id' in closest CSVs")
    if all_df.duplicated('row_id').any():
        all_df = all_df.sort_values(['row_id','sep_arcsec'], ascending=[True, True]).drop_duplicates('row_id', keep='first')
    flags = pd.DataFrame({
        'row_id': all_df['row_id'].astype('string'),
        'in_ra': all_df.get('in_ra', np.nan).astype('float32'),
        'in_dec': all_df.get('in_dec', np.nan).astype('float32'),
        'sep_arcsec': all_df['sep_arcsec'].astype('float32'),
    })
    flags['ir_match_strict'] = flags['sep_arcsec'].le(np.float32(radius_arcsec)).astype('boolean')
    for name in ('mjd','w1snr','w2snr','cntr','qual_frame','qi_fact','saa_sep','moon_masked'):
        if name in all_df.columns:
            flags[name] = all_df[name]
    ra_bin, dec_bin = compute_bins(flags['in_ra'], flags['in_dec'], bin_deg=bin_deg)
    flags['ra_bin'] = ra_bin
    flags['dec_bin'] = dec_bin
    for c in ('mjd','w1snr','w2snr','qi_fact','saa_sep'):
        if c in flags.columns: flags[c] = flags[c].astype('float32')
    if 'cntr' in flags.columns: flags['cntr'] = flags['cntr'].astype('Int64')
    if 'qual_frame' in flags.columns: flags['qual_frame'] = flags['qual_frame'].astype('Int16')
    for c in ('ra_bin','dec_bin'):
        if c in flags.columns: flags[c] = flags[c].astype('Int32')
    return flags

def upsert_partition(part_df: pd.DataFrame, sidecar_root: Path):
    # part_df must have columns row_id, ra_bin, dec_bin
    for (r, d), g in part_df.groupby(['ra_bin','dec_bin'], dropna=True):
        subdir = sidecar_root / f"ra_bin={int(r)}" / f"dec_bin={int(d)}"
        subdir.mkdir(parents=True, exist_ok=True)
        dest = subdir / 'part-flags.parquet'
        if dest.exists():
            old = pd.read_parquet(dest)
            # anti-join then concat -> upsert by row_id
            old_no_new = old[~old['row_id'].isin(g['row_id'])]
            new_all = pd.concat([old_no_new, g], ignore_index=True)
            new_all.to_parquet(dest, engine='pyarrow', index=False)
        else:
            g.to_parquet(dest, engine='pyarrow', index=False)

def rebuild_global_from_sidecar(sidecar_root: Path, out_path: Path):
    parts = list(sidecar_root.rglob('part-flags.parquet'))
    frames = []
    for p in parts:
        try:
            frames.append(pd.read_parquet(p, columns=['row_id','in_ra','in_dec','sep_arcsec','ir_match_strict','mjd','w1snr','w2snr','cntr','qual_frame','qi_fact','saa_sep','moon_masked','ra_bin','dec_bin']))
        except Exception:
            frames.append(pd.read_parquet(p))
    if not frames:
        raise SystemExit('No sidecar partitions found; cannot rebuild global parquet')
    all_flags = pd.concat(frames, ignore_index=True)
    # ensure unique by row_id (keep closest already enforced in build_flags, but be safe)
    all_flags = all_flags.drop_duplicates(subset=['row_id']).reset_index(drop=True)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    all_flags.to_parquet(out_path, engine='pyarrow', index=False)

def main():
    p = argparse.ArgumentParser(description='Concatenate NEOWISE-SE flags and write sidecar parquet (incremental-aware)')
    p.add_argument('--closest-dir', required=True, help='Directory containing *_closest.csv files')
    p.add_argument('--master-root', default='', help='(optional) root of master optical parquet')
    p.add_argument('--out-root', required=True, help='Output root for ALL parquet and sidecar tree')
    p.add_argument('--radius-arcsec', type=float, default=5.0, help='Strict match radius in arcsec')
    p.add_argument('--bin-deg', type=int, default=5, help='Bin size in degrees for ra_bin/dec_bin')
    p.add_argument('--dataset-name', type=str, default='neowise_se', help='Dataset base name')
    p.add_argument('--incremental', action='store_true', help='Process only new/changed *_closest.csv per manifest')
    p.add_argument('--manifest', default=DEF_MANIFEST, help='Path to closest ingest manifest (JSON)')
    args = p.parse_args()

    closest_dir = Path(args.closest_dir)
    out_root = Path(args.out_root)
    sidecar_root = out_root / 'sidecar'
    all_parquet = out_root / f"{args.dataset_name}_flags_ALL.parquet"

    files = collect_closest_paths(closest_dir)
    if not files:
        raise SystemExit(f"No *_closest.csv files found in: {closest_dir}")

    manifest = load_manifest(Path(args.manifest)) if args.incremental else {}
    to_process = []
    for f in files:
        key = str(f)
        sha = sha1_file(f)
        if not args.incremental:
            to_process.append((f, sha))
        else:
            rec = manifest.get(key)
            if rec is None or rec.get('sha1') != sha:
                to_process.append((f, sha))

    if not to_process:
        print('[INFO] No new/changed closest CSVs; sidecar/global remain up-to-date.')
        return

    print(f"[INFO] Loading {len(to_process)} closest CSV(s) from {closest_dir} ...")
    frames: List[pd.DataFrame] = []
    for i, (f, sha) in enumerate(to_process, 1):
        try:
            df = load_and_normalize(f)
            frames.append(df)
            manifest[str(f)] = {'sha1': sha}
        except Exception as e:
            print(f"[WARN] Skipping {f.name}: {e}")
        if i % 50 == 0:
            print(f"[INFO] ... {i}/{len(to_process)} files loaded")
    if not frames:
        print('[INFO] No valid closest CSVs to process after filtering.'); return

    all_df = pd.concat(frames, ignore_index=True)
    print(f"[INFO] Combined changed rows: {len(all_df):,}")
    flags = build_flags(all_df, radius_arcsec=args.radius_arcsec, bin_deg=args.bin_deg)
    print(f"[INFO] Flags rows (unique row_id in changed set): {len(flags):,}")

    # Upsert per (ra_bin,dec_bin)
    upsert_partition(flags, sidecar_root=sidecar_root)
    print(f"[OK] Upserted sidecar under: {sidecar_root}")

    # Rebuild global from sidecar (simple & consistent)
    rebuild_global_from_sidecar(sidecar_root=sidecar_root, out_path=all_parquet)
    print(f"[OK] Rebuilt global flags parquet: {all_parquet}")

    # SUCCESS marker
    (out_root / '_SUCCESS').write_text('ok
', encoding='utf-8')
    print(f"[OK] Wrote marker: {out_root / '_SUCCESS'}")

    # Save manifest
    save_manifest(Path(args.manifest), manifest)
    print(f"[OK] Updated manifest: {args.manifest}")

if __name__ == '__main__':
    main()
