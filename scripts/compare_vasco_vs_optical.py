
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Memory-friendly VASCO (NEOWISE-only) ↔ optical matching with Parquet bin pruning.

Key capabilities:
- Partition-aware scan of an optical Parquet dataset (5°×5° bins by default)
- Builds a KD-tree (SciPy) or grid index over ONLY the partitions relevant to the current VASCO chunk
- Streams results to disk per chunk to avoid large in-memory DataFrames
- Falls back to reading a CSV master or scanning tiles if Parquet is not provided

Outputs (compatible with your current pipeline):
  <out>/vasco_matched_to_optical.csv           # or chunked files if --write-chunks=true
  <out>/vasco_still_ir_only.csv
  <out>/match_summary.txt
"""

import argparse
from pathlib import Path
import glob
import sys
import math
import numpy as np
import pandas as pd
import os

# Optional: pyarrow for Parquet dataset scanning
try:
    import pyarrow as pa
    import pyarrow.dataset as ds
    _HAS_ARROW = True
except Exception:
    _HAS_ARROW = False

# Candidate RA/Dec column names
OPT_RA_CANDS    = ["ALPHA_J2000", "RAJ2000", "RA", "X_WORLD", "ra"]
OPT_DEC_CANDS   = ["DELTA_J2000", "DEJ2000", "DEC", "Y_WORLD", "dec"]
VASCO_RA_CANDS  = ["RA_NEOWISE", "RAJ2000", "RA", "ra"]
VASCO_DEC_CANDS = ["DEC_NEOWISE", "DEJ2000", "DEC", "dec"]


DEFAULT_OPTICAL_PARQUET = os.getenv(
    "OPTICAL_PARQUET_DIR",
    "data/local-cats/_master_optical_parquet"  # or "data/metadata/_master_optical_parquet"
)


# ------------------ Generic helpers ------------------


def find_coord_columns(df, ra_cands, dec_cands, label):
    ra = next((c for c in ra_cands  if c in df.columns), None)
    de = next((c for c in dec_cands if c in df.columns), None)
    if not ra or not de:
        raise ValueError(f"[{label}] Could not find RA/Dec in: {list(df.columns)}")
    return ra, de

def to_unit_sphere_xyz(ra_deg, dec_deg):
    """Convert RA/Dec degrees arrays to unit sphere xyz (float32)."""
    ra = np.deg2rad(ra_deg.astype(np.float32))
    de = np.deg2rad(dec_deg.astype(np.float32))
    cosd = np.cos(de)
    x = (cosd * np.cos(ra)).astype(np.float32)
    y = (cosd * np.sin(ra)).astype(np.float32)
    z = np.sin(de).astype(np.float32)
    return np.column_stack((x, y, z))

def angsep_arcsec(ra1_deg, dec1_deg, ra2_deg, dec2_deg):
    ra1 = np.deg2rad(ra1_deg); dec1 = np.deg2rad(dec1_deg)
    ra2 = np.deg2rad(ra2_deg); dec2 = np.deg2rad(dec2_deg)
    cos_d = np.sin(dec1)*np.sin(dec2) + np.cos(dec1)*np.cos(dec2)*np.cos(ra1-ra2)
    cos_d = np.clip(cos_d, -1.0, 1.0)
    return np.rad2deg(np.arccos(cos_d)) * 3600.0


# ------------------ Readers (VASCO + optical) ------------------

def read_vasco_csv(path):
    df = pd.read_csv(path)   # NEOWISE-only; keep all columns
    v_ra, v_de = find_coord_columns(df, VASCO_RA_CANDS, VASCO_DEC_CANDS, "VASCO")
    return df, v_ra, v_de

def read_optical_master_csv(path):
    # Load minimal columns from CSV master to keep RAM down
    probe = pd.read_csv(path, nrows=1)
    o_ra, o_de = find_coord_columns(probe, OPT_RA_CANDS, OPT_DEC_CANDS, "OPTICAL(master CSV)")
    usecols = [o_ra, o_de] + [c for c in ["source_file","tile_id","image_catalog_path"] if c in probe.columns]
    df = pd.read_csv(path, usecols=usecols, dtype={o_ra:"float32", o_de:"float32"})
    for c in ["source_file","tile_id","image_catalog_path"]:
        if c not in df.columns:
            df[c] = ""
    return df, o_ra, o_de

def read_optical_from_tiles(tiles_root):
    patterns = [
        str(Path(tiles_root) / "tile-RA*-DEC*/catalogs/sextractor_pass2.csv"),
        str(Path(tiles_root) / "*/catalogs/sextractor_pass2.csv"),
        str(Path(tiles_root) / "*/*/sextractor_pass2.csv"),
    ]
    files = []
    for pat in patterns:
        files += glob.glob(pat)
    if not files:
        raise FileNotFoundError(f"No sextractor_pass2.csv found under {tiles_root}")

    frames = []
    o_ra = o_de = None
    for f in files:
        probe = pd.read_csv(f, nrows=1)
        ra_col, de_col = find_coord_columns(probe, OPT_RA_CANDS, OPT_DEC_CANDS, "OPTICAL(tiles)")
        dfi = pd.read_csv(f, usecols=[ra_col, de_col], dtype={ra_col:"float32", de_col:"float32"})
        dfi["source_file"] = f
        frames.append(dfi)
        o_ra, o_de = ra_col, de_col
    big = pd.concat(frames, ignore_index=True)
    for c in ["tile_id","image_catalog_path"]:
        if c not in big.columns:
            big[c] = ""
    return big, o_ra, o_de


# ------------------ Parquet (partition-aware) ------------------

def bins_for_chunk(ra_deg_arr, dec_deg_arr, bin_deg):
    """Return sorted unique RA and Dec bin indices for an array, incl. ±1-neighbors."""
    ra = np.asarray(ra_deg_arr, dtype=np.float32) % 360.0
    de = np.asarray(dec_deg_arr, dtype=np.float32)
    ra_bin  = np.floor(ra / bin_deg).astype(np.int32)
    de_bin  = np.floor((de + 90.0) / bin_deg).astype(np.int32)

    ra_bins = set()
    de_bins = set()
    ra_bins_all = np.unique(ra_bin)
    de_bins_all = np.unique(de_bin)

    # include neighbors to catch border cases
    ra_mod = int(np.ceil(360.0 / bin_deg))
    de_mod = int(np.ceil(180.0 / bin_deg))

    for rb in ra_bins_all:
        for d in (-1,0,1):
            ra_bins.add((rb + d) % ra_mod)
    for db in de_bins_all:
        for d in (-1,0,1):
            nb = db + d
            if 0 <= nb < de_mod:
                de_bins.add(nb)
    return sorted(ra_bins), sorted(de_bins), ra_mod, de_mod



def read_parquet_slice(dataset_root, o_ra_name, o_de_name, ra_bins, de_bins):
    """
    Read only the ra_bin/dec_bin partitions and return a pandas DataFrame with
    float32 RA/Dec plus optional provenance columns, robust to Arrow dict/binary types.
    """
    if not _HAS_ARROW:
        raise RuntimeError("pyarrow is required for Parquet dataset scanning.")

    cols = [o_ra_name, o_de_name]
    maybe_prov = ["source_file", "tile_id", "image_catalog_path"]

    def _normalize_table(tbl: pa.Table) -> pa.Table:
        """Cast Arrow columns that can trip pandas (dict/binary) to safe types."""
        tbl = tbl.combine_chunks()  # defragment columns
        fields = tbl.schema
        new_cols = []
        new_names = []
        for i, field in enumerate(fields):
            col = tbl.column(i)
            t = field.type
            # Cast dictionary to plain strings
            if pa.types.is_dictionary(t):
                col = pa.compute.cast(col, pa.string())
                t = pa.string()
            # Cast binary to string (paths/provenance sometimes end up binary)
            elif pa.types.is_binary(t):
                col = pa.compute.cast(col, pa.string())
                t = pa.string()
            # Leave floats/ints/strings untouched
            new_cols.append(col)
            new_names.append(field.name)
        return pa.Table.from_arrays(new_cols, names=new_names)

    try:
        # Preferred: one Dataset with hive partitioning + filter
        dset = ds.dataset(str(dataset_root), format="parquet", partitioning="hive")
        # Include provenance columns if they exist in the dataset schema
        for c in maybe_prov:
            if c in dset.schema.names and c not in cols:
                cols.append(c)
        f = (ds.field("ra_bin").isin(ra_bins)) & (ds.field("dec_bin").isin(de_bins))
        table = dset.to_table(columns=cols, filter=f)
        table = _normalize_table(table)
        # Safe conversion: avoid pandas ExtensionArray by NOT passing Arrow dtype mapper
        df = table.to_pandas(strings_to_categorical=False)

    except Exception:
        # Fallback: read only the matching subdirectories (robust if hive discovery is unavailable)
        pieces = []
        for rb in ra_bins:
            for db in de_bins:
                subdir = Path(dataset_root) / f"ra_bin={rb}" / f"dec_bin={db}"
                if not subdir.exists():
                    continue
                sub_ds = ds.dataset(str(subdir), format="parquet")
                sub_cols = cols[:]
                for c in maybe_prov:
                    if c in sub_ds.schema.names and c not in sub_cols:
                        sub_cols.append(c)
                tbl = sub_ds.to_table(columns=sub_cols)
                pieces.append(_normalize_table(tbl))
        if not pieces:
            df = pd.DataFrame(columns=cols)
        else:
            df = pa.concat_tables(pieces).to_pandas(strings_to_categorical=False)

    # Final dtype fixes
    if o_ra_name in df.columns:
        df[o_ra_name] = df[o_ra_name].astype("float32")
    if o_de_name in df.columns:
        df[o_de_name] = df[o_de_name].astype("float32")
    for c in maybe_prov:
        if c not in df.columns:
            df[c] = ""
    return df




# ------------------ Candidate index (KD-tree or grid) ------------------

class KDIndex:
    def __init__(self, ra_deg, dec_deg):
        self._ra = np.asarray(ra_deg, dtype=np.float32)
        self._de = np.asarray(dec_deg, dtype=np.float32)
        self.mode = "kdtree"
        pts = to_unit_sphere_xyz(self._ra, self._de)
        try:
            from scipy.spatial import cKDTree
            self.tree = cKDTree(pts)
        except Exception:
            self.mode = "grid"
            self._build_grid()

    def _build_grid(self, cell_arcsec=3.0):
        cell = np.float32(cell_arcsec / 3600.0)
        self.cell_deg = cell
        ra = self._ra % 360.0
        de = self._de
        self.n_ra = int(np.ceil(360.0 / cell))
        self.n_de = int(np.ceil(180.0 / cell))
        rb = np.floor(ra / cell).astype(np.int32)
        db = np.floor((de + 90.0) / cell).astype(np.int32)
        self.grid = {}
        for i, rbi, dbi in zip(range(ra.size), rb, db):
            key = (int(rbi), int(dbi))
            self.grid.setdefault(key, []).append(int(i))

    def query_radius(self, ra_deg, dec_deg, radius_arcsec):
        if self.mode == "kdtree":
            theta = np.deg2rad(radius_arcsec / 3600.0)
            r_chord = 2.0 * np.sin(theta / 2.0)
            xyz = to_unit_sphere_xyz(np.asarray([ra_deg], dtype=np.float32),
                                     np.asarray([dec_deg], dtype=np.float32))
            try:
                idxs = self.tree.query_ball_point(xyz[0], r=r_chord)
            except Exception:
                idxs = []
            return np.asarray(idxs, dtype=np.int32)
        else:
            cell = self.cell_deg
            rb = int(np.floor((ra_deg % 360.0) / cell))
            db = int(np.floor((dec_deg + 90.0) / cell))
            cand = []
            for dr in (-1,0,1):
                r2 = (rb + dr) % self.n_ra
                for dd in (-1,0,1):
                    d2 = db + dd
                    if 0 <= d2 < self.n_de:
                        cand.extend(self.grid.get((r2, d2), []))
            return np.asarray(cand, dtype=np.int32)


# ------------------ Matching driver (with Parquet pruning) ------------------

def nearest_within_radius_streaming(
    vasco_df, v_ra, v_de,
    optical_source,              # dict describing source mode
    o_ra_hint, o_de_hint,        # preferred RA/Dec column names to look for
    radius_arcsec,
    out_dir: Path,
    chunk_size=20000,
    bin_deg=5.0,
    write_chunks=True
):
    """
    Stream VASCO rows by chunk. For each chunk:
      - identify (ra_bin, dec_bin) sets
      - load only those Parquet partitions (or full CSV/tiles if needed)
      - build KD-index
      - match, then write matched/unmatched chunk CSVs immediately (if write_chunks)
    Returns: (matched_paths, unmatched_paths, summary_dict)
    """
    matched_paths, unmatched_paths = [], []
    total_matched = total_unmatched = 0

    if optical_source["mode"] == "parquet":
        o_ra_name, o_de_name = o_ra_hint, o_de_hint   # names in the parquet dataset
        ds_root = optical_source["root"]
    elif optical_source["mode"] == "csv":
        df_csv, o_ra_name, o_de_name = read_optical_master_csv(optical_source["path"])
    else:
        df_csv, o_ra_name, o_de_name = read_optical_from_tiles(optical_source["tiles_root"])

    for chunk_id in range(0, len(vasco_df), chunk_size):
        sub = vasco_df.iloc[chunk_id: chunk_id + chunk_size]
        ra_arr = sub[v_ra].astype("float32").values
        de_arr = sub[v_de].astype("float32").values

        # 1) Load optical slice for this chunk
        if optical_source["mode"] == "parquet":
            ra_bins, de_bins, _, _ = bins_for_chunk(ra_arr, de_arr, bin_deg)
            opt = read_parquet_slice(ds_root, o_ra_name, o_de_name, ra_bins, de_bins)
        else:
            opt = df_csv  # CSV or tiles already in memory

        if opt.empty:
            # everything unmatched for this chunk
            unmatched_df = sub.copy()
            if write_chunks:
                up = out_dir / f"vasco_still_ir_only_chunk_{chunk_id:06d}.csv"
                unmatched_df.to_csv(up, index=False)
                unmatched_paths.append(up)
            total_unmatched += len(unmatched_df)
            continue

        # 2) Build index over the optical slice
        idx = KDIndex(opt[o_ra_name].values, opt[o_de_name].values)

        # 3) Match each row (lightweight per-row candidate retrieval)
        matched_rows, unmatched_rows = [], []
        opt_ra = opt[o_ra_name].values
        opt_de = opt[o_de_name].values

        for i, row in sub.iterrows():
            vra = float(row[v_ra]); vde = float(row[v_de])
            cand_idx = idx.query_radius(vra, vde, radius_arcsec)
            if cand_idx.size == 0:
                unmatched_rows.append(row.to_dict()); continue
            sep = angsep_arcsec(vra, vde, opt_ra[cand_idx], opt_de[cand_idx])
            j_rel = int(sep.argmin()); j = int(cand_idx[j_rel]); m = float(sep[j_rel])
            if m <= radius_arcsec:
                out = row.to_dict()
                out["match_arcsec"] = m
                out["opt_index"]    = j
                out["opt_ra"]       = float(opt_ra[j])
                out["opt_dec"]      = float(opt_de[j])
                # provenance if present
                for c in ["source_file","tile_id","image_catalog_path"]:
                    if c in opt.columns:
                        out[f"opt_{c}"] = opt.iloc[j][c]
                matched_rows.append(out)
            else:
                unmatched_rows.append(row.to_dict())

        matched_df   = pd.DataFrame(matched_rows)
        unmatched_df = pd.DataFrame(unmatched_rows)

        if write_chunks:
            if not matched_df.empty:
                mp = out_dir / f"vasco_matched_to_optical_chunk_{chunk_id:06d}.csv"
                matched_df.to_csv(mp, index=False); matched_paths.append(mp)
            if not unmatched_df.empty:
                up = out_dir / f"vasco_still_ir_only_chunk_{chunk_id:06d}.csv"
                unmatched_df.to_csv(up, index=False); unmatched_paths.append(up)
        total_matched  += len(matched_df)
        total_unmatched+= len(unmatched_df)

        print(f"[CHUNK {chunk_id:06d}] matched={len(matched_df)} unmatched={len(unmatched_df)} "
              f"opt_slice={len(opt)} index={idx.mode}")

    summary = {
        "vasco_rows": len(vasco_df),
        "matched": total_matched,
        "unmatched": total_unmatched,
        "chunks": math.ceil(len(vasco_df)/chunk_size),
        "bin_deg": bin_deg,
        "radius_arcsec": radius_arcsec,
        "optical_mode": optical_source["mode"],
    }
    return matched_paths, unmatched_paths, summary


# ------------------ CLI ------------------

def main():
    ap = argparse.ArgumentParser(description="Parquet-pruned, streaming VASCO vs optical matcher.")
    ap.add_argument("--vasco", required=True, help="Path to vasco.csv (NEOWISE-only)")
    # Preferred: Parquet dataset (partitioned by ra_bin/dec_bin)
    ap.add_argument("--optical-master-parquet", default=None, help="Path to Parquet dataset root")
    # Fallbacks:
    ap.add_argument("--optical-master-csv", default=None, help="Path to CSV master (heavier memory)")
    ap.add_argument("--tiles-root", default=None, help="Tiles root (scan sextractor_pass2.csv if no master)")
    # Tuning:
    ap.add_argument("--radius-arcsec", type=float, default=2.0)
    ap.add_argument("--bin-deg", type=float, default=5.0, help="Partition bin size used when writing the Parquet dataset")
    ap.add_argument("--chunk-size", type=int, default=20000)
    ap.add_argument("--out-dir", default="./out")
    ap.add_argument("--write-chunks", action="store_true", help="Write per-chunk CSVs and skip concatenation")
    args = ap.parse_args()

    out = Path(args.out_dir); out.mkdir(parents=True, exist_ok=True)
    vasco_df, v_ra, v_de = read_vasco_csv(args.vasco)

    # Decide optical source
    if not (args.optical_master_parquet or args.optical_master_csv or args.tiles_root):
        if Path(DEFAULT_OPTICAL_PARQUET).exists():
            args.optical_master_parquet = DEFAULT_OPTICAL_PARQUET
    
    if args.optical_master_parquet:
        if not _HAS_ARROW:
            print("[ERROR] pyarrow is required for --optical-master-parquet.", file=sys.stderr)
            return 2
        optical_source = {"mode":"parquet", "root": Path(args.optical_master_parquet)}
        # Required RA/Dec column names in Parquet dataset must be provided by you at write time.
        # By convention we used ALPHA_J2000/DELTA_J2000 in the conversion step.
        o_ra_hint, o_de_hint = "ALPHA_J2000", "DELTA_J2000"
    elif args.optical_master_csv:
        optical_source = {"mode":"csv", "path": args.optical_master_csv}
        o_ra_hint, o_de_hint = None, None  # will be inferred in reader
    elif args.tiles_root:
        optical_source = {"mode":"tiles", "tiles_root": args.tiles_root}
        o_ra_hint, o_de_hint = None, None
    else:
        print("[ERROR] Provide one of --optical-master-parquet | --optical-master-csv | --tiles-root", file=sys.stderr)
        return 2

    # Run streaming matcher
    matched_paths, unmatched_paths, summary = nearest_within_radius_streaming(
        vasco_df, v_ra, v_de,
        optical_source,
        o_ra_hint or "ALPHA_J2000", o_de_hint or "DELTA_J2000",
        args.radius_arcsec,
        out,
        chunk_size=args.chunk_size,
        bin_deg=args.bin_deg,
        write_chunks=args.write_chunks
    )

    # Write summary
    with open(out / "match_summary.txt", "w") as f:
        f.write(f"VASCO rows: {summary['vasco_rows']}\n")
        f.write(f"Radius (arcsec): {summary['radius_arcsec']}\n")
        f.write(f"Optical source mode: {summary['optical_mode']}\n")
        f.write(f"Bin size (deg): {summary['bin_deg']}\n")
        f.write(f"Chunks: {summary['chunks']}\n")
        f.write(f"Matched: {summary['matched']}\n")
        f.write(f"Still IR-only: {summary['unmatched']}\n")

    # Concatenate if not writing chunks
    if not args.write_chunks:
        # Read all in-memory (final small step); if you prefer, keep chunked files.
        if matched_paths:
            matched_all = pd.concat([pd.read_csv(p) for p in matched_paths], ignore_index=True)
            matched_all.to_csv(out / "vasco_matched_to_optical.csv", index=False)
        else:
            pd.DataFrame([]).to_csv(out / "vasco_matched_to_optical.csv", index=False)

        if unmatched_paths:
            unmatched_all = pd.concat([pd.read_csv(p) for p in unmatched_paths], ignore_index=True)
            unmatched_all.to_csv(out / "vasco_still_ir_only.csv", index=False)
        else:
            pd.DataFrame([]).to_csv(out / "vasco_still_ir_only.csv", index=False)

    print("Wrote:", out / "match_summary.txt")
    if args.write_chunks:
        print("Chunked outputs in", out)
    else:
        print("Wrote:", out / "vasco_matched_to_optical.csv")
        print("Wrote:", out / "vasco_still_ir_only.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
