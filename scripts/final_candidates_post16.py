#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
final_candidates_post16.py (Post 1.6)

Fix A + OOM fix:
- Join optical master to NEOWISE IR flags sidecar using durable key row_id (string digits).
- If optical master parquet does NOT contain row_id, derive it deterministically per parquet fragment
  exactly like extract_positions_for_neowise_se.py: stable_row_id(tile_id, local_index) using SHA1,
  first 8 bytes, signed=False, and ALWAYS materialize as string to avoid float/scientific issues.
- Avoid OOM: stream parquet fragments and stage rows into an on-disk DuckDB database, then run
  a global approx-dedupe (0.5" default) and compute counts via SQL.

Outputs:
- <out_dir>/post16_match_summary.txt
- Optional: <out_dir>/annotated.parquet if --publish-annotated

Important DuckDB note:
- DuckDB does NOT allow prepared parameters inside some DDL (e.g., CREATE VIEW ... read_parquet(?)).
  We must inline the parquet path as a quoted SQL string literal instead.
"""

import argparse
import hashlib
import os
from pathlib import Path
from typing import Optional, Tuple, List

import numpy as np
import pandas as pd
import pyarrow.dataset as ds


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--optical-master-parquet", required=True)
    p.add_argument("--irflags-parquet", required=True)
    p.add_argument("--annotate-ir", action="store_true")
    p.add_argument("--counts-only", action="store_true")
    p.add_argument("--publish-annotated", action="store_true")
    p.add_argument("--join-key", default=None, help="Override join key; supported: row_id, NUMBER")
    p.add_argument("--dedupe-tol-arcsec", type=float, default=0.5)
    p.add_argument("--out-dir", required=True)
    p.add_argument("--ra-col", default=None, help="Override RA column name")
    p.add_argument("--dec-col", default=None, help="Override Dec column name")
    p.add_argument("--duckdb-path", default=None, help="Optional path for on-disk duckdb file (default: <out_dir>/post16_tmp.duckdb)")
    p.add_argument("--duckdb-threads", type=int, default=4, help="DuckDB threads")
    return p.parse_args()


def sql_quote(s: str) -> str:
    """Return a SQL single-quoted literal with embedded quotes escaped."""
    return "'" + str(s).replace("'", "''") + "'"


def pick_coords(schema_names: List[str], ra_override: Optional[str], dec_override: Optional[str]) -> Tuple[str, str]:
    if ra_override and dec_override and ra_override in schema_names and dec_override in schema_names:
        return ra_override, dec_override
    for pair in [("ALPHAWIN_J2000", "DELTAWIN_J2000"),
                 ("ALPHA_J2000", "DELTA_J2000"),
                 ("X_WORLD", "Y_WORLD")]:
        if pair[0] in schema_names and pair[1] in schema_names:
            return pair
    raise SystemExit("[ERROR] RA/Dec columns not found; use --ra-col/--dec-col.")


def stable_row_id(tile_id: str, local_index: int) -> str:
    """
    Exactly matches extract_positions_for_neowise_se.py:
    sha1(f"{tile_id}:{local_index}") -> first 8 bytes -> unsigned int -> string digits
    """
    h = hashlib.sha1(f"{tile_id}:{local_index}".encode("utf-8")).digest()
    return str(int.from_bytes(h[:8], byteorder="big", signed=False))


def normalize_row_id_series(existing: pd.Series) -> pd.Series:
    """
    Normalize an existing row_id column to string without float round-trips.
    If values are already strings (including scientific notation), keep the literal string.
    """
    def conv(x):
        if pd.isna(x):
            return pd.NA
        if isinstance(x, str):
            return x.strip()
        try:
            return str(int(x))
        except Exception:
            return str(x)
    return existing.apply(conv).astype("string")


def ensure_duckdb():
    try:
        import duckdb  # noqa
    except Exception as e:
        raise SystemExit(f"[ERROR] duckdb is required for this streaming implementation: {e}")


def main():
    a = parse_args()
    ensure_duckdb()
    import duckdb

    out_dir = Path(a.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load datasets via Arrow (schema only up-front)
    opt_ds = ds.dataset(a.optical_master_parquet, format="parquet")
    ir_ds = ds.dataset(a.irflags_parquet, format="parquet")

    opt_cols = list(opt_ds.schema.names)
    ir_cols = list(ir_ds.schema.names)

    # Decide join key
    join_key = a.join_key
    if join_key is None:
        # Prefer row_id if sidecar is row_id-keyed
        if "row_id" in ir_cols:
            join_key = "row_id"
        elif "NUMBER" in ir_cols:
            join_key = "NUMBER"
        else:
            raise SystemExit("[ERROR] IR flags parquet has neither row_id nor NUMBER; cannot join.")

    if join_key not in ("row_id", "NUMBER"):
        raise SystemExit("[ERROR] --join-key must be one of: row_id, NUMBER")

    # Coordinates
    ra_col, dec_col = pick_coords(opt_cols, a.ra_col, a.dec_col)

    # Determine whether optical already has row_id
    optical_has_row_id = "row_id" in opt_cols
    optical_has_number = "NUMBER" in opt_cols
    optical_has_tile = "tile_id" in opt_cols

    if join_key == "row_id" and not optical_has_row_id and not optical_has_tile:
        raise SystemExit("[ERROR] Optical master lacks row_id and tile_id; cannot derive row_id.")

    if join_key == "NUMBER" and not optical_has_number:
        raise SystemExit("[ERROR] Join key NUMBER requested but optical master lacks NUMBER.")

    # Mask columns (optional in optical; default False when absent)
    mask_cols = ["is_morphology_bad", "is_spike", "is_hpm", "is_skybot", "is_supercosmos_artifact"]
    present_masks = [c for c in mask_cols if c in opt_cols]

    # DuckDB staging (on disk)
    db_path = Path(a.duckdb_path) if a.duckdb_path else (out_dir / "post16_tmp.duckdb")
    if db_path.exists():
        db_path.unlink()  # start clean each run

    con = duckdb.connect(str(db_path))
    con.execute(f"PRAGMA threads={int(a.duckdb_threads)};")
    # Keep conservative memory limit; this is mostly disk-staged
    con.execute("PRAGMA memory_limit='2GB';")

    # ---- FIX: no prepared parameter in CREATE VIEW ----
    ir_path_sql = sql_quote(a.irflags_parquet)
    con.execute(f"""
        CREATE VIEW ir AS
        SELECT
            CAST(row_id AS VARCHAR) AS row_id,
            CAST(has_ir_match AS BOOLEAN) AS has_ir_match,
            CAST(dist_arcsec AS DOUBLE) AS dist_arcsec
        FROM read_parquet({ir_path_sql});
    """)

    # Staging table: store per-row dedupe keys and joined flags
    con.execute("""
        CREATE TABLE staging (
            row_id VARCHAR,
            opt_number BIGINT,
            dk_ra BIGINT,
            dk_dec BIGINT,
            has_ir_match BOOLEAN,
            dist_arcsec DOUBLE,
            is_morphology_bad BOOLEAN,
            is_spike BOOLEAN,
            is_hpm BOOLEAN,
            is_skybot BOOLEAN,
            is_supercosmos_artifact BOOLEAN
        );
    """)

    grid = float(a.dedupe_tol_arcsec) / 3600.0
    dropped_no_coords = 0
    staged_rows = 0
    frag_count = 0

    # Stream fragments; read minimal columns per fragment
    cols_to_read = [ra_col, dec_col]
    if optical_has_tile:
        cols_to_read.append("tile_id")
    if optical_has_number:
        cols_to_read.append("NUMBER")
    if optical_has_row_id:
        cols_to_read.append("row_id")
    cols_to_read += present_masks

    for idx, frag in enumerate(opt_ds.get_fragments(), 1):
        frag_count = idx
        tbl = frag.to_table(columns=cols_to_read)
        if tbl.num_rows == 0:
            continue

        df = tbl.to_pandas()
        ra = pd.to_numeric(df[ra_col], errors="coerce")
        dec = pd.to_numeric(df[dec_col], errors="coerce")

        # Build join key column
        if join_key == "row_id":
            if optical_has_row_id:
                rid = normalize_row_id_series(df["row_id"])
            else:
                tiles = df["tile_id"].astype(str).fillna("unknown")
                n = len(df)
                if tiles.nunique() == 1:
                    t = tiles.iloc[0]
                    rid = pd.Series([stable_row_id(t, i) for i in range(n)], dtype="string")
                else:
                    rid = pd.Series([stable_row_id(tiles.iloc[i], i) for i in range(n)], dtype="string")
        else:
            # NUMBER join (not used in current row_id-sidecar workflow)
            rid = pd.Series([pd.NA] * len(df), dtype="string")

        # Approx-dedupe keys
        dk_ra = np.rint(ra.to_numpy() / grid).astype("float64")
        dk_dec = np.rint(dec.to_numpy() / grid).astype("float64")
        ok = np.isfinite(dk_ra) & np.isfinite(dk_dec)
        dropped_no_coords += int((~ok).sum())

        # Prepare chunk for insert
        if optical_has_number:
            opt_number = pd.to_numeric(df["NUMBER"], errors="coerce").astype("Int64")[ok]
        else:
            opt_number = pd.Series([pd.NA] * int(ok.sum()), dtype="Int64")

        chunk = pd.DataFrame({
            "row_id": rid[ok].astype("string"),
            "opt_number": opt_number,
            "dk_ra": dk_ra[ok].astype("int64"),
            "dk_dec": dk_dec[ok].astype("int64"),
        })

        # Masks (default False if missing)
        for c in mask_cols:
            if c in df.columns:
                chunk[c] = df[c][ok].astype(bool)
            else:
                chunk[c] = False

        con.register("chunk_df", chunk)

        con.execute("""
            INSERT INTO staging
            SELECT
                c.row_id,
                CAST(c.opt_number AS BIGINT) AS opt_number,
                c.dk_ra,
                c.dk_dec,
                COALESCE(i.has_ir_match, FALSE) AS has_ir_match,
                i.dist_arcsec AS dist_arcsec,
                c.is_morphology_bad,
                c.is_spike,
                c.is_hpm,
                c.is_skybot,
                c.is_supercosmos_artifact
            FROM chunk_df c
            LEFT JOIN ir i
            USING(row_id);
        """)

        staged_rows += int(chunk.shape[0])
        con.unregister("chunk_df")

        if idx % 500 == 0:
            print(f"[INFO] fragments={idx} staged_rows={staged_rows} dropped_no_coords={dropped_no_coords}")

    # Global approx dedupe: pick one row per (dk_ra, dk_dec) cell (deterministic by lowest row_id)
    res = con.execute("""
        WITH ranked AS (
            SELECT *,
                   row_number() OVER (PARTITION BY dk_ra, dk_dec ORDER BY row_id) AS rn
            FROM staging
        )
        SELECT
            count(*) AS total,
            sum(CASE WHEN has_ir_match THEN 1 ELSE 0 END) AS ir_pos,
            sum(CASE WHEN is_morphology_bad THEN 1 ELSE 0 END) AS morph_bad,
            sum(CASE WHEN is_spike THEN 1 ELSE 0 END) AS spikes,
            sum(CASE WHEN is_hpm THEN 1 ELSE 0 END) AS hpm,
            sum(CASE WHEN is_skybot THEN 1 ELSE 0 END) AS skybot,
            sum(CASE WHEN is_supercosmos_artifact THEN 1 ELSE 0 END) AS sc_art
        FROM ranked
        WHERE rn = 1;
    """).fetchone()

    total = int(res[0] or 0)
    ir_pos = int(res[1] or 0)
    morph_bad = int(res[2] or 0)
    spikes = int(res[3] or 0)
    hpm = int(res[4] or 0)
    skybot = int(res[5] or 0)
    sc_art = int(res[6] or 0)

    survivors_ir_strict = total - ir_pos
    survivors_after_filters = survivors_ir_strict - (morph_bad + spikes + hpm + skybot + sc_art)

    summary_path = out_dir / "post16_match_summary.txt"
    with summary_path.open("w", encoding="utf-8") as f:
        f.write("POST16 SUMMARY (streaming + DuckDB staging)\n")
        f.write(f"Optical fragments scanned: {frag_count}\n")
        f.write(f"Rows staged (with finite coords): {staged_rows}\n")
        f.write(f"Rows dropped (no coords): {dropped_no_coords}\n")
        f.write(f"Total (after approx dedupe): {total}\n")
        f.write(f"IR-positive rows: {ir_pos}\n")
        f.write(f"Morphology bad: {morph_bad}\n")
        f.write(f"Diffraction spikes: {spikes}\n")
        f.write(f"High proper motion (POSS-I epoch): {hpm}\n")
        f.write(f"SkyBoT asteroid proximity: {skybot}\n")
        f.write(f"SuperCOSMOS artifacts: {sc_art}\n")
        f.write("———\n")
        f.write(f"Survivors (IR-strict): {survivors_ir_strict}\n")
        f.write(f"Survivors (after all filters): {survivors_after_filters}\n")

    print(f"[OK] Summary written: {summary_path}")

    # Optional annotated export
    if a.publish_annotated:
        out_parquet = out_dir / "annotated.parquet"
        con.execute(f"""
            COPY (
                WITH ranked AS (
                    SELECT *,
                           row_number() OVER (PARTITION BY dk_ra, dk_dec ORDER BY row_id) AS rn
                    FROM staging
                )
                SELECT
                    row_id,
                    opt_number,
                    has_ir_match,
                    dist_arcsec,
                    is_morphology_bad,
                    is_spike,
                    is_hpm,
                    is_skybot,
                    is_supercosmos_artifact
                FROM ranked
                WHERE rn = 1
            )
            TO {sql_quote(out_parquet.as_posix())}
            (FORMAT PARQUET);
        """)
        print(f"[OK] Annotated dataset written: {out_parquet}")

    # If counts-only and no annotated export, stop
    if a.counts_only and not a.publish_annotated:
        con.close()
        return

    con.close()


if __name__ == "__main__":
    main()