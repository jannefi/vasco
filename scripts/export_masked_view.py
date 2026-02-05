#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
export_masked_view.py (Post 1.6) — OUT-OF-CORE, COMPOSITE-KEY JOIN

Strict export using a mask expression with DuckDB, joining on (tile_id, NUMBER).
Adds CLI controls for DuckDB memory and temp spill directory.

- --input-parquet: optical master parquet root (required)
- --irflags-parquet: IR flags parquet keyed by (tile_id, NUMBER) (required)
- --mask: boolean expression over derived exclude_* columns (required)
- --dedupe-tol-arcsec: approx dedupe grid (default 0.5")
- --out: output parquet path (required)
- --ra-col / --dec-col: optional coordinate overrides
- --duckdb-threads: worker threads (default 4)
- --duckdb-mem: DuckDB memory limit, e.g. "auto", "12GB" (default: "auto")
- --temp-dir: temp/spill directory (default: <out_dir>/_duckdb_tmp)
- --use-file-db: if set, use a file-backed DuckDB DB next to the output
"""

import argparse
import os
from pathlib import Path
from typing import List, Optional, Tuple

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input-parquet", required=True)
    p.add_argument("--irflags-parquet", required=True)
    p.add_argument("--mask", required=True)
    p.add_argument("--dedupe-tol-arcsec", type=float, default=0.5)
    p.add_argument("--out", required=True)
    p.add_argument("--ra-col", default=None)
    p.add_argument("--dec-col", default=None)
    p.add_argument("--duckdb-threads", type=int, default=4)
    p.add_argument("--duckdb-mem", default="auto")
    p.add_argument("--temp-dir", default="")
    p.add_argument("--use-file-db", action="store_true")
    return p.parse_args()

def sql_quote(s: str) -> str:
    return "'" + str(s).replace("'", "''") + "'"

def pick_coords(cols: List[str], ra_override: Optional[str], dec_override: Optional[str]) -> Tuple[str, str]:
    if ra_override and dec_override and ra_override in cols and dec_override in cols:
        return ra_override, dec_override
    for ra, dec in [
        ("ALPHAWIN_J2000", "DELTAWIN_J2000"),
        ("ALPHA_J2000", "DELTA_J2000"),
        ("X_WORLD", "Y_WORLD"),
    ]:
        if ra in cols and dec in cols:
            return ra, dec
    raise SystemExit("[ERROR] Could not auto-detect RA/Dec columns; pass --ra-col/--dec-col.")

def main():
    a = parse_args()
    try:
        import duckdb
    except Exception as e:
        raise SystemExit(f"[ERROR] duckdb is required: {e}")

    out_path = Path(a.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Configure spill directory (default: sibling of output)
    temp_dir = Path(a.temp_dir).resolve() if a.temp_dir else (out_path.parent / "_duckdb_tmp")
    temp_dir.mkdir(parents=True, exist_ok=True)

    # Optionally use a file DB to improve locality & reuse
    db_path = (out_path.parent / "export_tmp.duckdb").as_posix() if a.use_file_db else ":memory:"
    con = duckdb.connect(database=db_path)

    # Threads / temp / memory
    con.execute(f"PRAGMA threads={int(a.duckdb_threads)};")
    con.execute(f"PRAGMA temp_directory={sql_quote(temp_dir.as_posix())};")
    mem = (a.duckdb_mem or "auto").strip().lower()
    if mem == "auto":
        con.execute("PRAGMA memory_limit='auto';")
    else:
        con.execute(f"PRAGMA memory_limit={sql_quote(a.duckdb_mem)};")

    # Log effective settings (best effort; may vary by DuckDB version)
    try:
        ml = con.execute("PRAGMA memory_limit").fetchone()[0]
        td = con.execute("PRAGMA temp_directory").fetchone()[0]
        print(f"[INFO] DuckDB memory_limit={ml}, temp_directory={td}, threads={a.duckdb_threads}")
    except Exception:
        pass

    opt_glob = os.path.join(a.input_parquet, "**", "*.parquet")
    ir_path = a.irflags_parquet

    opt_cols = [r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet({sql_quote(opt_glob)}, hive_partitioning=1) LIMIT 0;"
    ).fetchall()]
    ir_cols = [r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet({sql_quote(ir_path)}) LIMIT 0;"
    ).fetchall()]

    if not ("tile_id" in opt_cols and "NUMBER" in opt_cols):
        raise SystemExit("[ERROR] Optical master must contain tile_id and NUMBER for composite join.")
    if not ("tile_id" in ir_cols and "NUMBER" in ir_cols):
        raise SystemExit("[ERROR] IR flags must contain tile_id and NUMBER for composite join.")

    ra_col, dec_col = pick_coords(opt_cols, a.ra_col, a.dec_col)
    grid = float(a.dedupe_tol_arcsec) / 3600.0

    # Optional source mask columns present in master
    src_cols = {
        "is_hpm": "is_hpm" in opt_cols,
        "is_skybot": "is_skybot" in opt_cols,
        "is_supercosmos_artifact": "is_supercosmos_artifact" in opt_cols,
        "is_spike": "is_spike" in opt_cols,
        "is_morphology_bad": "is_morphology_bad" in opt_cols,
    }

    con.execute(f"CREATE VIEW optical AS SELECT * FROM read_parquet({sql_quote(opt_glob)}, hive_partitioning=1);")
    con.execute(f"""
        CREATE VIEW ir AS
        SELECT
          tile_id::VARCHAR AS tile_id,
          NUMBER::BIGINT  AS NUMBER,
          has_ir_match::BOOLEAN AS has_ir_match,
          dist_arcsec::DOUBLE   AS dist_arcsec
        FROM read_parquet({sql_quote(ir_path)});
    """)

    def src_or_false(name: str) -> str:
        return f"CAST({name} AS BOOLEAN)" if src_cols.get(name, False) else "FALSE"

    # NOTE: Windowed dedupe + LEFT JOIN; with temp_directory set, DuckDB can spill as needed.
    con.execute(f"""
        CREATE VIEW joined_dedup AS
        WITH base AS (
          SELECT
            o.*,
            o.tile_id::VARCHAR AS _tile_id,
            o.NUMBER::BIGINT   AS _NUMBER,
            {ra_col}::DOUBLE   AS _ra,
            {dec_col}::DOUBLE  AS _dec,
            CAST(round(({ra_col}::DOUBLE) / {grid}) AS BIGINT) AS dk_ra,
            CAST(round(({dec_col}::DOUBLE) / {grid}) AS BIGINT) AS dk_dec
          FROM optical o
          WHERE {ra_col} IS NOT NULL AND {dec_col} IS NOT NULL
        ),
        ranked AS (
          SELECT *,
                 row_number() OVER (PARTITION BY dk_ra, dk_dec ORDER BY _tile_id, _NUMBER) AS rn
          FROM base
        )
        SELECT
          r.* EXCLUDE(_tile_id, _NUMBER, _ra, _dec, dk_ra, dk_dec, rn),
          r._tile_id AS tile_id,
          r._NUMBER  AS NUMBER,
          COALESCE(i.has_ir_match, FALSE) AS has_ir_match,
          i.dist_arcsec AS dist_arcsec,
          -- Derived exclusions:
          (NOT COALESCE(i.has_ir_match, FALSE))          AS exclude_ir_strict,
          {src_or_false("is_hpm")}                       AS exclude_hpm,
          {src_or_false("is_skybot")}                    AS exclude_skybot,
          {src_or_false("is_supercosmos_artifact")}      AS exclude_supercosmos,
          {src_or_false("is_spike")}                     AS exclude_spike,
          {src_or_false("is_morphology_bad")}            AS exclude_morphology_bad
        FROM ranked r
        LEFT JOIN ir i
          ON r._tile_id = i.tile_id AND r._NUMBER = i.NUMBER
        WHERE r.rn = 1;
    """)

    mask_expr = a.mask

    con.execute(f"""
        COPY (
          SELECT * FROM joined_dedup
          WHERE ({mask_expr})
        )
        TO {sql_quote(out_path.as_posix())}
        (FORMAT PARQUET);
    """)

    print(f"[OK] Wrote strict export: {out_path}")
    con.close()

if __name__ == "__main__":
    main()