#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
export_masked_view.py (Post 1.6)

Strict export using a mask expression, out-of-core with DuckDB.

Key update (critical):
- Use composite join key (tile_id, NUMBER). NUMBER alone is not globally unique.

Inputs:
- --input-parquet: optical master parquet root
- --irflags-parquet: IR flags parquet keyed by (tile_id, NUMBER)
- --mask: boolean expression over exclude_* columns (e.g., "exclude_ir_strict and exclude_hpm and exclude_skybot and exclude_supercosmos")
- --dedupe-tol-arcsec: approximate dedupe grid (default 0.5")
- --out: output parquet path

Behavior:
- Adds/derives exclusion columns:
  - exclude_ir_strict := NOT has_ir_match
  - exclude_hpm := is_hpm (if present else FALSE)
  - exclude_skybot := is_skybot (if present else FALSE)
  - exclude_supercosmos := is_supercosmos_artifact (if present else FALSE)
  - exclude_spike := is_spike (if present else FALSE)
  - exclude_morphology := is_morphology_bad (if present else FALSE)
- Dedupe is applied before filtering (consistent with counts-only).
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

    out_path = Path(a.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    con.execute(f"PRAGMA threads={int(a.duckdb_threads)};")
    con.execute("PRAGMA memory_limit='2GB';")

    opt_glob = os.path.join(a.input_parquet, "**", "*.parquet")
    ir_path = a.irflags_parquet

    opt_cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({sql_quote(opt_glob)}, hive_partitioning=1) LIMIT 0;").fetchall()]
    ir_cols  = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({sql_quote(ir_path)}) LIMIT 0;").fetchall()]

    if not ("tile_id" in opt_cols and "NUMBER" in opt_cols):
        raise SystemExit("[ERROR] Optical master must contain tile_id and NUMBER for composite join.")
    if not ("tile_id" in ir_cols and "NUMBER" in ir_cols):
        raise SystemExit("[ERROR] IR flags must contain tile_id and NUMBER for composite join.")

    ra_col, dec_col = pick_coords(opt_cols, a.ra_col, a.dec_col)
    grid = float(a.dedupe_tol_arcsec) / 3600.0

    # Optional source mask columns
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
          NUMBER::BIGINT AS NUMBER,
          has_ir_match::BOOLEAN AS has_ir_match,
          dist_arcsec::DOUBLE AS dist_arcsec
        FROM read_parquet({sql_quote(ir_path)});
    """)

    # Build deduped joined table with derived exclude_* columns
    def src_or_false(name: str) -> str:
        return f"CAST({name} AS BOOLEAN)" if src_cols.get(name, False) else "FALSE"

    con.execute(f"""
        CREATE VIEW joined_dedup AS
        WITH base AS (
          SELECT
            o.*,
            o.tile_id::VARCHAR AS _tile_id,
            o.NUMBER::BIGINT AS _NUMBER,
            {ra_col}::DOUBLE AS _ra,
            {dec_col}::DOUBLE AS _dec,
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
          r._NUMBER AS NUMBER,
          COALESCE(i.has_ir_match, FALSE) AS has_ir_match,
          i.dist_arcsec AS dist_arcsec,

          -- Derived exclusion columns
          (NOT COALESCE(i.has_ir_match, FALSE)) AS exclude_ir_strict,
          {src_or_false("is_hpm")} AS exclude_hpm,
          {src_or_false("is_skybot")} AS exclude_skybot,
          {src_or_false("is_supercosmos_artifact")} AS exclude_supercosmos,
          {src_or_false("is_spike")} AS exclude_spike,
          {src_or_false("is_morphology_bad")} AS exclude_morphology_bad
        FROM ranked r
        LEFT JOIN ir i
          ON r._tile_id = i.tile_id AND r._NUMBER = i.NUMBER
        WHERE r.rn = 1;
    """)

    # Apply mask expression (user provided)
    # Important: mask expression is evaluated in SQL context over derived columns above.
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
