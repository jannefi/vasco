#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
export_masked_view.py (Post 1.6) — TWO-PHASE, OUT-OF-CORE, COMPOSITE-KEY JOIN

Phase 1 (narrow): (tile_id, NUMBER, coords) -> dedupe grid -> join IR -> apply mask -> survivor KEYS ONLY
Phase 2 (wide):  survivor KEYS -> join back to optical o.* -> write parquet (single file or partitioned dataset)

CLI:
  --input-parquet <dir>        optical master parquet root (required)
  --irflags-parquet <file>     IR flags parquet keyed by (tile_id, NUMBER) (required)
  --mask "<expr>"              boolean expression over derived exclude_* columns (required)
  --dedupe-tol-arcsec <f>      approx dedupe grid, default 0.5"
  --out <file>                 (optional) single-file parquet output
  --out-dataset-dir <dir>      (optional) write a partitioned parquet dataset under a directory
  --ra-col / --dec-col         optional coordinate overrides
  --duckdb-threads <int>       default 8
  --duckdb-mem <str>           "auto" or numeric string like "14GB" (default: "auto")
  --temp-dir <dir>             spill directory (default: /tmp/vasco_duckdb_tmp)
  --use-file-db                use file-backed DB near output

Notes:
  - Provide either --out (single file) OR --out-dataset-dir (partitioned).
  - For speed + memory, prefer --out-dataset-dir (partitioned).
"""

import argparse
import os
import re
from pathlib import Path
from typing import List, Optional, Tuple

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input-parquet", required=True)
    p.add_argument("--irflags-parquet", required=True)
    p.add_argument("--mask", required=True)
    p.add_argument("--dedupe-tol-arcsec", type=float, default=0.5)
    p.add_argument("--out", default="")
    p.add_argument("--out-dataset-dir", default="")
    p.add_argument("--ra-col", default=None)
    p.add_argument("--dec-col", default=None)
    p.add_argument("--duckdb-threads", type=int, default=8)
    p.add_argument("--duckdb-mem", default="auto")  # "auto" or "14GB" etc.
    p.add_argument("--temp-dir", default="/tmp/vasco_duckdb_tmp")
    p.add_argument("--use-file-db", action="store_true")
    a = p.parse_args()
    if not a.out and not a.out_dataset_dir:
        raise SystemExit("[ERROR] Provide --out (single file) or --out-dataset-dir (partitioned).")
    return a

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
    raise SystemExit("[ERROR] Could not auto-detect RA/Dec; pass --ra-col/--dec-col.")

def bytes_to_human(n: int) -> str:
    # Return a DuckDB-friendly string like "14GB"
    gb = max(1, int(n / (1024**3)))
    return f"{gb}GB"

def get_system_mem_approx() -> int:
    # Try /proc/meminfo (WSL/Linux)
    try:
        with open("/proc/meminfo", "r") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    # in kB
                    parts = line.split()
                    kb = int(parts[1])
                    return kb * 1024
    except Exception:
        pass
    # Fallback minimal guess: 16GB
    return 16 * 1024**3

def normalize_mask_for_phase1(mask: str) -> str:
    """
    Phase-1 has only IR info (via i.*) and no source-side boolean columns.
    Map mask names:
      exclude_ir_strict -> (NOT COALESCE(i.has_ir_match, FALSE))
      exclude_hpm/skybot/supercosmos/spike/morphology_bad -> FALSE  (absent in phase 1)
    Use word-boundary regex to avoid replacing substrings.
    """
    m = mask
    repls = {
        r"\bexclude_ir_strict\b": "(NOT COALESCE(i.has_ir_match, FALSE))",
        r"\bexclude_hpm\b": "FALSE",
        r"\bexclude_skybot\b": "FALSE",
        r"\bexclude_supercosmos\b": "FALSE",
        r"\bexclude_spike\b": "FALSE",
        r"\bexclude_morphology_bad\b": "FALSE",
    }
    for pat, rep in repls.items():
        m = re.sub(pat, rep, m)
    return m

def main():
    a = parse_args()
    try:
        import duckdb
    except Exception as e:
        raise SystemExit(f"[ERROR] duckdb is required: {e}")

    # Resolve outputs
    out_file = Path(a.out).resolve() if a.out else None
    out_ds = Path(a.out_dataset_dir).resolve() if a.out_dataset_dir else None
    if out_file:
        out_file.parent.mkdir(parents=True, exist_ok=True)
    if out_ds:
        out_ds.mkdir(parents=True, exist_ok=True)

    # Spill dir (use SSD path like /mnt/c/wsltmp/vasco_duckdb_tmp on WSL)
    temp_dir = Path(a.temp_dir).resolve()
    temp_dir.mkdir(parents=True, exist_ok=True)

    # DB location
    db_root = out_ds if out_ds else (out_file.parent if out_file else Path.cwd())
    db_path = (db_root / "export_tmp.duckdb").as_posix() if a.use_file_db else ":memory:"
    con = duckdb.connect(database=db_path)

    # Threads / temp / memory pragmas with robust 'auto' handling
    con.execute(f"PRAGMA threads={int(a.duckdb_threads)};")
    con.execute(f"PRAGMA temp_directory={sql_quote(temp_dir.as_posix())};")

    mem = (a.duckdb_mem or "auto").strip().lower()
    effective_mem = None
    if mem == "auto":
        # Try 'auto', fallback to ~80% of total RAM as <N>GB if parser rejects 'auto'
        try:
            con.execute("PRAGMA memory_limit='auto';")
            effective_mem = "auto"
        except Exception:
            total = get_system_mem_approx()
            target = int(total * 0.8)
            fallback = bytes_to_human(target)
            con.execute(f"PRAGMA memory_limit={sql_quote(fallback)};")
            effective_mem = fallback
    else:
        # Expect value like "14GB"
        con.execute(f"PRAGMA memory_limit={sql_quote(a.duckdb_mem)};")
        effective_mem = a.duckdb_mem

    try:
        ml = con.execute("PRAGMA memory_limit").fetchone()[0]
        td = con.execute("PRAGMA temp_directory").fetchone()[0]
        print(f"[INFO] DuckDB memory_limit={ml} (effective={effective_mem}), temp_directory={td}, threads={a.duckdb_threads}")
    except Exception:
        pass

    # Input schemas
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

    # Views — narrow projection for phase 1 (keys + coords + bins only)
    con.execute(f"""
        CREATE VIEW optical_narrow AS
        SELECT
          tile_id::VARCHAR AS tile_id,
          NUMBER::BIGINT   AS NUMBER,
          {ra_col}::DOUBLE AS ra,
          {dec_col}::DOUBLE AS dec,
          ra_bin::BIGINT   AS ra_bin,
          dec_bin::BIGINT  AS dec_bin
        FROM read_parquet({sql_quote(opt_glob)}, hive_partitioning=1)
        WHERE {ra_col} IS NOT NULL AND {dec_col} IS NOT NULL;
    """)

    con.execute(f"""
        CREATE VIEW ir AS
        SELECT
          tile_id::VARCHAR  AS tile_id,
          NUMBER::BIGINT    AS NUMBER,
          has_ir_match::BOOLEAN AS has_ir_match,
          dist_arcsec::DOUBLE   AS dist_arcsec
        FROM read_parquet({sql_quote(ir_path)});
    """)

    # Phase 1 — dedupe (narrow), join IR, apply normalized mask; keep KEYS ONLY
    mask_phase1 = normalize_mask_for_phase1(a.mask)

    con.execute(f"""
        CREATE TEMP VIEW base AS
        SELECT
          o.tile_id AS _tile_id,
          o.NUMBER  AS _NUMBER,
          o.ra, o.dec, o.ra_bin, o.dec_bin,
          CAST(round(o.ra  / {grid}) AS BIGINT) AS dk_ra,
          CAST(round(o.dec / {grid}) AS BIGINT) AS dk_dec
        FROM optical_narrow o;
    """)

    con.execute(f"""
        CREATE TEMP VIEW ranked AS
        SELECT *,
               row_number() OVER (PARTITION BY dk_ra, dk_dec ORDER BY _tile_id, _NUMBER) AS rn
        FROM base;
    """)

    con.execute(f"""
        CREATE TEMP TABLE survivors_keys AS
        SELECT
          r._tile_id AS tile_id,
          r._NUMBER  AS NUMBER,
          r.ra_bin, r.dec_bin
        FROM ranked r
        LEFT JOIN ir i
          ON r._tile_id = i.tile_id AND r._NUMBER = i.NUMBER
        WHERE r.rn = 1
          AND ({mask_phase1});
    """)

    con.execute("CREATE INDEX survivors_idx ON survivors_keys(tile_id, NUMBER);")

    # Phase 2 — join survivor keys back to full optical rows and write output
    con.execute(f"CREATE VIEW optical_wide AS SELECT * FROM read_parquet({sql_quote(opt_glob)}, hive_partitioning=1);")

    if out_ds:
        out_dir = sql_quote(out_ds.as_posix())
        con.execute(f"""
            COPY (
              SELECT o.*
              FROM optical_wide o
              SEMI JOIN survivors_keys s
                ON o.tile_id = s.tile_id AND o.NUMBER = s.NUMBER
            )
            TO {out_dir}
            (FORMAT PARQUET, PARTITION_BY (ra_bin, dec_bin));
        """)
        print(f"[OK] Wrote partitioned dataset under: {out_ds}")
    else:
        out_file_q = sql_quote(out_file.as_posix())
        con.execute(f"""
            COPY (
              SELECT o.*
              FROM optical_wide o
              SEMI JOIN survivors_keys s
                ON o.tile_id = s.tile_id AND o.NUMBER = s.NUMBER
            )
            TO {out_file_q}
            (FORMAT PARQUET);
        """)
        print(f"[OK] Wrote strict export: {out_file}")

    con.close()

if __name__ == "__main__":
    main()