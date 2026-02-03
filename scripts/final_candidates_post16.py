#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
final_candidates_post16.py (Post 1.6)

Counts-only and optional annotated export, using out-of-core DuckDB execution.

Key update (critical):
- Optical master NUMBER is NOT globally unique. Use composite join key (tile_id, NUMBER).
- IR flags parquet is expected to contain tile_id, NUMBER, has_ir_match, dist_arcsec.

Inputs:
- --optical-master-parquet: root of partitioned parquet dataset
- --irflags-parquet: parquet file keyed by (tile_id, NUMBER)

Outputs:
- post16_match_summary.txt (always)
- annotated.parquet (optional if --publish-annotated)

Dedupe:
- Approximate spatial dedupe using 0.5" (default) grid on RA/Dec columns.
- RA/Dec auto-detection: ALPHAWIN_J2000/DELTAWIN_J2000 → ALPHA_J2000/DELTA_J2000 → X_WORLD/Y_WORLD.

Masks:
- If mask columns do not exist in optical master, they default to False.
"""

import argparse
import os
from pathlib import Path
from typing import Tuple, List, Optional

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--optical-master-parquet", required=True)
    p.add_argument("--irflags-parquet", required=True)
    p.add_argument("--annotate-ir", action="store_true")
    p.add_argument("--counts-only", action="store_true")
    p.add_argument("--publish-annotated", action="store_true")
    p.add_argument("--dedupe-tol-arcsec", type=float, default=0.5)
    p.add_argument("--out-dir", required=True)
    p.add_argument("--ra-col", default=None)
    p.add_argument("--dec-col", default=None)
    p.add_argument("--duckdb-path", default=None, help="Optional on-disk duckdb file (default: <out-dir>/post16_tmp.duckdb)")
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

    out_dir = Path(a.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    db_path = Path(a.duckdb_path) if a.duckdb_path else (out_dir / "post16_tmp.duckdb")
    if db_path.exists():
        db_path.unlink()

    con = duckdb.connect(str(db_path))
    con.execute(f"PRAGMA threads={int(a.duckdb_threads)};")
    con.execute("PRAGMA memory_limit='2GB';")

    opt_glob = os.path.join(a.optical_master_parquet, "**", "*.parquet")
    ir_path = a.irflags_parquet

    # Read schemas (lightweight)
    opt_cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({sql_quote(opt_glob)}, hive_partitioning=1) LIMIT 0;").fetchall()]
    ir_cols  = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({sql_quote(ir_path)}) LIMIT 0;").fetchall()]

    # Join key selection: prefer (tile_id, NUMBER)
    if not ("tile_id" in opt_cols and "NUMBER" in opt_cols):
        raise SystemExit("[ERROR] Optical master must contain tile_id and NUMBER for Post 1.6 composite join.")
    if not ("tile_id" in ir_cols and "NUMBER" in ir_cols):
        raise SystemExit("[ERROR] IR flags must contain tile_id and NUMBER (composite key).")

    ra_col, dec_col = pick_coords(opt_cols, a.ra_col, a.dec_col)
    grid = float(a.dedupe_tol_arcsec) / 3600.0

    # Optional mask columns (default False if missing)
    mask_cols = ["is_morphology_bad", "is_spike", "is_hpm", "is_skybot", "is_supercosmos_artifact"]
    present_masks = [c for c in mask_cols if c in opt_cols]

    # Create views
    con.execute(f"""
        CREATE VIEW optical AS
        SELECT * FROM read_parquet({sql_quote(opt_glob)}, hive_partitioning=1);
    """)
    con.execute(f"""
        CREATE VIEW ir AS
        SELECT
          tile_id::VARCHAR AS tile_id,
          NUMBER::BIGINT AS NUMBER,
          has_ir_match::BOOLEAN AS has_ir_match,
          dist_arcsec::DOUBLE AS dist_arcsec
        FROM read_parquet({sql_quote(ir_path)});
    """)

    # Dedupe + join + summary in one go.
    # Dedupe key uses rounded grid cell on RA/Dec.
    # Deterministic choice: smallest (tile_id, NUMBER) per cell.
    # Also ensure mask columns exist (default False if missing).
    select_masks = []
    for c in mask_cols:
        if c in present_masks:
            select_masks.append(f"CAST({c} AS BOOLEAN) AS {c}")
        else:
            select_masks.append(f"FALSE AS {c}")
    select_masks_sql = ",\n          ".join(select_masks)

    # Create deduped+joined view
    con.execute(f"""
        CREATE VIEW joined_dedup AS
        WITH base AS (
          SELECT
            tile_id::VARCHAR AS tile_id,
            NUMBER::BIGINT AS NUMBER,
            {ra_col}::DOUBLE AS ra,
            {dec_col}::DOUBLE AS dec,
            CAST(round(({ra_col}::DOUBLE) / {grid}) AS BIGINT) AS dk_ra,
            CAST(round(({dec_col}::DOUBLE) / {grid}) AS BIGINT) AS dk_dec,
            {select_masks_sql}
          FROM optical
        ),
        ranked AS (
          SELECT *,
            row_number() OVER (PARTITION BY dk_ra, dk_dec ORDER BY tile_id, NUMBER) AS rn
          FROM base
          WHERE ra IS NOT NULL AND dec IS NOT NULL
        )
        SELECT
          r.tile_id, r.NUMBER,
          r.dk_ra, r.dk_dec,
          COALESCE(i.has_ir_match, FALSE) AS has_ir_match,
          i.dist_arcsec AS dist_arcsec,
          r.is_morphology_bad, r.is_spike, r.is_hpm, r.is_skybot, r.is_supercosmos_artifact
        FROM ranked r
        LEFT JOIN ir i
          ON r.tile_id = i.tile_id AND r.NUMBER = i.NUMBER
        WHERE r.rn = 1;
    """)

    # Summary
    total, ir_pos, morph_bad, spikes, hpm, skybot, scos = con.execute("""
      SELECT
        count(*) AS total,
        sum(CASE WHEN has_ir_match THEN 1 ELSE 0 END) AS ir_pos,
        sum(CASE WHEN is_morphology_bad THEN 1 ELSE 0 END) AS morph_bad,
        sum(CASE WHEN is_spike THEN 1 ELSE 0 END) AS spikes,
        sum(CASE WHEN is_hpm THEN 1 ELSE 0 END) AS hpm,
        sum(CASE WHEN is_skybot THEN 1 ELSE 0 END) AS skybot,
        sum(CASE WHEN is_supercosmos_artifact THEN 1 ELSE 0 END) AS scos
      FROM joined_dedup;
    """).fetchone()

    total = int(total or 0)
    ir_pos = int(ir_pos or 0)
    morph_bad = int(morph_bad or 0)
    spikes = int(spikes or 0)
    hpm = int(hpm or 0)
    skybot = int(skybot or 0)
    scos = int(scos or 0)

    survivors_ir_strict = total - ir_pos
    survivors_after_filters = survivors_ir_strict - (morph_bad + spikes + hpm + skybot + scos)

    summary_path = out_dir / "post16_match_summary.txt"
    with summary_path.open("w", encoding="utf-8") as f:
        f.write("POST16 SUMMARY (DuckDB out-of-core; composite join tile_id+NUMBER)\n")
        f.write(f"Optical parquet glob: {opt_glob}\n")
        f.write(f"IR flags parquet: {ir_path}\n")
        f.write(f"RA/Dec columns: {ra_col}/{dec_col}\n")
        f.write(f"Approx dedupe tol (arcsec): {a.dedupe_tol_arcsec}\n")
        f.write(f"Total (after approx dedupe): {total}\n")
        f.write(f"IR-positive rows: {ir_pos}\n")
        f.write(f"Morphology bad: {morph_bad}\n")
        f.write(f"Diffraction spikes: {spikes}\n")
        f.write(f"High proper motion: {hpm}\n")
        f.write(f"SkyBoT: {skybot}\n")
        f.write(f"SuperCOSMOS artifacts: {scos}\n")
        f.write("———\n")
        f.write(f"Survivors (IR-strict): {survivors_ir_strict}\n")
        f.write(f"Survivors (after all filters): {survivors_after_filters}\n")

    print(f"[OK] Summary written: {summary_path}")

    # Optional annotated export
    if a.publish_annotated:
        out_parquet = out_dir / "annotated.parquet"
        con.execute(f"""
          COPY (
            SELECT
              tile_id, NUMBER,
              has_ir_match, dist_arcsec,
              is_morphology_bad, is_spike, is_hpm, is_skybot, is_supercosmos_artifact
            FROM joined_dedup
          )
          TO {sql_quote(out_parquet.as_posix())}
          (FORMAT PARQUET);
        """)
        print(f"[OK] Annotated dataset written: {out_parquet}")

    con.close()

if __name__ == "__main__":
    main()