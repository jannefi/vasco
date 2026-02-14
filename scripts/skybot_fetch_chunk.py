#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SkyBoT fetcher for one survivors chunk (verbose, production-safe) with optional per-row fallback.

Inputs:
  - chunk CSV:  work/scos_chunks/chunk_XXXXX.csv (columns: number,row_id,ra,dec)
  - lookups:    metadata/tiles/tile_to_plate_lookup.parquet (tile_id,plate_id)
                metadata/plates/plate_epoch_lookup.parquet (plate_id,date_obs_iso,jd)

Outputs under --out-root:
  parts/flags_skybot__<chunk>.parquet
  audit/skybot_audit__<chunk>.parquet
  ledger/skybot_ledger__<chunk>.json

Local match policy (locked today):
  - strict:  5 arcsec   -> has_skybot_match = True
  - wide:   30 arcsec   -> wide_skybot_match = True (strict remains False)
"""

import argparse, json, math, time
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

SKYBOT_URL = "https://vo.imcce.fr/webservices/skybot/skybotconesearch_query.php"

# ---------- CLI & helpers ----------

def parse_args():
    p = argparse.ArgumentParser(description="Fetch SkyBoT flags for one survivors chunk.")
    p.add_argument("--chunk-csv", required=True)
    p.add_argument("--tile-to-plate", default="metadata/tiles/tile_to_plate_lookup.parquet")
    p.add_argument("--plate-epoch",  default="metadata/plates/plate_epoch_lookup.parquet")
    p.add_argument("--out-root",     default="data/local-cats/_master_optical_parquet_flags/skybot")
    # Behavior knobs
    p.add_argument("--field-radius-arcmin", type=float, default=9.0)   # ~8–10′
    p.add_argument("--match-arcsec",         type=float, default=5.0)  # strict
    p.add_argument("--fallback-wide-arcsec", type=float, default=30.0) # labeled fallback
    # Fallback per-row (OFF by default)
    p.add_argument("--fallback-per-row", type=str, default="false",
                   help="true/false (default=false). If true, try per-row 5\" cones only for fields with 200/0.")
    p.add_argument("--fallback-per-row-cap", type=int, default=250,
                   help="Safety cap: maximum rows per chunk to probe with per-row cones.")
    # HTTP/runtime
    p.add_argument("--workers", type=int, default=1)                   # kept sequential for clearer logs
    p.add_argument("--connect-timeout", type=float, default=5.0)
    p.add_argument("--read-timeout",    type=float, default=5.0)
    p.add_argument("--max-retries",     type=int,   default=0)
    # Verbosity / limiting
    p.add_argument("--limit-fields", type=int, default=0, help="0=all fields; otherwise cap to N fields for smoke tests")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()

def vprint(verbose: bool, *a, **k):
    if verbose:
        print(*a, **k, flush=True)

def ensure_dirs(root: Path) -> Dict[str, Path]:
    parts  = root / "parts";  parts.mkdir(parents=True, exist_ok=True)
    audit  = root / "audit";  audit.mkdir(parents=True, exist_ok=True)
    ledger = root / "ledger"; ledger.mkdir(parents=True, exist_ok=True)
    return {"parts": parts, "audit": audit, "ledger": ledger}

# ---------- IO & enrichment ----------

def load_chunk(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    low = {c.lower(): c for c in df.columns}
    need = {"number", "row_id", "ra", "dec"}
    if not need.issubset(set(low.keys())):
        missing = need - set(low.keys())
        raise SystemExit(f"[ERROR] chunk missing columns {missing}: {path}")
    df = df.rename(columns={low["number"]:"number", low["row_id"]:"row_id",
                            low["ra"]:"ra", low["dec"]:"dec"})
    # derive tile_id from row_id "<tile_id>:<NUMBER>"
    df["tile_id"] = df["row_id"].str.split(":").str[0]
    return df

def enrich(df: pd.DataFrame, t2p_path: Path, pep_path: Path, verbose=False) -> pd.DataFrame:
    vprint(verbose, "[INFO] Loading lookups …")
    t2p = pd.read_parquet(t2p_path)  # tile_id, plate_id
    pep = pd.read_parquet(pep_path)  # plate_id, date_obs_iso, jd
    vprint(verbose, f"[INFO] tile->plate rows={len(t2p)}; plate->epoch rows={len(pep)}")

    df = df.merge(t2p, on="tile_id", how="left", validate="many_to_one")
    if df["plate_id"].isna().any():
        sample = df.loc[df["plate_id"].isna(),"tile_id"].unique()[:10]
        raise SystemExit(f"[ERROR] plate_id missing for tiles: {sample}")
    df = df.merge(pep, on="plate_id", how="left", validate="many_to_one")
    if (df["date_obs_iso"].isna() & df["jd"].isna()).any():
        bad = df.loc[(df["date_obs_iso"].isna() & df["jd"].isna()),"plate_id"].unique()[:10]
        raise SystemExit(f"[ERROR] epoch missing for plate_ids: {bad}")

    # prefer ISO; keep JD numeric as fallback
    df["epoch_iso"] = df["date_obs_iso"]
    df["epoch_jd"]  = df["jd"]
    vprint(verbose, "[INFO] Enrichment OK (plate_id + epoch present for all rows).")
    return df

# ---------- Fielding & HTTP ----------

def grid_fields(df: pd.DataFrame, field_radius_arcmin: float) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Simple grid that yields ~100–200 rows/field for typical densities."""
    step_deg = field_radius_arcmin / 60.0
    dec_bucket = np.floor((df["dec"] + 90.0) / step_deg).astype(int)
    cosd = np.clip(np.cos(np.deg2rad(df["dec"].astype(float))), 1e-4, 1.0)
    step_ra = step_deg / cosd
    ra_bucket = np.floor((df["ra"] % 360.0) / step_ra).astype(int)

    out = df.copy()
    out["field_id"] = (dec_bucket.astype(str) + "_" + ra_bucket.astype(str)).values

    centers = (out.groupby("field_id", as_index=False)
                 .agg(ra_f=("ra","median"),
                      dec_f=("dec","median"),
                      epoch_iso=("epoch_iso", lambda s: s.mode().iloc[0] if len(s.mode()) else None),
                      epoch_jd=("epoch_jd",  lambda s: s.mode().iloc[0] if len(s.mode()) else None),
                      n=("ra","size")))
    return out, centers

def call_skybot_field(ra_f: float, dec_f: float, epoch_iso: str, epoch_jd: float,
                      rs_arcmin: float, ct: float, rt: float, max_retries: int,
                      verbose=False) -> Tuple[int, List[Tuple[float,float]]]:
    """Field call, SR in arcmin; returns HTTP status and list of (RAdeg,DECdeg)."""
    params = {
        "RA": f"{ra_f:.8f}",
        "DEC": f"{dec_f:.8f}",
        "SR": f"{rs_arcmin/60.0:.6f}",            # degrees
        "EPOCH": f"{epoch_jd:.6f}" if pd.notna(epoch_jd) else epoch_iso,
        "EQUINOX": "J2000",
        "REFSYS": "EQJ2000",
        "OUTPUT": "all",
        "mime": "text",
    }
    tries = 0
    while True:
        try:
            if verbose:
                print(f"[HTTP] FIELD RA={params['RA']} DEC={params['DEC']} EP={params['EPOCH']} SR={params['SR']}", flush=True)
            r = requests.get(SKYBOT_URL, params=params, timeout=(ct, rt))
            status = r.status_code
            if status == 200:
                lines = [ln for ln in r.text.splitlines() if ln and not ln.startswith("#")]
                objs: List[Tuple[float,float]] = []
                if lines:
                    # Many outputs have RA(deg),DEC(deg) at positions 5,6
                    for ln in lines:
                        parts = [p for p in ln.replace("|"," ").split() if p]
                        if len(parts) >= 6:
                            try:
                                ra_deg = float(parts[4]); de_deg = float(parts[5])
                                objs.append((ra_deg, de_deg))
                            except Exception:
                                continue
                return status, objs
            elif status in (429, 500, 502, 503, 504) and tries < max_retries:
                time.sleep(2.0 * (tries + 1)); tries += 1; continue
            else:
                return status, []
        except requests.RequestException as e:
            if verbose: print(f"[WARN] HTTP exception (field): {e}", flush=True)
            if tries < max_retries:
                time.sleep(2.0 * (tries + 1)); tries += 1; continue
            return -1, []

def call_skybot_cone(ra: float, dec: float, epoch_iso: str, epoch_jd: float,
                     rs_arcsec: float, ct: float, rt: float, max_retries: int,
                     verbose=False) -> Tuple[int, List[Tuple[float,float]]]:
    """Per-row cone, SR in arcsec; returns HTTP status and list of (RAdeg,DECdeg)."""
    sr_deg = rs_arcsec / 3600.0
    params = {
        "RA": f"{ra:.8f}",
        "DEC": f"{dec:.8f}",
        "SR": f"{sr_deg:.8f}",
        "EPOCH": f"{epoch_jd:.6f}" if pd.notna(epoch_jd) else epoch_iso,
        "EQUINOX": "J2000",
        "REFSYS": "EQJ2000",
        "OUTPUT": "all",
        "mime": "text",
    }
    tries = 0
    while True:
        try:
            if verbose:
                print(f"[HTTP] ROW RA={params['RA']} DEC={params['DEC']} EP={params['EPOCH']} SR={params['SR']}", flush=True)
            r = requests.get(SKYBOT_URL, params=params, timeout=(ct, rt))
            status = r.status_code
            if status == 200:
                lines = [ln for ln in r.text.splitlines() if ln and not ln.startswith("#")]
                objs: List[Tuple[float,float]] = []
                if lines:
                    for ln in lines:
                        parts = [p for p in ln.replace("|"," ").split() if p]
                        if len(parts) >= 6:
                            try:
                                ra_deg = float(parts[4]); de_deg = float(parts[5])
                                objs.append((ra_deg, de_deg))
                            except Exception:
                                continue
                return status, objs
            elif status in (429, 500, 502, 503, 504) and tries < max_retries:
                time.sleep(2.0 * (tries + 1)); tries += 1; continue
            else:
                return status, []
        except requests.RequestException as e:
            if verbose: print(f"[WARN] HTTP exception (row): {e}", flush=True)
            if tries < max_retries:
                time.sleep(2.0 * (tries + 1)); tries += 1; continue
            return -1, []

# ---------- Main processing ----------

def process_chunk(args):
    chunk_path = Path(args.chunk_csv)
    chunk_name = chunk_path.stem.replace(".csv","")
    out_dirs = ensure_dirs(Path(args.out_root))

    # Parse booleans safely
    fallback_per_row = str(args.fallback_per_row).strip().lower() in ("1","true","yes","y","on")

    t0 = time.time()
    print(f"[RUN] {chunk_name} …", flush=True)

    df = load_chunk(chunk_path)
    print(f"[INFO] rows_in={len(df)}", flush=True)

    df = enrich(df, Path(args.tile_to_plate), Path(args.plate_epoch), verbose=args.verbose)
    df, centers = grid_fields(df, args.field_radius_arcmin)

    total_fields = len(centers)
    if args.limit_fields and args.limit_fields > 0:
        centers = centers.head(args.limit_fields)

    print(f"[INFO] fields_planned={len(centers)} (of total {total_fields}); radius={args.field_radius_arcmin:.1f} arcmin", flush=True)

    fetched: Dict[str, List[Tuple[float,float]]] = {}
    statuses: Dict[str, int] = {}
    fields_ok = 0; http_errors = 0; rate_limits = 0

    # sequential for clearer logs
    for i, row in enumerate(centers.itertuples(index=False), start=1):
        fid = row.field_id
        n_rows_in_field = int(df[df["field_id"] == fid].shape[0])
        print(f"[FIELD {i}/{len(centers)}] fid={fid} n={n_rows_in_field}", flush=True)
        status, objs = call_skybot_field(row.ra_f, row.dec_f, row.epoch_iso, row.epoch_jd,
                                         args.field_radius_arcmin,
                                         args.connect_timeout, args.read_timeout,
                                         args.max_retries, verbose=args.verbose)
        statuses[fid] = status; fetched[fid] = objs
        if status == 200: fields_ok += 1
        elif status == 429: rate_limits += 1
        else: http_errors += 1

    # local match + optional per-row fallback for 200/0 fields
    strict = args.match_arcsec
    wide   = args.fallback_wide_arcsec
    out_rows = []
    aud_rows = []
    rows_matched_5 = 0
    rows_matched_30 = 0

    # Track fallback stats
    fb_attempted = 0
    fb_matched   = 0
    fb_http_err  = 0

    # Identify fields eligible for per-row fallback: 200/0
    empty_fields = {fid for fid,objs in fetched.items() if statuses.get(fid, -1) == 200 and len(objs) == 0}

    for fid, sub in df.groupby("field_id"):
        objs = fetched.get(fid, [])
        aud_rows.append({
            "chunk": chunk_name,
            "field_id": fid,
            "field_radius_arcmin": float(args.field_radius_arcmin),
            "http_status": int(statuses.get(fid, -1)),
            "returned_rows": int(len(objs)),
        })

        # Fast path: field had objects; do local match against objs
        if objs:
            objs_arr = np.array(objs, dtype=float)  # (N,2)
            for _, r in sub.iterrows():
                ra0 = float(r["ra"]); de0 = float(r["dec"])
                dra  = np.deg2rad(objs_arr[:,0] - ra0)
                ddec = np.deg2rad(objs_arr[:,1] - de0)
                cd   = math.cos(math.radians((de0 + np.median(objs_arr[:,1])) / 2.0))
                seps = np.hypot(dra * cd, ddec) * (180.0/np.pi) * 3600.0
                best_sep = float(np.min(seps)) if seps.size else None
                nmatch   = int(np.sum(seps <= wide)) if seps.size else 0

                is_strict = (best_sep is not None) and (best_sep <= strict)
                is_wide   = (best_sep is not None) and (not is_strict) and (best_sep <= wide)

                if is_strict: rows_matched_5  += 1
                if is_wide:   rows_matched_30 += 1

                out_rows.append({
                    "row_id": r["row_id"],
                    "NUMBER": int(r["number"]),
                    "tile_id": r["tile_id"],
                    "plate_id": r["plate_id"],
                    "has_skybot_match": bool(is_strict),
                    "wide_skybot_match": bool(is_wide),
                    "matched_count": int(nmatch),
                    "best_sep_arcsec": best_sep if seps.size else None,
                    "epoch_used": r["epoch_iso"] if pd.notna(r["epoch_iso"]) else r["epoch_jd"],
                    "source_chunk": chunk_name
                })
            continue  # next field

        # Slow path: field empty; optionally probe per-row 5" cones
        if (fid in empty_fields) and fallback_per_row and (fb_attempted < args.fallback_per_row_cap):
            n_left = args.fallback_per_row_cap - fb_attempted
            # Only probe up to the remaining budget in this field
            sub_probe = sub.head(n_left)
            print(f"[FALLBACK] per-row cones for field={fid} rows={len(sub_probe)} (cap left {n_left})", flush=True)

            for _, r in sub_probe.iterrows():
                fb_attempted += 1
                ra0 = float(r["ra"]); de0 = float(r["dec"])
                status, ob_list = call_skybot_cone(
                    ra0, de0,
                    str(r["epoch_iso"]) if pd.notna(r["epoch_iso"]) else None,
                    float(r["epoch_jd"]) if pd.notna(r["epoch_jd"]) else float("nan"),
                    strict,  # 5"
                    args.connect_timeout, args.read_timeout, args.max_retries,
                    verbose=args.verbose
                )
                if status not in (200, 429) and status != -1 and status != 0:
                    fb_http_err += 1

                # local evaluate with the small list (already within 5")
                best_sep = None; nmatch = 0
                if ob_list:
                    objs_arr = np.array(ob_list, dtype=float)
                    dra  = np.deg2rad(objs_arr[:,0] - ra0)
                    ddec = np.deg2rad(objs_arr[:,1] - de0)
                    cd   = math.cos(math.radians(de0))  # tiny cone, median not needed
                    seps = np.hypot(dra * cd, ddec) * (180.0/np.pi) * 3600.0
                    best_sep = float(np.min(seps)) if seps.size else None
                    nmatch   = int(np.sum(seps <= strict)) if seps.size else 0

                is_strict = (best_sep is not None) and (best_sep <= strict)
                is_wide   = False  # per-row uses 5" cones only; no 30" here

                if is_strict:
                    rows_matched_5 += 1
                    fb_matched     += 1

                out_rows.append({
                    "row_id": r["row_id"],
                    "NUMBER": int(r["number"]),
                    "tile_id": r["tile_id"],
                    "plate_id": r["plate_id"],
                    "has_skybot_match": bool(is_strict),
                    "wide_skybot_match": bool(is_wide),
                    "matched_count": int(nmatch),
                    "best_sep_arcsec": best_sep,
                    "epoch_used": r["epoch_iso"] if pd.notna(r["epoch_iso"]) else r["epoch_jd"],
                    "source_chunk": chunk_name
                })

            # For any rows in the field we did not probe (cap exhausted), emit unmatched rows now
            if len(sub) > len(sub_probe):
                remainder = sub.iloc[len(sub_probe):]
                for _, r in remainder.iterrows():
                    out_rows.append({
                        "row_id": r["row_id"],
                        "NUMBER": int(r["number"]),
                        "tile_id": r["tile_id"],
                        "plate_id": r["plate_id"],
                        "has_skybot_match": False,
                        "wide_skybot_match": False,
                        "matched_count": 0,
                        "best_sep_arcsec": None,
                        "epoch_used": r["epoch_iso"] if pd.notna(r["epoch_iso"]) else r["epoch_jd"],
                        "source_chunk": chunk_name
                    })
            continue

        # Default (no fallback or not eligible): mark as unmatched for all rows in this field
        for _, r in sub.iterrows():
            out_rows.append({
                "row_id": r["row_id"],
                "NUMBER": int(r["number"]),
                "tile_id": r["tile_id"],
                "plate_id": r["plate_id"],
                "has_skybot_match": False,
                "wide_skybot_match": False,
                "matched_count": 0,
                "best_sep_arcsec": None,
                "epoch_used": r["epoch_iso"] if pd.notna(r["epoch_iso"]) else r["epoch_jd"],
                "source_chunk": chunk_name
            })

    # write parts
    parts_df = pd.DataFrame(out_rows)
    parts_path = out_dirs["parts"] / f"flags_skybot__{chunk_name}.parquet"
    pq.write_table(pa.Table.from_pandas(parts_df, preserve_index=False), parts_path, compression="zstd")

    # write audit
    audit_df = pd.DataFrame(aud_rows)
    audit_path = out_dirs["audit"] / f"skybot_audit__{chunk_name}.parquet"
    pq.write_table(pa.Table.from_pandas(audit_df, preserve_index=False), audit_path, compression="zstd")

    # write ledger
    elapsed = round(time.time() - t0, 3)
    ledger = {
        "chunk": chunk_name,
        "ts_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "rows_in": int(df.shape[0]),
        "fields_planned": int(len(centers)),
        "fields_ok": int(fields_ok),
        "http_errors": int(http_errors),
        "rate_limits": int(rate_limits),
        "rows_matched_5as": int(rows_matched_5),
        "rows_matched_30as": int(rows_matched_30),
        "fallback_per_row": bool(fallback_per_row),
        "fallback_per_row_cap": int(args.fallback_per_row_cap),
        "fallback_rows_attempted": int(fb_attempted),
        "fallback_rows_matched": int(fb_matched),
        "fallback_http_errors": int(fb_http_err),
        "elapsed_s": elapsed,
        "params": {
            "field_radius_arcmin": args.field_radius_arcmin,
            "local_match_arcsec": args.match_arcsec,
            "fallback_wide_arcsec": args.fallback_wide_arcsec,
            "connect_timeout_s": args.connect_timeout,
            "read_timeout_s": args.read_timeout,
            "max_retries": args.max_retries
        }
    }
    ledger_path = out_dirs["ledger"] / f"skybot_ledger__{chunk_name}.json"
    with open(ledger_path, "w", encoding="utf-8") as f:
        json.dump(ledger, f, indent=2)

    print(f"[DONE] {chunk_name} parts={parts_path} audit={audit_path} ledger={ledger_path} "
          f"matched_5as={rows_matched_5} matched_30as={rows_matched_30} fb_rows={fb_attempted}/{fb_matched}", flush=True)

# ---------- Entrypoint ----------

if __name__ == "__main__":
    args = parse_args()
    process_chunk(args)
