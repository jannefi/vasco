#!/usr/bin/env python3
"""stage_skybot_post.py

Run-scoped postprocess stage: SkyBoT asteroid screening (epoch-aware) using VO-SSP SkyBoT
cone-search, writing both (a) Parquet artifacts (parts/audit/ledger) and (b) a shrinking-set
stage CSV + flags CSV + ledger JSON under <run-dir>/stages.

This modernizes the older SkyBoT runner that operated under ./work/scos_chunks with chunk
parallelism. For current run-scoped stages (typically <= ~20k rows) we run ONE input file in
one process. Background execution is handled by a small nohup wrapper script.

Inputs
------
- One CSV (or glob) under --run-dir containing at minimum:
    src_id (or row_id), ra, dec
  src_id must be "tile_id:object_id".

Epoch requirement
-----------------
SkyBoT queries MUST be epoch-aware. We derive per-row epoch from:
  metadata/tiles/tile_to_plate_lookup.parquet  (tile_id -> plate_id)
  metadata/plates/plate_epoch_lookup.parquet   (plate_id -> date_obs_iso, jd)

Outputs
-------
Artifacts under <run-dir>/skybot/{parts,audit,ledger}:
- parts/flags_skybot__<tag>.parquet
- audit/skybot_audit__<tag>.parquet
- ledger/skybot_ledger__<tag>.json

Stage outputs under <run-dir>/stages:
1) stage_<STAGE>_SKYBOT.csv
   Kept remainder AFTER SkyBoT elimination (drops strict matches within --match-arcsec).
   Columns: src_id, ra, dec

2) stage_<STAGE>_SKYBOT_flags.csv
   Audit table for ALL input rows.
   Columns: src_id, ra, dec, has_skybot_match, wide_skybot_match, matched_count, best_sep_arcsec, epoch_used

3) stage_<STAGE>_SKYBOT_ledger.json
   Totals + parameters + paths to Parquet artifacts.

Notes
-----
- Query radius default is 60 arcmin for parity (locked).
- grid_step_arcmin controls grouping into "fields" (bigger => fewer HTTP calls).
- The VO-SSP endpoint sometimes returns QUERY_STATUS=ERROR with message "No solar system object was found".
  This is treated as a VALID empty result (http_status=200, returned_rows=0), matching the existing fetcher.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import xml.etree.ElementTree as ET

SKYBOT_URL = "https://ssp.imcce.fr/webservices/skybot/api/conesearch.php"


def parse_args():
    p = argparse.ArgumentParser(description="Run-scoped SkyBoT stage (single input; epoch-aware).")
    p.add_argument("--run-dir", required=True, help="Run folder, e.g. ./work/runs/run-S1-...")
    p.add_argument(
        "--input-glob",
        default="stages/stage_S0_PS1SH.csv",
        help="Glob (relative to run-dir) for input stage CSV(s). Default: stages/stage_S0_PS1SH.csv",
    )
    p.add_argument("--stage", default="S1", help="Stage label used in output filenames. Default: S1")
    p.add_argument(
        "--tag",
        default="",
        help="Optional tag suffix for Parquet artifacts (default derives from stage + input stem)",
    )

    # Lookups
    p.add_argument("--tile-to-plate", default="metadata/tiles/tile_to_plate_lookup.parquet")
    p.add_argument("--plate-epoch", default="metadata/plates/plate_epoch_lookup.parquet")

    # Match policy
    p.add_argument("--match-arcsec", type=float, default=5.0)
    p.add_argument("--fallback-wide-arcsec", type=float, default=60.0)

    # Field query geometry
    p.add_argument("--grid-step-arcmin", type=float, default=80.0,
                   help="Grouping size for building field centers (bigger => fewer HTTP calls).")
    p.add_argument("--query-radius-arcmin", type=float, default=60.0,
                   help="SkyBoT query radius around each field center (arcmin). Keep 60 for parity.")

    # Optional per-row fallback (kept, but off by default)
    p.add_argument("--fallback-per-row", type=str, default="false")
    p.add_argument("--fallback-per-row-cap", type=int, default=100)

    # HTTP
    p.add_argument("--connect-timeout", type=float, default=10.0)
    p.add_argument("--read-timeout", type=float, default=30.0)
    p.add_argument("--max-retries", type=int, default=3)

    # Utilities
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()


def vprint(verbose: bool, *a, **k):
    if verbose:
        print(*a, **k, flush=True)


def ensure_dirs(run_dir: Path) -> Dict[str, Path]:
    sky = run_dir / "skybot"
    parts = sky / "parts"; parts.mkdir(parents=True, exist_ok=True)
    audit = sky / "audit"; audit.mkdir(parents=True, exist_ok=True)
    ledger = sky / "ledger"; ledger.mkdir(parents=True, exist_ok=True)
    stages = run_dir / "stages"; stages.mkdir(parents=True, exist_ok=True)
    return {"sky": sky, "parts": parts, "audit": audit, "ledger": ledger, "stages": stages}


def load_input_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    low = {c.lower(): c for c in df.columns}
    if "ra" not in low or "dec" not in low:
        raise SystemExit(f"[ERROR] missing ra/dec in {path}")

    if "src_id" in low:
        df = df.rename(columns={low["src_id"]: "src_id", low["ra"]: "ra", low["dec"]: "dec"})
        df["src_id"] = df["src_id"].astype(str)
    elif "row_id" in low:
        df = df.rename(columns={low["row_id"]: "src_id", low["ra"]: "ra", low["dec"]: "dec"})
        df["src_id"] = df["src_id"].astype(str)
    else:
        raise SystemExit(f"[ERROR] missing src_id/row_id in {path}")

    df["tile_id"] = df["src_id"].str.split(":").str[0]
    df["object_id"] = df["src_id"].str.split(":").str[1].apply(lambda x: int(float(x)))
    return df[["src_id", "tile_id", "object_id", "ra", "dec"]]


def enrich_epoch(df: pd.DataFrame, t2p_path: Path, pep_path: Path, verbose=False) -> pd.DataFrame:
    vprint(verbose, "[INFO] loading lookups")
    t2p = pd.read_parquet(t2p_path)  # tile_id, plate_id
    pep = pd.read_parquet(pep_path)  # plate_id, date_obs_iso, jd
    df = df.merge(t2p, on="tile_id", how="left", validate="many_to_one")
    if df["plate_id"].isna().any():
        bad = df.loc[df["plate_id"].isna(), "tile_id"].unique()[:10]
        raise SystemExit(f"[ERROR] plate_id missing for tiles: {bad}")
    df = df.merge(pep, on="plate_id", how="left", validate="many_to_one")
    if (df["date_obs_iso"].isna() & df["jd"].isna()).any():
        bad = df.loc[(df["date_obs_iso"].isna() & df["jd"].isna()), "plate_id"].unique()[:10]
        raise SystemExit(f"[ERROR] epoch missing for plate_ids: {bad}")
    df["epoch_iso"] = df["date_obs_iso"]
    df["epoch_jd"] = df["jd"]
    return df


def grid_fields(df: pd.DataFrame, grid_step_arcmin: float) -> Tuple[pd.DataFrame, pd.DataFrame]:
    step_deg = grid_step_arcmin / 60.0
    dec_bucket = np.floor((df["dec"].astype(float) + 90.0) / step_deg).astype(int)
    cosd = np.clip(np.cos(np.deg2rad(df["dec"].astype(float))), 1e-4, 1.0)
    step_ra = step_deg / cosd
    ra_bucket = np.floor((df["ra"].astype(float) % 360.0) / step_ra).astype(int)

    out = df.copy()
    spatial = (dec_bucket.astype(str) + "_" + ra_bucket.astype(str)).values
    out["field_id"] = (pd.Series(spatial) + "__" + out["plate_id"].astype(str)).values

    def _mode(s: pd.Series):
        m = s.mode()
        return m.iloc[0] if len(m) else None

    centers = (
        out.groupby("field_id", as_index=False)
        .agg(
            ra_f=("ra", "median"),
            dec_f=("dec", "median"),
            epoch_iso=("epoch_iso", _mode),
            epoch_jd=("epoch_jd", _mode),
            n=("ra", "size"),
        )
    )
    return out, centers


def votable_query_status(votxt: str) -> Tuple[str, str]:
    try:
        root = ET.fromstring(votxt)
    except Exception:
        return "UNKNOWN", "xml_parse_failed"

    statuses: List[Tuple[str, str]] = []
    for info in root.findall(".//{*}INFO"):
        name = (info.get("name") or "").strip().upper()
        if name == "QUERY_STATUS":
            val = (info.get("value") or "").strip().upper()
            msg = (info.text or "").strip()
            statuses.append((val, msg))

    if not statuses:
        return "UNKNOWN", "no_QUERY_STATUS"

    for val, msg in statuses:
        if val == "ERROR" and "No solar system object was found" in msg:
            return "EMPTY", msg

    for val, msg in reversed(statuses):
        if val == "ERROR":
            return "ERROR", msg or "QUERY_STATUS=ERROR"

    for val, msg in reversed(statuses):
        if val == "OK":
            return "OK", msg

    return "UNKNOWN", f"QUERY_STATUS={statuses[-1][0]}"


def sexa_ra_to_deg(s: str) -> Optional[float]:
    if not s:
        return None
    t = s.strip().replace(":", " ").split()
    try:
        hh = float(t[0]); mm = float(t[1]) if len(t) > 1 else 0.0; ss = float(t[2]) if len(t) > 2 else 0.0
        return 15.0 * (hh + mm / 60.0 + ss / 3600.0)
    except Exception:
        return None


def sexa_dec_to_deg(s: str) -> Optional[float]:
    if not s:
        return None
    t = s.strip().replace(":", " ").split()
    try:
        dd0 = float(t[0])
        sign = -1.0 if dd0 < 0 else 1.0
        dd = abs(dd0); mm = float(t[1]) if len(t) > 1 else 0.0; ss = float(t[2]) if len(t) > 2 else 0.0
        return sign * (dd + mm / 60.0 + ss / 3600.0)
    except Exception:
        return None


def parse_skybot_votable_radec(votxt: str) -> List[Tuple[float, float]]:
    out: List[Tuple[float, float]] = []
    try:
        root = ET.fromstring(votxt)
    except Exception:
        return out

    fields = root.findall(".//{*}FIELD")
    if not fields:
        return out

    idx: Dict[str, int] = {}
    for i, f in enumerate(fields):
        fid = (f.get("ID") or "").strip().lower()
        fname = (f.get("name") or "").strip().lower()
        if fid:
            idx[fid] = i
        if fname:
            idx[fname] = i

    ira = idx.get("_raj2000")
    ide = idx.get("_decj2000")
    ira_s = idx.get("ra")
    ide_s = idx.get("dec") or idx.get("de")

    for tr in root.findall(".//{*}TABLEDATA/{*}TR"):
        tds = tr.findall("{*}TD")
        if not tds:
            continue
        ra = dec = None
        if ira is not None and ide is not None and len(tds) > max(ira, ide):
            try:
                ra = float((tds[ira].text or "").strip())
                dec = float((tds[ide].text or "").strip())
            except Exception:
                ra = dec = None
        if (ra is None or dec is None) and ira_s is not None and ide_s is not None and len(tds) > max(ira_s, ide_s):
            ra_txt = (tds[ira_s].text or "").strip()
            de_txt = (tds[ide_s].text or "").strip()
            ra = sexa_ra_to_deg(ra_txt)
            dec = sexa_dec_to_deg(de_txt)
        if ra is None or dec is None:
            continue
        if 0.0 <= ra <= 360.0 and -90.0 <= dec <= 90.0:
            out.append((ra, dec))
    return out


def call_skybot(ra: float, dec: float, epoch_jd: float, rs_arcsec: float,
               ct: float, rt: float, max_retries: int, verbose=False) -> Tuple[int, List[Tuple[float, float]]]:
    params = {
        "-ep": f"{float(epoch_jd):.6f}",
        "-ra": f"{float(ra):.8f}",
        "-dec": f"{float(dec):.8f}",
        "-rs": f"{float(rs_arcsec):.3f}",
        "-mime": "votable",
        "-output": "all",
        "-refsys": "EQJ2000",
        "-observer": "500",
        "-from": "vasco",
    }

    tries = 0
    while True:
        try:
            if verbose:
                print(f"[HTTP] ra={params['-ra']} dec={params['-dec']} ep={params['-ep']} rs={params['-rs']}", flush=True)
            r = requests.get(SKYBOT_URL, params=params, timeout=(ct, rt))
            status = r.status_code
            if status == 200:
                txt = r.text
                qs, _msg = votable_query_status(txt)
                if qs == "EMPTY":
                    return 200, []
                if qs == "ERROR":
                    return 422, []
                objs = parse_skybot_votable_radec(txt)
                return 200, objs
            if status in (429, 500, 502, 503, 504) and tries < max_retries:
                time.sleep(2.0 * (tries + 1))
                tries += 1
                continue
            return status, []
        except requests.RequestException:
            if tries < max_retries:
                time.sleep(2.0 * (tries + 1))
                tries += 1
                continue
            return -1, []


def angular_sep_arcsec(ra0_deg: float, de0_deg: float, ra1_deg: np.ndarray, de1_deg: np.ndarray) -> np.ndarray:
    dra = ((ra1_deg - ra0_deg + 180.0) % 360.0) - 180.0
    cd = math.cos(math.radians((de0_deg + float(np.median(de1_deg))) / 2.0))
    d_ra_rad = np.deg2rad(dra * cd)
    d_dec_rad = np.deg2rad(de1_deg - de0_deg)
    return np.hypot(d_ra_rad, d_dec_rad) * (180.0 / np.pi) * 3600.0


def main() -> int:
    args = parse_args()
    run_dir = Path(args.run_dir)
    if not run_dir.exists():
        raise SystemExit(f"run-dir not found: {run_dir}")

    dirs = ensure_dirs(run_dir)

    inputs = sorted(run_dir.glob(args.input_glob))
    if not inputs:
        raise SystemExit(f"No inputs matched: {run_dir}/{args.input_glob}")

    # Single effective input (concatenate) but de-dup by src_id
    all_df = []
    for p in inputs:
        all_df.append(load_input_csv(p))
    df = pd.concat(all_df, ignore_index=True)
    df = df.drop_duplicates(subset=["src_id"], keep="first").reset_index(drop=True)

    # Enrich epoch and group fields
    df = enrich_epoch(df, Path(args.tile_to_plate), Path(args.plate_epoch), verbose=args.verbose)
    df, centers = grid_fields(df, args.grid_step_arcmin)

    stage = args.stage
    tag = args.tag.strip()
    if not tag:
        # derive a stable tag
        stem = inputs[0].stem if len(inputs) == 1 else f"multi_{len(inputs)}"
        tag = f"{stage}__{stem}"

    parts_path = dirs["parts"] / f"flags_skybot__{tag}.parquet"
    audit_path = dirs["audit"] / f"skybot_audit__{tag}.parquet"
    ledger_path = dirs["ledger"] / f"skybot_ledger__{tag}.json"

    # Idempotent: if parts exists, do not requery; just rebuild stage outputs from parts
    need_query = not (parts_path.exists() and parts_path.stat().st_size > 0)

    rs_arcsec = float(args.query_radius_arcmin) * 60.0
    strict = float(args.match_arcsec)
    wide = float(args.fallback_wide_arcsec)

    fallback_per_row = str(args.fallback_per_row).strip().lower() in ("1", "true", "yes", "y", "on")

    t0 = time.time()

    fetched: Dict[str, List[Tuple[float, float]]] = {}
    statuses: Dict[str, int] = {}
    aud_rows = []

    fields_ok = 0
    http_errors = 0
    rate_limits = 0

    if need_query:
        print(f"[SKYBOT] querying fields={len(centers)} rows_in={len(df)} tag={tag}", flush=True)
        for i, row in enumerate(centers.itertuples(index=False), start=1):
            fid = row.field_id
            n_in_field = int(df[df["field_id"] == fid].shape[0])
            if args.verbose:
                print(f"[FIELD {i}/{len(centers)}] fid={fid} n={n_in_field}", flush=True)
            status, objs = call_skybot(
                float(row.ra_f), float(row.dec_f), float(row.epoch_jd),
                rs_arcsec, args.connect_timeout, args.read_timeout, args.max_retries,
                verbose=args.verbose,
            )
            statuses[fid] = int(status)
            fetched[fid] = objs
            aud_rows.append({
                "tag": tag,
                "field_id": fid,
                "grid_step_arcmin": float(args.grid_step_arcmin),
                "query_radius_arcmin": float(args.query_radius_arcmin),
                "http_status": int(status),
                "returned_rows": int(len(objs)),
                "n_candidates": int(n_in_field),
            })
            if status == 200:
                fields_ok += 1
            elif status == 429:
                rate_limits += 1
            else:
                http_errors += 1

        # Build per-row flags
        out_rows = []
        fb_attempted = 0
        fb_matched = 0

        # Eligible for per-row fallback: fields with 200 and 0 objects
        empty_fields = {fid for fid, st in statuses.items() if st == 200 and len(fetched.get(fid, [])) == 0}

        for fid, sub in df.groupby("field_id"):
            objs = fetched.get(fid, [])
            if objs:
                objs_arr = np.array(objs, dtype=float)
                for _, r in sub.iterrows():
                    ra0 = float(r["ra"]); de0 = float(r["dec"])
                    seps = angular_sep_arcsec(ra0, de0, objs_arr[:, 0], objs_arr[:, 1])
                    best_sep = float(np.min(seps)) if seps.size else None
                    nmatch = int(np.sum(seps <= wide))
                    is_strict = (best_sep is not None) and (best_sep <= strict)
                    is_wide = (best_sep is not None) and (not is_strict) and (best_sep <= wide)
                    out_rows.append({
                        "src_id": r["src_id"],
                        "tile_id": r["tile_id"],
                        "object_id": int(r["object_id"]),
                        "plate_id": r["plate_id"],
                        "has_skybot_match": bool(is_strict),
                        "wide_skybot_match": bool(is_wide),
                        "matched_count": int(nmatch),
                        "best_sep_arcsec": best_sep if seps.size else None,
                        "epoch_used": r["epoch_iso"] if pd.notna(r["epoch_iso"]) else r["epoch_jd"],
                    })
                continue

            # Optional per-row fallback only when field returned 200/0
            if (fid in empty_fields) and fallback_per_row and (fb_attempted < args.fallback_per_row_cap):
                n_left = int(args.fallback_per_row_cap - fb_attempted)
                sub_probe = sub.head(n_left)
                print(f"[FALLBACK] per-row cones for field={fid} rows={len(sub_probe)} (cap left {n_left})", flush=True)
                for _, r in sub_probe.iterrows():
                    fb_attempted += 1
                    status, ob_list = call_skybot(
                        float(r["ra"]), float(r["dec"]), float(r["epoch_jd"]),
                        strict, args.connect_timeout, args.read_timeout, args.max_retries,
                        verbose=args.verbose,
                    )
                    best_sep = None
                    nmatch = 0
                    is_strict = False
                    if ob_list:
                        objs_arr = np.array(ob_list, dtype=float)
                        seps = angular_sep_arcsec(float(r["ra"]), float(r["dec"]), objs_arr[:, 0], objs_arr[:, 1])
                        best_sep = float(np.min(seps)) if seps.size else None
                        nmatch = int(np.sum(seps <= strict)) if seps.size else 0
                        is_strict = (best_sep is not None) and (best_sep <= strict)
                    if is_strict:
                        fb_matched += 1
                    out_rows.append({
                        "src_id": r["src_id"],
                        "tile_id": r["tile_id"],
                        "object_id": int(r["object_id"]),
                        "plate_id": r["plate_id"],
                        "has_skybot_match": bool(is_strict),
                        "wide_skybot_match": False,
                        "matched_count": int(nmatch),
                        "best_sep_arcsec": best_sep,
                        "epoch_used": r["epoch_iso"] if pd.notna(r["epoch_iso"]) else r["epoch_jd"],
                    })

                # remainder beyond cap -> unmatched
                if len(sub) > len(sub_probe):
                    remainder = sub.iloc[len(sub_probe):]
                    for _, r in remainder.iterrows():
                        out_rows.append({
                            "src_id": r["src_id"],
                            "tile_id": r["tile_id"],
                            "object_id": int(r["object_id"]),
                            "plate_id": r["plate_id"],
                            "has_skybot_match": False,
                            "wide_skybot_match": False,
                            "matched_count": 0,
                            "best_sep_arcsec": None,
                            "epoch_used": r["epoch_iso"] if pd.notna(r["epoch_iso"]) else r["epoch_jd"],
                        })
                continue

            # Default unmatched
            for _, r in sub.iterrows():
                out_rows.append({
                    "src_id": r["src_id"],
                    "tile_id": r["tile_id"],
                    "object_id": int(r["object_id"]),
                    "plate_id": r["plate_id"],
                    "has_skybot_match": False,
                    "wide_skybot_match": False,
                    "matched_count": 0,
                    "best_sep_arcsec": None,
                    "epoch_used": r["epoch_iso"] if pd.notna(r["epoch_iso"]) else r["epoch_jd"],
                })

        parts_df = pd.DataFrame(out_rows)
        pq.write_table(pa.Table.from_pandas(parts_df, preserve_index=False), parts_path, compression="zstd")

        audit_df = pd.DataFrame(aud_rows)
        pq.write_table(pa.Table.from_pandas(audit_df, preserve_index=False), audit_path, compression="zstd")

        elapsed = round(time.time() - t0, 3)
        ledger = {
            "tag": tag,
            "ts_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "rows_in": int(len(df)),
            "fields_total": int(len(centers)),
            "fields_ok": int(fields_ok),
            "http_errors": int(http_errors),
            "rate_limits": int(rate_limits),
            "fallback_per_row": bool(fallback_per_row),
            "fallback_per_row_cap": int(args.fallback_per_row_cap),
            "fallback_rows_attempted": int(fb_attempted),
            "fallback_rows_matched": int(fb_matched),
            "elapsed_s": elapsed,
            "params": {
                "grid_step_arcmin": float(args.grid_step_arcmin),
                "query_radius_arcmin": float(args.query_radius_arcmin),
                "local_match_arcsec": float(args.match_arcsec),
                "fallback_wide_arcsec": float(args.fallback_wide_arcsec),
                "connect_timeout_s": float(args.connect_timeout),
                "read_timeout_s": float(args.read_timeout),
                "max_retries": int(args.max_retries),
                "endpoint": SKYBOT_URL,
                "mime": "votable",
            },
            "artifacts": {
                "parts": str(parts_path),
                "audit": str(audit_path),
            },
        }
        ledger_path.write_text(json.dumps(ledger, indent=2), encoding="utf-8")

    # Load parts (either newly written or existing) and write stage outputs
    parts_df = pd.read_parquet(parts_path)

    # Stage flags CSV (all rows)
    flags_out = dirs["stages"] / f"stage_{stage}_SKYBOT_flags.csv"
    keep_out = dirs["stages"] / f"stage_{stage}_SKYBOT.csv"
    ledg_out = dirs["stages"] / f"stage_{stage}_SKYBOT_ledger.json"

    # Build a compact flags view
    flags_view = parts_df[[
        "src_id",
        "has_skybot_match",
        "wide_skybot_match",
        "matched_count",
        "best_sep_arcsec",
        "epoch_used",
    ]].copy()

    # re-attach ra/dec from the original df (stable join by src_id)
    ra_dec = df[["src_id", "ra", "dec"]].drop_duplicates("src_id")
    flags_view = flags_view.merge(ra_dec, on="src_id", how="left")
    flags_view = flags_view[[
        "src_id", "ra", "dec",
        "has_skybot_match", "wide_skybot_match",
        "matched_count", "best_sep_arcsec", "epoch_used"
    ]]
    flags_view.to_csv(flags_out, index=False)

    # Kept remainder: drop strict matches
    kept = flags_view[~flags_view["has_skybot_match"].astype(bool)][["src_id", "ra", "dec"]]
    kept.to_csv(keep_out, index=False)

    totals = {
        "input_rows": int(flags_view.shape[0]),
        "matched_strict": int(flags_view["has_skybot_match"].astype(bool).sum()),
        "matched_wide": int(flags_view["wide_skybot_match"].astype(bool).sum()),
        "kept_rows": int(kept.shape[0]),
    }

    stage_ledger = {
        "run_dir": str(run_dir),
        "input_glob": args.input_glob,
        "stage": stage,
        "tag": tag,
        "totals": totals,
        "params": {
            "grid_step_arcmin": float(args.grid_step_arcmin),
            "query_radius_arcmin": float(args.query_radius_arcmin),
            "match_arcsec": float(args.match_arcsec),
            "fallback_wide_arcsec": float(args.fallback_wide_arcsec),
            "fallback_per_row": bool(fallback_per_row),
            "fallback_per_row_cap": int(args.fallback_per_row_cap),
        },
        "artifacts": {
            "parts_parquet": str(parts_path),
            "audit_parquet": str(audit_path),
            "ledger_json": str(ledger_path),
        },
        "outputs": {
            "stage_csv": str(keep_out),
            "flags_csv": str(flags_out),
            "stage_ledger_json": str(ledg_out),
        },
    }
    ledg_out.write_text(json.dumps(stage_ledger, indent=2), encoding="utf-8")

    print(f"[SKYBOT] input_rows={totals['input_rows']} matched={totals['matched_strict']} kept={totals['kept_rows']} tag={tag}")
    print(f"[SKYBOT] wrote: {keep_out}")
    print(f"[SKYBOT] wrote: {flags_out}")
    print(f"[SKYBOT] wrote: {ledg_out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
