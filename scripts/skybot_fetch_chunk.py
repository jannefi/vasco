#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SkyBoT fetcher for one survivors chunk (verbose, production-safe) with optional per-row fallback.

Supports TWO input CSV schemas:
  A) New run-scoped publisher chunks:
       src_id,ra,dec
     where src_id == "<tile_id>:<object_id>"

  B) Legacy chunks:
       number,row_id,ra,dec
     where row_id == "<tile_id>:<NUMBER>"

Lookups:
  metadata/tiles/tile_to_plate_lookup.parquet   (tile_id, plate_id)
  metadata/plates/plate_epoch_lookup.parquet    (plate_id, date_obs_iso, jd)

Outputs under --out-root:
  parts/flags_skybot__<chunk>.parquet
  audit/skybot_audit__<chunk>.parquet
  ledger/skybot_ledger__<chunk>.json

Match policy:
  strict: 5 arcsec  -> has_skybot_match = True
  wide:   60 arcsec -> wide_skybot_match = True (strict remains False)
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


def parse_args():
    p = argparse.ArgumentParser(description="Fetch SkyBoT flags for one chunk.")
    p.add_argument("--chunk-csv", required=True)
    p.add_argument("--tile-to-plate", default="metadata/tiles/tile_to_plate_lookup.parquet")
    p.add_argument("--plate-epoch", default="metadata/plates/plate_epoch_lookup.parquet")
    p.add_argument("--out-root", default="data/local-cats/_master_optical_parquet_flags/skybot")

    p.add_argument("--field-radius-arcmin", type=float, default=22.0)
    p.add_argument("--match-arcsec", type=float, default=5.0)
    p.add_argument("--fallback-wide-arcsec", type=float, default=60.0)

    p.add_argument("--fallback-per-row", type=str, default="false")
    p.add_argument("--fallback-per-row-cap", type=int, default=100)

    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--connect-timeout", type=float, default=5.0)
    p.add_argument("--read-timeout", type=float, default=5.0)
    p.add_argument("--max-retries", type=int, default=0)

    p.add_argument("--limit-fields", type=int, default=0)
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()


def vprint(verbose: bool, *a, **k):
    if verbose:
        print(*a, **k, flush=True)


def ensure_dirs(root: Path) -> Dict[str, Path]:
    parts = root / "parts"; parts.mkdir(parents=True, exist_ok=True)
    audit = root / "audit"; audit.mkdir(parents=True, exist_ok=True)
    ledger = root / "ledger"; ledger.mkdir(parents=True, exist_ok=True)
    return {"parts": parts, "audit": audit, "ledger": ledger}


def load_chunk(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    low = {c.lower(): c for c in df.columns}

    # required coords
    if "ra" not in low or "dec" not in low:
        raise SystemExit(f"[ERROR] chunk missing ra/dec: {path}")

    # schema A: src_id
    if "src_id" in low:
        df = df.rename(columns={low["src_id"]: "src_id", low["ra"]: "ra", low["dec"]: "dec"})
        df["src_id"] = df["src_id"].astype(str)
        df["tile_id"] = df["src_id"].str.split(":").str[0]
        df["object_id"] = df["src_id"].str.split(":").str[1].apply(lambda x: int(float(x)))
        return df[["src_id", "tile_id", "object_id", "ra", "dec"]]

    # schema B: legacy row_id + number
    need = {"row_id", "number"}
    if not need.issubset(set(low.keys())):
        raise SystemExit(f"[ERROR] chunk missing columns {need}: {path}")

    df = df.rename(columns={low["row_id"]: "src_id", low["number"]: "object_id", low["ra"]: "ra", low["dec"]: "dec"})
    df["src_id"] = df["src_id"].astype(str)
    df["tile_id"] = df["src_id"].str.split(":").str[0]
    df["object_id"] = df["object_id"].apply(lambda x: int(float(x)))
    return df[["src_id", "tile_id", "object_id", "ra", "dec"]]


def enrich(df: pd.DataFrame, t2p_path: Path, pep_path: Path, verbose=False) -> pd.DataFrame:
    vprint(verbose, "[INFO] Loading lookups …")
    t2p = pd.read_parquet(t2p_path)  # tile_id, plate_id
    pep = pd.read_parquet(pep_path)  # plate_id, date_obs_iso, jd

    df = df.merge(t2p, on="tile_id", how="left", validate="many_to_one")
    if df["plate_id"].isna().any():
        sample = df.loc[df["plate_id"].isna(), "tile_id"].unique()[:10]
        raise SystemExit(f"[ERROR] plate_id missing for tiles: {sample}")

    df = df.merge(pep, on="plate_id", how="left", validate="many_to_one")
    if (df["date_obs_iso"].isna() & df["jd"].isna()).any():
        bad = df.loc[(df["date_obs_iso"].isna() & df["jd"].isna()), "plate_id"].unique()[:10]
        raise SystemExit(f"[ERROR] epoch missing for plate_ids: {bad}")

    df["epoch_iso"] = df["date_obs_iso"]
    df["epoch_jd"] = df["jd"]
    return df


def grid_fields(df: pd.DataFrame, field_radius_arcmin: float) -> Tuple[pd.DataFrame, pd.DataFrame]:
    step_deg = field_radius_arcmin / 60.0
    dec_bucket = np.floor((df["dec"].astype(float) + 90.0) / step_deg).astype(int)
    cosd = np.clip(np.cos(np.deg2rad(df["dec"].astype(float))), 1e-4, 1.0)
    step_ra = step_deg / cosd
    ra_bucket = np.floor((df["ra"].astype(float) % 360.0) / step_ra).astype(int)

    out = df.copy()
    out["field_id"] = (dec_bucket.astype(str) + "_" + ra_bucket.astype(str)).values

    def _mode(s: pd.Series):
        m = s.mode()
        return m.iloc[0] if len(m) else None

    centers = (out.groupby("field_id", as_index=False)
               .agg(ra_f=("ra", "median"),
                    dec_f=("dec", "median"),
                    epoch_iso=("epoch_iso", _mode),
                    epoch_jd=("epoch_jd", _mode),
                    n=("ra", "size")))
    return out, centers


def parse_skybot_text_to_radec_list(text: str) -> List[Tuple[float, float]]:
    out: List[Tuple[float, float]] = []
    for ln in text.splitlines():
        s = ln.strip()
        if not s or s.startswith("#"):
            continue
        toks = s.replace(",", " ").split()
        floats: List[float] = []
        for t in toks:
            try:
                floats.append(float(t))
            except Exception:
                continue
        pair = None
        for i in range(len(floats) - 1):
            ra = floats[i]; de = floats[i + 1]
            if 0.0 <= ra <= 360.0 and -90.0 <= de <= 90.0:
                pair = (ra, de)
                break
        if pair:
            out.append(pair)
    return out


def call_skybot(ra: float, dec: float, epoch_jd: float, rs_arcsec: float,
               ct: float, rt: float, max_retries: int, verbose=False) -> Tuple[int, List[Tuple[float, float]]]:
    params = {
        "-ra": f"{float(ra):.8f}",
        "-dec": f"{float(dec):.8f}",
        "-rs": f"{float(rs_arcsec):.3f}",
        "-ep": f"{float(epoch_jd):.6f}",
        "-mime": "text",
        "-output": "all",
        "-refsys": "EQJ2000",
        "-equinox": "J2000",
    }
    tries = 0
    while True:
        try:
            if verbose:
                print(f"[HTTP] -ra={params['-ra']} -dec={params['-dec']} -ep={params['-ep']} -rs={params['-rs']}", flush=True)
            r = requests.get(SKYBOT_URL, params=params, timeout=(ct, rt))
            status = r.status_code
            if status == 200:
                return status, parse_skybot_text_to_radec_list(r.text)
            if status in (429, 500, 502, 503, 504) and tries < max_retries:
                time.sleep(2.0 * (tries + 1)); tries += 1; continue
            return status, []
        except requests.RequestException:
            if tries < max_retries:
                time.sleep(2.0 * (tries + 1)); tries += 1; continue
            return -1, []


def angular_sep_arcsec(ra0_deg: float, de0_deg: float, ra1_deg: np.ndarray, de1_deg: np.ndarray) -> np.ndarray:
    dra = ((ra1_deg - ra0_deg + 180.0) % 360.0) - 180.0
    cd = math.cos(math.radians((de0_deg + np.median(de1_deg)) / 2.0))
    d_ra_rad = np.deg2rad(dra * cd)
    d_dec_rad = np.deg2rad(de1_deg - de0_deg)
    return np.hypot(d_ra_rad, d_dec_rad) * (180.0 / np.pi) * 3600.0


def process_chunk(args):
    chunk_path = Path(args.chunk_csv)
    chunk_name = chunk_path.stem
    out_dirs = ensure_dirs(Path(args.out_root))

    fallback_per_row = str(args.fallback_per_row).strip().lower() in ("1", "true", "yes", "y", "on")
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

    fetched: Dict[str, List[Tuple[float, float]]] = {}
    statuses: Dict[str, int] = {}
    fields_ok = 0; http_errors = 0; rate_limits = 0

    for i, row in enumerate(centers.itertuples(index=False), start=1):
        fid = row.field_id
        print(f"[FIELD {i}/{len(centers)}] fid={fid} n={int(df[df['field_id']==fid].shape[0])}", flush=True)
        status, objs = call_skybot(row.ra_f, row.dec_f, row.epoch_jd,
                                  args.field_radius_arcmin * 60.0,
                                  args.connect_timeout, args.read_timeout,
                                  args.max_retries, verbose=args.verbose)
        statuses[fid] = status
        fetched[fid] = objs
        if status == 200: fields_ok += 1
        elif status == 429: rate_limits += 1
        else: http_errors += 1

    strict = float(args.match_arcsec)
    wide = float(args.fallback_wide_arcsec)

    out_rows = []
    aud_rows = []
    rows_matched_5 = 0
    rows_matched_60 = 0

    fb_attempted = 0
    fb_matched = 0
    fb_http_err = 0

    empty_fields = {fid for fid, objs in fetched.items() if statuses.get(fid, -1) == 200 and len(objs) == 0}

    for fid, sub in df.groupby("field_id"):
        objs = fetched.get(fid, [])
        aud_rows.append({
            "chunk": chunk_name,
            "field_id": fid,
            "field_radius_arcmin": float(args.field_radius_arcmin),
            "http_status": int(statuses.get(fid, -1)),
            "returned_rows": int(len(objs)),
        })

        if objs:
            objs_arr = np.array(objs, dtype=float)
            for _, r in sub.iterrows():
                ra0 = float(r["ra"]); de0 = float(r["dec"])
                seps = angular_sep_arcsec(ra0, de0, objs_arr[:, 0], objs_arr[:, 1]) if objs_arr.size else np.array([])
                best_sep = float(np.min(seps)) if seps.size else None
                nmatch = int(np.sum(seps <= wide)) if seps.size else 0

                is_strict = (best_sep is not None) and (best_sep <= strict)
                is_wide = (best_sep is not None) and (not is_strict) and (best_sep <= wide)

                if is_strict: rows_matched_5 += 1
                if is_wide: rows_matched_60 += 1

                out_rows.append({
                    "src_id": r["src_id"],
                    "object_id": int(r["object_id"]),
                    "tile_id": r["tile_id"],
                    "plate_id": r["plate_id"],
                    "has_skybot_match": bool(is_strict),
                    "wide_skybot_match": bool(is_wide),
                    "matched_count": int(nmatch),
                    "best_sep_arcsec": best_sep if seps.size else None,
                    "epoch_used": r["epoch_iso"] if pd.notna(r["epoch_iso"]) else r["epoch_jd"],
                    "source_chunk": chunk_name,
                })
            continue

        # optional per-row fallback only when field returned 200/0
        if (fid in empty_fields) and fallback_per_row and (fb_attempted < args.fallback_per_row_cap):
            n_left = int(args.fallback_per_row_cap - fb_attempted)
            sub_probe = sub.head(n_left)
            print(f"[FALLBACK] per-row cones for field={fid} rows={len(sub_probe)} (cap left {n_left})", flush=True)

            for _, r in sub_probe.iterrows():
                fb_attempted += 1
                status, ob_list = call_skybot(float(r["ra"]), float(r["dec"]), float(r["epoch_jd"]),
                                              strict,
                                              args.connect_timeout, args.read_timeout,
                                              args.max_retries, verbose=args.verbose)
                if status not in (200, 429) and status not in (-1, 0):
                    fb_http_err += 1

                best_sep = None
                nmatch = 0
                is_strict = False
                if ob_list:
                    objs_arr = np.array(ob_list, dtype=float)
                    seps = angular_sep_arcsec(float(r["ra"]), float(r["dec"]), objs_arr[:, 0], objs_arr[:, 1]) if objs_arr.size else np.array([])
                    best_sep = float(np.min(seps)) if seps.size else None
                    nmatch = int(np.sum(seps <= strict)) if seps.size else 0
                    is_strict = (best_sep is not None) and (best_sep <= strict)

                if is_strict:
                    rows_matched_5 += 1
                    fb_matched += 1

                out_rows.append({
                    "src_id": r["src_id"],
                    "object_id": int(r["object_id"]),
                    "tile_id": r["tile_id"],
                    "plate_id": r["plate_id"],
                    "has_skybot_match": bool(is_strict),
                    "wide_skybot_match": False,
                    "matched_count": int(nmatch),
                    "best_sep_arcsec": best_sep,
                    "epoch_used": r["epoch_iso"] if pd.notna(r["epoch_iso"]) else r["epoch_jd"],
                    "source_chunk": chunk_name,
                })

            # remainder beyond cap -> unmatched
            if len(sub) > len(sub_probe):
                remainder = sub.iloc[len(sub_probe):]
                for _, r in remainder.iterrows():
                    out_rows.append({
                        "src_id": r["src_id"],
                        "object_id": int(r["object_id"]),
                        "tile_id": r["tile_id"],
                        "plate_id": r["plate_id"],
                        "has_skybot_match": False,
                        "wide_skybot_match": False,
                        "matched_count": 0,
                        "best_sep_arcsec": None,
                        "epoch_used": r["epoch_iso"] if pd.notna(r["epoch_iso"]) else r["epoch_jd"],
                        "source_chunk": chunk_name,
                    })
            continue

        # default unmatched
        for _, r in sub.iterrows():
            out_rows.append({
                "src_id": r["src_id"],
                "object_id": int(r["object_id"]),
                "tile_id": r["tile_id"],
                "plate_id": r["plate_id"],
                "has_skybot_match": False,
                "wide_skybot_match": False,
                "matched_count": 0,
                "best_sep_arcsec": None,
                "epoch_used": r["epoch_iso"] if pd.notna(r["epoch_iso"]) else r["epoch_jd"],
                "source_chunk": chunk_name,
            })

    parts_df = pd.DataFrame(out_rows)
    parts_path = out_dirs["parts"] / f"flags_skybot__{chunk_name}.parquet"
    pq.write_table(pa.Table.from_pandas(parts_df, preserve_index=False), parts_path, compression="zstd")

    audit_df = pd.DataFrame(aud_rows)
    audit_path = out_dirs["audit"] / f"skybot_audit__{chunk_name}.parquet"
    pq.write_table(pa.Table.from_pandas(audit_df, preserve_index=False), audit_path, compression="zstd")

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
        "rows_matched_60as": int(rows_matched_60),
        "fallback_per_row": bool(fallback_per_row),
        "fallback_per_row_cap": int(args.fallback_per_row_cap),
        "fallback_rows_attempted": int(fb_attempted),
        "fallback_rows_matched": int(fb_matched),
        "fallback_http_errors": int(fb_http_err),
        "elapsed_s": elapsed,
        "params": {
            "field_radius_arcmin": float(args.field_radius_arcmin),
            "local_match_arcsec": float(args.match_arcsec),
            "fallback_wide_arcsec": float(args.fallback_wide_arcsec),
            "connect_timeout_s": float(args.connect_timeout),
            "read_timeout_s": float(args.read_timeout),
            "max_retries": int(args.max_retries),
        }
    }
    ledger_path = out_dirs["ledger"] / f"skybot_ledger__{chunk_name}.json"
    ledger_path.write_text(json.dumps(ledger, indent=2), encoding="utf-8")

    print(f"[DONE] {chunk_name} parts={parts_path} audit={audit_path} ledger={ledger_path} "
          f"matched_5as={rows_matched_5} matched_60as={rows_matched_60} fb_rows={fb_attempted}/{fb_matched}",
          flush=True)


if __name__ == "__main__":
    args = parse_args()
    process_chunk(args)
