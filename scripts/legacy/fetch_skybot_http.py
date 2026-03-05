#!/usr/bin/env python3
"""
SkyBoT per-candidate flagger (HTTP conesearch), resume-safe.

Outputs Parquet with at least:
  NUMBER, is_skybot, query_radius_arcsec, epoch_used, matched_count

Reasonable defaults:
  --radius-arcsec 5  (identity-style match)
  --workers 2        (gentle to the service; raise if needed)
"""

import argparse, os, sys, json, random
from time import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import requests

API = "https://vo.imcce.fr/webservices/skybot/skybotconesearch_query.php"

# ---------- helpers ----------

def pick_cols(schema_names):
    """Pick RA/Dec/ID columns from a master dataset schema."""
    names = set(schema_names)
    ra  = next((c for c in ("RA_corr","RA","ALPHAWIN_J2000","ALPHA_J2000","X_WORLD") if c in names), None)
    dec = next((c for c in ("Dec_corr","DEC","DELTAWIN_J2000","DELTA_J2000","Y_WORLD") if c in names), None)
    key = next((c for c in ("NUMBER","row_id","source_id") if c in names), None)
    reg = next((c for c in ("REGION","plate_id") if c in names), None)
    return ra, dec, key, reg

def read_master_minimal(master_path):
    """Project only needed columns, dedup by NUMBER (string)."""
    dataset = ds.dataset(master_path, format="parquet")
    ra, dec, key, reg = pick_cols(dataset.schema.names)
    if not (ra and dec and key):
        raise SystemExit("Master dataset does not contain identifiable RA/Dec/ID columns.")

    # Project minimal columns per fragment
    frames = []
    needed = [c for c in {key, ra, dec, reg} if c is not None]
    for frag in dataset.get_fragments():
        tbl = frag.to_table(columns=needed)
        df  = tbl.to_pandas()
        df = df.dropna(subset=[ra, dec, key])
        df = df.rename(columns={key:"NUMBER", ra:"ra", dec:"dec"})
        df["NUMBER"] = df["NUMBER"].astype(str)
        if reg:
            df["REGION"] = df[reg]
        frames.append(df[["NUMBER","ra","dec"] + (["REGION"] if "REGION" in df.columns else [])])

    if not frames:
        raise SystemExit("Master scan produced zero rows after projection.")
    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates("NUMBER")
    return df

def load_epoch_table(epoch_parquet, epoch_lookup, epoch_iso, master_df, save_epoch_parquet=None):
    """
    Return DataFrame with columns: NUMBER, epoch_used (ISO or JD as str).
    Priority:
      1) epoch_parquet (if exists)
      2) derive from epoch_lookup via REGION/plate_id in master
      3) epoch_iso applied to all
    """
    if epoch_parquet and os.path.exists(epoch_parquet):
        ep = pq.read_table(epoch_parquet).to_pandas()
        ep_cols = {c.lower(): c for c in ep.columns}
        if not set(ep.columns) & {"epoch_utc","epoch_iso","epoch_jd"}:
            # try common names
            for cand in ("epoch","date_obs","DATE-OBS","DATE_OBS"):
                if cand in ep.columns:
                    ep["epoch_iso"] = ep[cand]
                    ep_cols["epoch_iso"] = "epoch_iso"
                    break
        epoch_col = "epoch_iso" if "epoch_iso" in ep.columns or "epoch_utc" in ep.columns else "epoch_jd"
        if "epoch_utc" in ep.columns and "epoch_iso" not in ep.columns:
            ep["epoch_iso"] = ep["epoch_utc"]
            epoch_col = "epoch_iso"
        if "NUMBER" not in ep.columns:
            raise SystemExit("--epoch-parquet must include NUMBER column.")
        out = ep[["NUMBER", epoch_col]].dropna().drop_duplicates("NUMBER").copy()
        out = out.rename(columns={epoch_col:"epoch_used"})
        out["NUMBER"] = out["NUMBER"].astype(str)
        return out

    if epoch_lookup and os.path.exists(epoch_lookup):
        lk = pq.read_table(epoch_lookup).to_pandas()
        lk_cols = {c.lower(): c for c in lk.columns}
        if "region" not in lk_cols and "plate_id" not in lk_cols:
            raise SystemExit("--epoch-lookup must have REGION or plate_id for joining.")
        key_col = lk_cols.get("region", lk_cols.get("plate_id"))
        # epoch source preference: epoch_iso/DATE-OBS > epoch_jd > MJD
        if "epoch_iso" in lk.columns:
            lk["epoch_used"] = lk["epoch_iso"]
        elif "DATE-OBS" in lk.columns:
            lk["epoch_used"] = lk["DATE-OBS"]
        elif "epoch_jd" in lk.columns:
            lk["epoch_used"] = lk["epoch_jd"].astype(str)
        elif "MJD" in lk.columns:
            # JD = MJD + 2400000.5 ; SkyBoT accepts JD as numeric string
            lk["epoch_used"] = (lk["MJD"].astype(float) + 2400000.5).astype(str)
        else:
            raise SystemExit("--epoch-lookup needs one of: epoch_iso, DATE-OBS, epoch_jd, MJD")

        if "REGION" not in master_df.columns:
            raise SystemExit("Master lacks REGION/plate_id to join with --epoch-lookup.")
        merged = master_df[["NUMBER","REGION"]].merge(
            lk[[key_col, "epoch_used"]], left_on="REGION", right_on=key_col, how="left"
        )
        out = merged[["NUMBER","epoch_used"]].dropna().drop_duplicates("NUMBER")
        if save_epoch_parquet:
            pq.write_table(pa.Table.from_pandas(out, preserve_index=False), save_epoch_parquet)
        return out

    if epoch_iso:
        # apply single epoch to all
        out = master_df[["NUMBER"]].copy()
        out["epoch_used"] = epoch_iso
        return out

    raise SystemExit("No epoch source available. Provide --epoch-parquet OR --epoch-lookup OR --epoch-iso.")

def skybot_query_one(ra_deg, dec_deg, epoch_used, radius_arcsec, timeout_s=10, max_retries=4, base_backoff=0.5):
    """Return matched_count (int) or None on failure after retries."""
    params = {
        "-ep": str(epoch_used),         # ISO string or JD numeric string
        "-ra": f"{ra_deg}",
        "-dec": f"{dec_deg}",
        "-rs": f"{int(radius_arcsec)}", # arcsec
        "-mime": "text",
        "-output": "all",
        "-loc": "500",
        "-filter": "120",
        "-objFilter": "110",
        "-refsys": "EQJ2000",
        "-from": "VASCO",
    }
    for attempt in range(max_retries + 1):
        try:
            r = requests.get(API, params=params, timeout=timeout_s)
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"transient {r.status_code}")
            r.raise_for_status()
            # Count non-empty, non-comment lines
            cnt = 0
            for ln in r.text.splitlines():
                s = ln.strip()
                if not s or s.startswith("#"):
                    continue
                cnt += 1
            return cnt
        except Exception:
            if attempt == max_retries:
                return None
            sleep(base_backoff * (2 ** attempt) + random.uniform(0, 0.2))

def write_parquet(df, out_path):
    table = pa.Table.from_pandas(df, preserve_index=False)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    pq.write_table(table, out_path)

# ---------- main ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True, help="Parquet dataset (dir or file) containing candidates")
    ap.add_argument("--epoch-parquet", help="Per-source epochs: NUMBER + (epoch_iso|epoch_utc|epoch_jd)")
    ap.add_argument("--epoch-lookup", help="Plate epochs: REGION/plate_id + (DATE-OBS|epoch_iso|epoch_jd|MJD)")
    ap.add_argument("--epoch-iso", help="Fallback single epoch for all rows (ISO-8601)")
    ap.add_argument("--save-epoch-parquet", help="If set and epochs were derived, save here")

    ap.add_argument("--out", required=True, help="Output Parquet flags")
    ap.add_argument("--radius-arcsec", type=float, default=5.0, help="Cone radius in arcsec (default 5)")
    ap.add_argument("--max-rows", type=int, default=0, help="Debug limiter; 0 = all")
    ap.add_argument("--workers", type=int, default=2, help="Parallel requests (default 2)")
    ap.add_argument("--checkpoint", default="./work/skybot_checkpoint.jsonl", help="Resume file (JSONL)")

    args = ap.parse_args()

    master_df = read_master_minimal(args.master)
    epochs_df  = load_epoch_table(args.epoch_parquet, args.epoch_lookup, args.epoch_iso, master_df, args.save_epoch_parquet)

    df = master_df.merge(epochs_df, on="NUMBER", how="inner")
    if args.max_rows:
        df = df.head(args.max_rows)

    # Resume: load already done IDs
    done = set()
    if os.path.exists(args.checkpoint):
        with open(args.checkpoint, "r", encoding="utf-8") as f:
            for ln in f:
                try:
                    j = json.loads(ln)
                    done.add(str(j["NUMBER"]))
                except Exception:
                    pass

    todo = df[~df["NUMBER"].astype(str).isin(done)].to_dict("records")
    out_rows = []

    # Worker pool
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(skybot_query_one, r["ra"], r["dec"], r["epoch_used"], args.radius_arcsec): r for r in todo}
        with open(args.checkpoint, "a", encoding="utf-8") as ck:
            for fut in as_completed(futs):
                r = futs[fut]
                mcnt = fut.result()  # int or None
                if mcnt is None:
                    # transient failure not checkpointed; re-run later
                    continue
                out_rows.append({
                    "NUMBER": str(r["NUMBER"]),
                    "is_skybot": bool(mcnt > 0),
                    "matched_count": int(mcnt),
                    "query_radius_arcsec": float(args.radius_arcsec),
                    "epoch_used": str(r["epoch_used"]),
                })
                ck.write(json.dumps({"NUMBER": str(r["NUMBER"])}) + "\n")

    # Include previously done rows if they exist in an older output? Keep it simple: emit only current batch.
    out_df = pd.DataFrame(out_rows).drop_duplicates("NUMBER")
    write_parquet(out_df, args.out)
    print(f"[OK] SkyBoT flags -> {args.out} rows={len(out_df)}  (checkpoint: {args.checkpoint})")

if __name__ == "__main__":
    main()