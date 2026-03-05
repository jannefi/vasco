
#!/usr/bin/env python3
"""
PTF flags via IRSA Gator (fallback when TAP is slow/unavailable).

- Primary attempt: Gator spatial=Upload (CSV upload); robust error surfacing.
- Fallback: threaded per-row cone queries with visible progress.
- Output: Parquet flags with columns: NUMBER, has_other_archive_match

Usage:
  python scripts/fetch_ptf_via_gator.py \
      --positions-csv ./work/positions_upload.csv \
      --out ./data/local-cats/_master_optical_parquet_flags/flags_ptf_objects.parquet \
      --catalog ptf_objects \
      --radius-arcsec 5 \
      --workers 12 --log-every 50
"""
import argparse, io, sys, time, math, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import pandas as pd
import pyarrow as pa, pyarrow.parquet as pq

GATOR_URL = "https://irsa.ipac.caltech.edu/cgi-bin/Gator/nph-query"

def _strip_comments(text: str) -> str:
    return "\n".join([ln for ln in text.splitlines() if not ln.strip().startswith("#")])

def _has_irsa_error(text: str) -> tuple[bool, str]:
    t = text.strip()
    if 'stat="ERROR"' in t or t.startswith("ERROR"):
        msg = t
        if 'msg="' in t:
            try:
                msg = t.split('msg="',1)[1].split('"',1)[0]
            except Exception:
                pass
        return True, msg
    return False, ""

def _write_flags(out_path: str, numbers: pd.Series):
    numbers = numbers.dropna().astype(str).drop_duplicates()
    flag = pd.DataFrame({"NUMBER": numbers, "has_other_archive_match": True})
    pq.write_table(pa.Table.from_pandas(flag, preserve_index=False), out_path)
    print(f"[OK] PTF flags -> {out_path} rows={len(flag)}")

def _gator_upload(df: pd.DataFrame, catalog: str, radius_arcsec: float, timeout: float) -> pd.DataFrame:
    # Upload attempt (may fail with "No upload table is defined")
    files = {"uploadfile": ("positions.csv", df.to_csv(index=False), "text/plain")}
    data = {
        "outfmt": "1",              # CSV
        "spatial": "Upload",
        "radius": f"{radius_arcsec}",
        "radunits": "arcsec",
        "catalog": catalog,
        "objstr": "",               # ignored with upload
        "selcols": "ra,dec,rowNum,cntr"
    }
    r = requests.post(GATOR_URL, data=data, files=files, timeout=timeout)
    r.raise_for_status()
    text = _strip_comments(r.text)
    is_err, msg = _has_irsa_error(text)
    if is_err:
        # Retry without selcols and different MIME (some gateways are picky)
        files_alt = {"uploadfile": ("upload.tbl", df.to_csv(index=False), "application/octet-stream")}
        data_alt = {k:v for k,v in data.items() if k not in ("selcols",)}
        r2 = requests.post(GATOR_URL, data=data_alt, files=files_alt, timeout=timeout)
        r2.raise_for_status()
        text2 = _strip_comments(r2.text)
        is_err2, msg2 = _has_irsa_error(text2)
        if is_err2:
            raise RuntimeError(f"IRSA Gator upload failed: {msg} | retry: {msg2}")
        text = text2

    try:
        return pd.read_csv(io.StringIO(text))
    except Exception as e:
        head = "\n".join(text.splitlines()[:20])
        raise RuntimeError(f"Failed to parse Gator CSV: {e}\n--- BEGIN IRSA REPLY ---\n{head}\n--- END IRSA REPLY ---") from e

def _cone_query_one(catalog: str, ra: float, dec: float, radius_arcsec: float, timeout: float) -> bool:
    data = {
        "outfmt": "1",
        "spatial": "cone",
        "radius": f"{radius_arcsec}",
        "radunits": "arcsec",
        "catalog": catalog,
        "objstr": f"{ra} {dec}",
        "selcols": "ra,dec"
    }
    r = requests.post(GATOR_URL, data=data, timeout=timeout)
    r.raise_for_status()
    text = _strip_comments(r.text)
    lines = [ln for ln in text.splitlines() if ln.strip()]
    return len(lines) > 1  # header + ≥1 row

def _fallback_cones(df: pd.DataFrame, catalog: str, radius_arcsec: float, timeout: float,
                    max_workers: int = 8, log_every: int = 50) -> pd.Series:
    matched_numbers = []
    total = len(df)
    done = 0
    lock = threading.Lock()
    last_log = time.time()

    def task(row):
        try:
            ok = _cone_query_one(catalog, float(row.ra), float(row.dec), radius_arcsec, timeout)
            return row.NUMBER if ok else None
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(task, row) for row in df.itertuples(index=False)]
        for fut in as_completed(futures):
            res = fut.result()
            with lock:
                done += 1
                if res is not None:
                    matched_numbers.append(res)
                if done % log_every == 0 or (time.time() - last_log) > 5:
                    print(f"[cone] {done}/{total} checked; matches={len(matched_numbers)}")
                    last_log = time.time()

    return pd.Series(matched_numbers, dtype=object)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--positions-csv", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--catalog", default="ptf_objects",
                    choices=["ptf_objects","ptf_sources","ptf_lightcurves","ptfphotcalcat"])
    ap.add_argument("--radius-arcsec", type=float, default=5.0)
    ap.add_argument("--timeout", type=float, default=90.0)
    ap.add_argument("--workers", type=int, default=8, help="cone fallback concurrency")
    ap.add_argument("--log-every", type=int, default=50, help="log progress every N cones")
    args = ap.parse_args()

    df = pd.read_csv(args.positions_csv)
    required = {"NUMBER","ra","dec"}
    if not required.issubset(df.columns):
        raise SystemExit(f"positions CSV must have columns: {sorted(required)}")
    df["NUMBER"] = df["NUMBER"].astype(str)

    # Try upload first
    try:
        res = _gator_upload(df, args.catalog, args.radius_arcsec, args.timeout)
        if {"ra","dec"}.issubset(res.columns):
            src = df.copy()
            src["ra"] = src["ra"].round(7); src["dec"] = src["dec"].round(7)
            res["ra"] = res["ra"].round(7); res["dec"] = res["dec"].round(7)
            matched = src.merge(res[["ra","dec"]].drop_duplicates(), on=["ra","dec"], how="inner")
            _write_flags(args.out, matched["NUMBER"])
            return
        else:
            _write_flags(args.out, pd.Series([], dtype=object))
            return
    except Exception as e:
        print(f"[WARN] Upload path failed: {e}")
        print("[INFO] Falling back to per-row cone queries ...")

    matched_numbers = _fallback_cones(df, args.catalog, args.radius_arcsec, args.timeout,
                                      max_workers=args.workers, log_every=args.log_every)
    _write_flags(args.out, matched_numbers)

if __name__ == "__main__":
    main()
