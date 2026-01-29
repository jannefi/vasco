#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Run comparator using a FLAT IRSA directory (no per-chunk subfolders).
- Seeds:   ./data/local-cats/tmp/positions/new/positions_chunk_<CID>.csv
- IRSA:    ./data/local-cats/tmp/positions/new/positions<CID>_closest.csv
- AWS:     ./data/local-cats/tmp/positions/aws_compare_out/positions<CID>_closest.csv
- Outputs: ./data/local-cats/tmp/positions/aws_compare_out/compare_chunk<CID>_*.csv + compare_summary.csv

Usage:
  python scripts/run_compare_flat.py \
    --chunks-list ./chunk_ids.txt \
    --tap-flat-dir ./data/local-cats/tmp/positions/new \
    --aws-closest-dir ./data/local-cats/tmp/positions/aws_compare_out \
    --optical-root-base ./data/local-cats/optical_seeds \
    --radius-arcsec 5.0 \
    --workers 8 \
    [--skip-seeds] [--no-summary]
"""
import argparse, os, sys, concurrent.futures as cf, subprocess, csv
from pathlib import Path

def exists(p): return Path(p).exists()

def run_cmd(cmd):
    res = subprocess.run(cmd, capture_output=True, text=True)
    print(res.stdout, end="")
    if res.returncode != 0:
        print(res.stderr, file=sys.stderr)
    return res.returncode

def make_seeds_if_needed(tap_flat, cid, optical_base):
    seeds_csv = os.path.join(tap_flat, f"positions_chunk_{cid}.csv")
    out_dir    = os.path.join(optical_base, f"chunk_{cid}")
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    if not exists(seeds_csv):
        print(f"[WARN] seed CSV missing for {cid}: {seeds_csv}")
        return 0
    cmd = [sys.executable, "scripts/make_optical_seed_from_TAPchunk.py",
           "--tap-chunk-csv", seeds_csv,
           "--chunk-id", cid,
           "--out-dir", out_dir]
    print(f"[CMD] {' '.join(cmd)}")
    return run_cmd(cmd)

def compare_one(tap_flat, aws_dir, cid, no_summary, radius_arcsec):
    tap_closest = os.path.join(tap_flat, f"positions{cid}_closest.csv")
    aws_closest = os.path.join(aws_dir, f"positions{cid}_closest.csv")
    out_prefix  = os.path.join(aws_dir, f"compare_chunk{cid}")
    if not exists(tap_closest):
        print(f"[WARN] IRSA closest missing for {cid}: {tap_closest} (skip)")
        return 0
    if not exists(aws_closest):
        print(f"[WARN] AWS closest missing for {cid}: {aws_closest} (skip)")
        return 0
    cmd = [sys.executable, "scripts/comparator_aws_vs_tap_fixed.py",
           "--tap", tap_closest,
           "--aws", aws_closest,
           "--out-prefix", out_prefix,
           "--ra-dec-atol-arcsec", str(radius_arcsec),
           "--mjd-atol", "5e-5",
           "--snr-rtol", "1e-3"]
    if no_summary:
        cmd.append("--no-summary")
    print(f"[CMD] {' '.join(cmd)}")
    return run_cmd(cmd)

def main():
    ap = argparse.ArgumentParser(description="Comparator runner for flat IRSA layout")
    ap.add_argument("--chunks-list", required=True)
    ap.add_argument("--tap-flat-dir", required=True)
    ap.add_argument("--aws-closest-dir", required=True)
    ap.add_argument("--optical-root-base", required=True)
    ap.add_argument("--radius-arcsec", type=float, default=5.0)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--skip-seeds", action="store_true")
    ap.add_argument("--no-summary", action="store_true")
    a = ap.parse_args()

    with open(a.chunks_list) as f:
        chunks = [ln.strip() for ln in f if ln.strip()]
    print(f"[INFO] {len(chunks)} chunk IDs loaded.")

    # Seeds (optional, but harmless if you run monthly)
    if not a.skip_seeds:
        for cid in chunks:
            rc = make_seeds_if_needed(a.tap_flat_dir, cid, a.optical_root_base)
            if rc != 0: print(f"[WARN] seed build returned rc={rc} for {cid}")

    # Comparator
    def work(cid):
        return compare_one(a.tap_flat_dir, a.aws_closest_dir, cid, a.no_summary, a.radius_arcsec)
    W = max(1, a.workers)
    with cf.ThreadPoolExecutor(max_workers=W) as ex:
        futs = {ex.submit(work, cid): cid for cid in chunks}
        for f in cf.as_completed(futs):
            cid = futs[f]
            try:
                rc = f.result()
                print(f"[DONE] Chunk {cid} rc={rc}")
            except Exception as e:
                print(f"[ERR] Chunk {cid} failed: {e}")
                continue

if __name__ == "__main__":
    sys.exit(main())
