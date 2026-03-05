#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Push optical seed parquet files (part-<CID>.parquet) to an S3 handshake path.

Example:
  python scripts/push_seeds_to_s3.py \
    --chunks-list ./chunk_ids.txt \
    --seeds-root ./data/local-cats/optical_seeds \
    --s3-dest s3://janne-vasco-usw2/vasco/handshake/from-prod/aws-seeds-2026-01-28/optical_seeds

Notes:
  - For each CID, this uploads: <seeds-root>/chunk_<CID>/part-<CID>.parquet
  - Destination layout: <s3-dest>/chunk_<CID>/part-<CID>.parquet
  - Uses 'aws s3 cp' per file with --only-show-errors
"""
import argparse, os, sys, shlex, subprocess
from pathlib import Path

def run(cmd):
    print("[CMD]", " ".join(shlex.quote(str(c)) for c in cmd))
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    for line in proc.stdout:
        sys.stdout.write(line)
    return proc.wait()

def main():
    ap = argparse.ArgumentParser(description="Push optical seeds to S3 handshake")
    ap.add_argument("--chunks-list", default="chunk_ids.txt", help="Text file: one chunk id per line (e.g., 00003)")
    ap.add_argument("--seeds-root", default="./data/local-cats/optical_seeds", help="Root containing chunk_<CID>/part-<CID>.parquet")
    ap.add_argument("--s3-dest", required=True, help="S3 dest base ending with /optical_seeds (e.g., s3://.../optical_seeds)")
    ap.add_argument("--dry-run", action="store_true", help="Print operations without uploading")
    args = ap.parse_args()

    if not Path(args.chunks_list).is_file():
        print(f"[ERROR] chunks-list not found: {args.chunks_list}"); sys.exit(2)

    with open(args.chunks_list, "r", encoding="utf-8") as fh:
        cids = [ln.strip() for ln in fh if ln.strip()]
    print(f"[INFO] {len(cids)} chunk IDs loaded.")

    ok = fail = 0
    for cid in cids:
        local = Path(args.seeds_root) / f"chunk_{cid}" / f"part-{cid}.parquet"
        if not local.is_file():
            print(f"[WARN] missing seed: {local}")
            fail += 1
            continue
        dest = f"{args.s3_dest.rstrip('/')}/chunk_{cid}/part-{cid}.parquet"
        print(f"[PUSH] {local}  ->  {dest}")
        if args.dry_run:
            ok += 1
            continue
        rc = run(["aws", "s3", "cp", str(local), dest, "--only-show-errors"])
        if rc == 0:
            ok += 1
        else:
            print(f"[ERR ] upload failed for {cid} (rc={rc})")
            fail += 1

    print(f"[SUMMARY] uploaded={ok} missing/failed={fail} total={len(cids)}")
    sys.exit(0 if fail == 0 else 1)

if __name__ == "__main__":
    main()
