#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pull optical seed parquet files (part-<CID>.parquet) from an S3 handshake path.

Example:
  python scripts/pull_seeds_from_s3.py \
    --chunks-list ./chunk_ids.txt \
    --s3-src s3://janne-vasco-usw2/vasco/handshake/from-prod/aws-seeds-2026-01-28/optical_seeds \
    --seeds-root ./data/local-cats/optical_seeds

Notes:
  - Source layout must be <s3-src>/chunk_<CID>/part-<CID>.parquet
  - Creates local <seeds-root>/chunk_<CID>/ and downloads the parquet
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
    ap = argparse.ArgumentParser(description="Pull optical seeds from S3 handshake")
    ap.add_argument("--chunks-list", default="chunk_ids.txt", help="Text file: one chunk id per line (e.g., 00003)")
    ap.add_argument("--s3-src", required=True, help="S3 source base ending with /optical_seeds (e.g., s3://.../optical_seeds)")
    ap.add_argument("--seeds-root", default="./data/local-cats/optical_seeds", help="Local destination root")
    ap.add_argument("--dry-run", action="store_true", help="Print operations without downloading")
    args = ap.parse_args()

    if not Path(args.chunks_list).is_file():
        print(f"[ERROR] chunks-list not found: {args.chunks_list}"); sys.exit(2)

    with open(args.chunks_list, "r", encoding="utf-8") as fh:
        cids = [ln.strip() for ln in fh if ln.strip()]
    print(f"[INFO] {len(cids)} chunk IDs loaded.")

    ok = fail = 0
    for cid in cids:
        local_dir = Path(args.seeds_root) / f"chunk_{cid}"
        local_dir.mkdir(parents=True, exist_ok=True)
        local = local_dir / f"part-{cid}.parquet"
        src = f"{args.s3_src.rstrip('/')}/chunk_{cid}/part-{cid}.parquet"
        print(f"[PULL] {src}  ->  {local}")
        if args.dry_run:
            ok += 1
            continue
        rc = run(["aws", "s3", "cp", src, str(local), "--only-show-errors"])
        if rc == 0 and local.is_file():
            ok += 1
        else:
            print(f"[ERR ] download failed for {cid} (rc={rc})")
            fail += 1

    print(f"[SUMMARY] downloaded={ok} failed={fail} total={len(cids)}")
    sys.exit(0 if fail == 0 else 1)

if __name__ == "__main__":
    main()
