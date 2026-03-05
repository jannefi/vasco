#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cleanup EC2 workspace between runs (safe defaults, optional purge).

Default behavior deletes:
  - AWS compare outputs: ./data/local-cats/tmp/positions/aws_compare_out/*
  - Sidecar tmp shards:  ./data/local-cats/_aws_sidecar_flags/tmp/k5=*.parquet
  - Logs:                ./logs/compare_chunks/*.log

Optional flags:
  --keep-compare        Keep aws_compare_out
  --keep-tmp            Keep sidecar tmp shards
  --keep-logs           Keep chunk logs
  --purge-flags         ALSO delete ./data/local-cats/_aws_sidecar_flags/neowise_se_flags_ALL.parquet
  --purge-seeds         ALSO delete ./data/local-cats/optical_seeds/chunk_* (seeds)
  --dry-run             Print what would be removed without deleting

Example:
  python scripts/clean_ec2_after_run.py --dry-run
  python scripts/clean_ec2_after_run.py --purge-flags --purge-seeds
"""
import argparse, os, sys, glob
from pathlib import Path

def rm_paths(paths, dry):
    for p in paths:
        if dry:
            print("[DRY]", p)
        else:
            try:
                if Path(p).is_dir():
                    for root, dirs, files in os.walk(p, topdown=False):
                        for f in files:
                            Path(os.path.join(root, f)).unlink(missing_ok=True)
                        for d in dirs:
                            Path(os.path.join(root, d)).rmdir()
                    Path(p).rmdir()
                else:
                    Path(p).unlink(missing_ok=True)
                print("[DEL]", p)
            except Exception as e:
                print("[ERR]", p, e)

def main():
    ap = argparse.ArgumentParser(description="Cleanup EC2 workspace after a run")
    ap.add_argument("--keep-compare", action="store_true")
    ap.add_argument("--keep-tmp", action="store_true")
    ap.add_argument("--keep-logs", action="store_true")
    ap.add_argument("--purge-flags", action="store_true")
    ap.add_argument("--purge-seeds", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    base = Path(".")
    compare_dir = base / "data/local-cats/tmp/positions/aws_compare_out"
    flags_dir   = base / "data/local-cats/_aws_sidecar_flags"
    seeds_dir   = base / "data/local-cats/optical_seeds"
    logs_dir    = base / "logs/compare_chunks"

    # 1) compare outputs
    if not args.keep_compare:
        rm_paths(glob.glob(str(compare_dir / "*")), args.dry_run)

    # 2) sidecar tmp shards
    if not args.keep_tmp:
        rm_paths(glob.glob(str(flags_dir / "tmp" / "k5=*.parquet")), args.dry_run)

    # 3) logs
    if not args.keep_logs:
        rm_paths(glob.glob(str(logs_dir / "*.log")), args.dry_run)

    # 4) optional purge: finalized flags parquet
    if args.purge_flags:
        rm_paths([str(flags_dir / "neowise_se_flags_ALL.parquet")], args.dry_run)

    # 5) optional purge: seeds
    if args.purge_seeds:
        for p in seeds_dir.glob("chunk_*"):
            rm_paths([str(p)], args.dry_run)

    print("[DONE] Cleanup complete.")

if __name__ == "__main__":
    main()
