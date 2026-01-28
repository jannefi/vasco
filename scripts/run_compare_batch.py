#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Run NEOWISER AWS comparison for a list of chunks:
  TAP chunk CSV -> seed parquet -> sidecar -> formatter -> comparator

Inputs:
  - chunk_ids.txt : one chunk ID per line (e.g., 00001)
  - TAP files expected under: ./data/local-cats/tmp/positions/TAP/<CID>/
      positions_chunk_<CID>.csv  (seed)
      positions<CID>_closest.csv (TAP reference for comparator)

Optional:
  - If TAP files are missing locally and --s3-handshake is provided, the script
    will attempt to `aws s3 sync` the files from the handshake path.

Outputs:
  - AWS "closest": ./data/local-cats/tmp/positions/new/positions<CID>_closest.csv
  - Comparator diffs: ./data/local-cats/tmp/positions/new/compare_chunk<CID>.*.csv
  - Summary CSV: ./data/local-cats/tmp/positions/new/compare_summary.csv
  - Per-chunk logs: ./logs/compare_chunks/<CID>.log
"""
import argparse
import os
import re
import subprocess
import sys
from pathlib import Path
import csv
import shlex

# ---------------- helpers ----------------

def run(cmd, log_file, cwd=None):
    """Run a command, stream to console and log, return rc."""
    if isinstance(cmd, (list, tuple)):
        cmd_display = " ".join(shlex.quote(c) for c in cmd)
    else:
        cmd_display = cmd
    print("[CMD]", cmd_display)
    with open(log_file, "a", encoding="utf-8") as lf:
        lf.write(f"\n[CMD] {cmd_display}\n")
        lf.flush()
        proc = subprocess.Popen(
            cmd, cwd=cwd, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True
        )
        for line in proc.stdout:
            sys.stdout.write(line)
            lf.write(line)
        rc = proc.wait()
        lf.write(f"\n[RC] {rc}\n")
    return rc

def ensure_dir(p):
    Path(p).mkdir(parents=True, exist_ok=True)

def file_exists(p):
    return Path(p).is_file()

def s3_ls(path, log_file):
    return run(["aws", "s3", "ls", path], log_file)

def s3_cp(src, dst, log_file):
    return run(["aws", "s3", "cp", src, dst, "--only-show-errors"], log_file)

def s3_sync_chunk(s3_base, cid, local_dir, log_file):
    """aws s3 sync TAP outputs for a chunk from handshake path to local TAP dir."""
    if not s3_base:
        return 1  # nothing to do
    ensure_dir(local_dir)
    src = f"{s3_base.rstrip('/')}/{cid}/"
    print(f"[INFO] S3 source for {cid}: {src}")
    # Verify source exists
    rc_ls = s3_ls(src, log_file)
    if rc_ls != 0:
        print(f"[WARN] S3 source not listable for {cid}: {src}")
    # Preferred: sync with includes
    patterns = [
        f"positions{cid}_closest.csv",
        f"positions_chunk_{cid}.csv",
        f"positions_chunk_{cid}.vot",
        f"positions{cid}_closest.qc.txt",
    ]
    args = [
        "aws", "s3", "sync", src, str(local_dir),
        "--exclude", "*",
        "--exact-timestamps", "--only-show-errors"
    ]
    for pat in patterns:
        args.extend(["--include", pat])
    rc_sync = run(args, log_file)
    # Post-sync verification; fallback cp if missing
    missing = [p for p in patterns if not file_exists(os.path.join(local_dir, p))]
    if missing:
        print(f"[WARN] Missing after sync for {cid}: {', '.join(missing)}; attempting per-file copy...")
        for p in missing:
            rc_cp = s3_cp(f"{src}{p}", os.path.join(local_dir, p), log_file)
            if rc_cp != 0:
                print(f"[ERR ] cp failed for {cid}: {p}")
                # Keep going to report in summary
    return rc_sync

def parse_comparator_blocks(text):
    """Parse comparator console output blocks → dict of key metrics."""
    out = {
        "tap_rows": None, "aws_rows": None, "overlap_on_cntr": None,
        "tap_only_on_cntr": None, "aws_only_on_cntr": None,
        "tap_cntr_duplicates": None, "aws_cntr_duplicates": None,
        "aws_gate_violations": None, "tap_gate_violations": None
    }
    # Coverage by cntr
    m = re.search(r'Coverage \(by "cntr"\).*?\{([^\}]*)\}', text, re.S)
    if m:
        kvs = m.group(1)
        def grab(name):
            r = re.search(rf"'{re.escape(name)}':\s*([0-9]+)", kvs)
            return int(r.group(1)) if r else None
        out["tap_rows"] = grab("tap_rows")
        out["aws_rows"] = grab("aws_rows")
        out["overlap_on_cntr"] = grab("overlap_on_cntr")
        out["tap_only_on_cntr"] = grab("tap_only_on_cntr")
        out["aws_only_on_cntr"] = grab("aws_only_on_cntr")
        out["tap_cntr_duplicates"] = grab("tap_cntr_duplicates")
        out["aws_cntr_duplicates"] = grab("aws_cntr_duplicates")
    # Gate checks
    m2 = re.search(r"Gate checks on overlap \(by cntr\).*?\{([^\}]*)\}", text, re.S)
    if m2:
        kvs2 = m2.group(1)
        def grab2(name):
            r = re.search(rf"'{re.escape(name)}':\s*([0-9]+)", kvs2)
            return int(r.group(1)) if r else None
        out["aws_gate_violations"] = grab2("aws_gate_violations")
        out["tap_gate_violations"] = grab2("tap_gate_violations")
    return out

# ---------------- main driver ----------------

def main():
    ap = argparse.ArgumentParser(
        description="Run NEOWISER AWS compare over a list of chunks"
    )
    ap.add_argument("--chunks-list", default="chunk_ids.txt",
                    help="Text file with chunk IDs, one per line (e.g., 00001)")
    ap.add_argument("--tap-root", default="./data/local-cats/tmp/positions/TAP",
                    help="Local TAP positions root containing per-chunk folders")
    ap.add_argument("--optical-root-base", default="./data/local-cats/optical_chunks",
                    help="Base path to write per-chunk optical seed parquet")
    ap.add_argument("--out-root-base",
                    default="./data/local-cats/_master_optical_parquet_irflags",
                    help="Base path to write per-chunk sidecar outputs")
    ap.add_argument("--aws-closest-out-dir", dest="aws_closest_out_dir",
                    default="./data/local-cats/tmp/positions/new",
                    help="Directory to write positions<CID>_closest.csv (AWS)")
    ap.add_argument("--s3-handshake", default="",
                    help="(Optional) s3://.../handshake/from-prod/<RUN_ID>/positions "
                         "(pull TAP files if missing)")
    ap.add_argument("--workers", type=int, default=8,
                    help="Sidecar worker count (bounded)")
    ap.add_argument("--radius-arcsec", type=float, default=5.0,
                    help="Match radius (arcsec)")
    ap.add_argument("--stop-on-error", action="store_true",
                    help="Stop on first error; by default continue to next chunk")
    args = ap.parse_args()

    # IO prep
    ensure_dir(args.aws_closest_out_dir)
    logs_root = "./logs/compare_chunks"
    ensure_dir(logs_root)

    # Summary CSV
    summary_csv = os.path.join(args.aws_closest_out_dir, "compare_summary.csv")
    summary_fields = [
        "chunk_id", "tap_rows", "aws_rows", "overlap_on_cntr",
        "tap_only_on_cntr", "aws_only_on_cntr",
        "tap_cntr_duplicates", "aws_cntr_duplicates",
        "aws_gate_violations", "tap_gate_violations", "rc"
    ]
    if not file_exists(summary_csv):
        with open(summary_csv, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=summary_fields).writeheader()

    # Read chunk IDs
    if not file_exists(args.chunks_list):
        print(f"[ERROR] chunks-list file not found: {args.chunks_list}")
        sys.exit(2)
    with open(args.chunks_list, "r", encoding="utf-8") as fh:
        chunk_ids = [ln.strip() for ln in fh if ln.strip()]
    print(f"[INFO] {len(chunk_ids)} chunk IDs loaded.")

    # Process each chunk
    for cid in chunk_ids:
        log_file = os.path.join(logs_root, f"{cid}.log")
        print(f"\n[RUN] Chunk {cid}")
        with open(log_file, "w", encoding="utf-8") as lf:
            lf.write(f"[RUN] Chunk {cid}\n")

        tap_dir = os.path.join(args.tap_root, cid)
        tap_chunk_csv = os.path.join(tap_dir, f"positions_chunk_{cid}.csv")
        tap_closest_csv = os.path.join(tap_dir, f"positions{cid}_closest.csv")

        # Pull TAP files from S3 handshake if missing
        if not file_exists(tap_chunk_csv) or not file_exists(tap_closest_csv):
            if args.s3_handshake:
                print("[INFO] TAP files missing locally; attempting S3 sync...")
                rc = s3_sync_chunk(args.s3_handshake, cid, tap_dir, log_file)
                # Verify again after sync/cp
                if not file_exists(tap_chunk_csv) or not file_exists(tap_closest_csv):
                    print(f"[WARN] TAP files still missing for {cid}; skipping chunk.")
                    with open(summary_csv, "a", newline="", encoding="utf-8") as f:
                        csv.DictWriter(f, fieldnames=summary_fields).writerow({
                            "chunk_id": cid, "rc": 2
                        })
                    if args.stop_on_error:
                        sys.exit(2)
                    continue
            else:
                print(f"[WARN] No --s3-handshake provided; TAP files missing for {cid}. Skipping.")
                with open(summary_csv, "a", newline="", encoding="utf-8") as f:
                    csv.DictWriter(f, fieldnames=summary_fields).writerow({
                        "chunk_id": cid, "rc": 2
                    })
                if args.stop_on_error:
                    sys.exit(2)
                continue

        # 1) Seed parquet from TAP chunk CSV
        opt_chunk_root = os.path.join(args.optical_root_base, f"chunk_{cid}")
        ensure_dir(opt_chunk_root)
        rc_seed = run([
            sys.executable, "scripts/make_optical_seed_from_TAPchunk.py",
            "--tap-chunk-csv", tap_chunk_csv,
            "--chunk-id", cid,
            "--out-dir", opt_chunk_root
        ], log_file)
        if rc_seed != 0:
            print(f"[ERR ] seed failed for {cid}")
            if args.stop_on_error: sys.exit(rc_seed)
            continue

        # 2) Sidecar (AWS Parquet)
        rc_sidecar = run([
            sys.executable, "scripts/neowise_s3_sidecar.py",
            "--optical-root", opt_chunk_root,
            "--out-root", args.out_root_base,
            "--radius-arcsec", str(args.radius_arcsec),
            "--parallel", "pixel", "--workers", str(args.workers),
            "--force"
        ], log_file)
        if rc_sidecar != 0:
            print(f"[ERR ] sidecar failed for {cid}")
            if args.stop_on_error: sys.exit(rc_sidecar)
            continue

        # 3) Formatter -> positions<CID>_closest.csv
        rc_fmt = run([
            sys.executable, "scripts/sidecar_to_closest_chunks.py",
            "--sidecar-all", os.path.join(args.out_root_base, "neowise_se_flags_ALL.parquet"),
            "--optical-root", opt_chunk_root,
            "--out-dir", args.aws_closest_out_dir
        ], log_file)
        if rc_fmt != 0:
            print(f"[ERR ] formatter failed for {cid}")
            if args.stop_on_error: sys.exit(rc_fmt)
            continue

        # 4) Comparator (AWS vs TAP closest)
        aws_closest_csv = os.path.join(args.aws_closest_out_dir, f"positions{cid}_closest.csv")
        out_prefix = os.path.join(args.aws_closest_out_dir, f"compare_chunk{cid}")
        comp_cmd = [
            sys.executable, "scripts/comparator_aws_vs_tap.py",
            "--tap", tap_closest_csv,
            "--aws", aws_closest_csv,
            "--out-prefix", out_prefix,
            "--ra-dec-atol-arcsec", "0.10",
            "--mjd-atol", "5e-5",
            "--snr-rtol", "1e-3"
        ]
        # Capture comparator output for summary parsing
        print("[CMD]", " ".join(shlex.quote(c) for c in comp_cmd))
        with open(log_file, "a", encoding="utf-8") as lf:
            lf.write("\n[CMD] " + " ".join(shlex.quote(c) for c in comp_cmd) + "\n")
            proc = subprocess.Popen(comp_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            comp_out = []
            for line in proc.stdout:
                sys.stdout.write(line)
                lf.write(line)
                comp_out.append(line)
            rc_comp = proc.wait()
            lf.write(f"\n[RC] {rc_comp}\n")
        # Parse key metrics and append summary CSV
        blocks = parse_comparator_blocks("".join(comp_out))
        blocks["chunk_id"] = cid
        blocks["rc"] = rc_comp
        with open(summary_csv, "a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=summary_fields).writerow(blocks)

        print(f"[DONE] Chunk {cid} rc={rc_comp}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[WARN] Interrupted.")
        sys.exit(130)
