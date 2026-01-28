#!/usr/bin/env python3
import argparse, os, subprocess, sys

def run(cmd):
    print('[CMD]', ' '.join(cmd)); sys.stdout.flush()
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    for line in proc.stdout:
        print(line.rstrip())
    return proc.wait()

def main():
    ap = argparse.ArgumentParser(description='Batch NEOWISER sidecar over chunk delta roots')
    ap.add_argument('--chunks-list', required=True, help='Text file: one optical_root path per line (local or s3://)')
    ap.add_argument('--out-root', required=True, help='Output root (local or s3://)')
    ap.add_argument('--workers', type=int, default=8)
    args = ap.parse_args()

    with open(args.chunks_list) as f:
        roots = [ln.strip() for ln in f if ln.strip()]

    for i, opt_root in enumerate(roots, 1):
        print(f"[RUN] {i}/{len(roots)}  optical_root={opt_root}")
        rc = run([sys.executable, 'scripts/neowise_s3_sidecar.py',
                  '--optical-root', opt_root,
                  '--out-root', args.out_root,
                  '--parallel', 'pixel', '--workers', str(args.workers),
                  '--force'])
        if rc != 0:
            print(f'[ERR] sidecar failed for {opt_root}'); continue

        rc = run([sys.executable, 'scripts/sidecar_to_closest_chunks.py',
                  '--sidecar-all', os.path.join(args.out_root, 'neowise_se_flags_ALL.parquet'),
                  '--optical-root', opt_root,
                  '--out-dir', './data/local-cats/tmp/positions/new'])
        if rc != 0:
            print(f'[ERR] formatter failed for {opt_root}'); continue

    print('[DONE] Batch complete.')

if __name__ == '__main__':
    sys.exit(main())
