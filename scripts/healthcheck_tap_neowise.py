
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Health checker for NEOWISE-SE TAP async runs (delta workflow).

Scans positions chunk files and classifies each chunk into states:
- NEW: chunk exists, no meta, no closest
- IN_FLIGHT: meta exists and TAP /phase != COMPLETED/ERROR; closest missing
- COMPLETED: closest exists and is non-empty
- NEED_RESUBMIT: meta exists but TAP /phase == ERROR or unreachable; closest missing
- PARTIAL: RAW exists but closest missing (post-processing needed)

Also outputs per-chunk CSV and a Markdown summary with remediation commands.

Usage:
  python scripts/healthcheck_tap_neowise.py     --positions-dir ./data/local-cats/tmp/positions     --glob 'new/positions_chunk_*.csv'     --out-md ./data/local-cats/tmp/healthcheck_neowise.md     --out-csv ./data/local-cats/tmp/healthcheck_neowise.csv

Optional:
  --curl-timeout 5   # seconds for phase checks
"""
import argparse, csv, os, sys, subprocess, shlex
from pathlib import Path

PHASE_OK = {'COMPLETED'}
PHASE_BAD = {'ERROR'}

class Row:
    __slots__ = ('chunk','closest','raw','meta','state','phase','job_url')
    def __init__(self, chunk, closest, raw, meta):
        self.chunk = chunk
        self.closest = closest
        self.raw = raw
        self.meta = meta
        self.state = ''
        self.phase = ''
        self.job_url = ''

def check_phase(job_url: str, timeout: int) -> str:
    if not job_url:
        return ''
    # Prefer curl; fallback to python urllib
    try:
        cmd = f"curl -s --max-time {timeout} {job_url}/phase"
        out = subprocess.check_output(shlex.split(cmd), stderr=subprocess.DEVNULL).decode('utf-8').strip()
        return out
    except Exception:
        try:
            import urllib.request
            with urllib.request.urlopen(f"{job_url}/phase", timeout=timeout) as r:
                return r.read().decode('utf-8').strip()
        except Exception:
            return ''

def classify(row: Row, curl_timeout: int):
    # COMPLETED if closest present and non-empty
    if row.closest.exists() and row.closest.stat().st_size > 0:
        row.state = 'COMPLETED'
        return
    # If RAW present but no closest
    if row.raw.exists() and not row.closest.exists():
        row.state = 'PARTIAL'
        return
    # If meta exists, query phase
    if row.meta.exists():
        try:
            import json
            d = json.loads(row.meta.read_text())
            row.job_url = d.get('job_url','')
        except Exception:
            row.job_url = ''
        phase = check_phase(row.job_url, curl_timeout)
        row.phase = phase
        if phase in PHASE_OK:
            # completed but closest missing -> treat as PARTIAL (post-processing needed)
            row.state = 'PARTIAL'
        elif phase in PHASE_BAD or phase == '':
            row.state = 'NEED_RESUBMIT'
        else:
            row.state = 'IN_FLIGHT'
        return
    # else NEW
    row.state = 'NEW'


def main():
    ap = argparse.ArgumentParser(description='NEOWISE TAP health checker')
    ap.add_argument('--positions-dir', default='./data/local-cats/tmp/positions')
    ap.add_argument('--glob', default='new/positions_chunk_*.csv')
    ap.add_argument('--out-md', default='./data/local-cats/tmp/healthcheck_neowise.md')
    ap.add_argument('--out-csv', default='./data/local-cats/tmp/healthcheck_neowise.csv')
    ap.add_argument('--curl-timeout', type=int, default=5)
    args = ap.parse_args()

    base = Path(args.positions_dir)
    chunks = sorted(base.glob(args.glob))
    if not chunks:
        print(f"[ERROR] No chunk files match: {base}/{args.glob}")
        sys.exit(2)

    rows = []
    for c in chunks:
        cb = c.stem  # positions_chunk_XXXXX
        closest = c.parent / f"{cb.replace('_chunk_', '_')}_closest.csv"
        raw = c.parent / f"{cb.replace('_chunk_', '_')}_raw.csv"
        meta = c.parent / f"{cb.replace('_chunk_', '_')}_tap.meta.json"
        r = Row(chunk=c, closest=closest, raw=raw, meta=meta)
        classify(r, args.curl_timeout)
        rows.append(r)

    # Summaries
    from collections import Counter
    counts = Counter(r.state for r in rows)

    # Write CSV
    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open('w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['chunk_csv','state','phase','job_url','closest_csv','raw_csv','meta_json'])
        for r in rows:
            w.writerow([str(r.chunk), r.state, r.phase, r.job_url, str(r.closest), str(r.raw), str(r.meta)])

    # Write Markdown (use % formatting to avoid brace escaping issues)
    out_md = Path(args.out_md)
    out_md.parent.mkdir(parents=True, exist_ok=True)
    def list_items(state):
        items = [f"- `{r.chunk.name}`" for r in rows if r.state == state]
        return '
'.join(items) if items else '- (none)'
    md = (
        "# NEOWISE TAP healthcheck summary

"
        "**Counts**
"
        "- NEW: %d
"
        "- IN_FLIGHT: %d
"
        "- PARTIAL: %d
"
        "- NEED_RESUBMIT: %d
"
        "- COMPLETED: %d

"
        "## Details
"
        "### NEW (ready to submit)
%s

"
        "### IN_FLIGHT (async job running)
%s

"
        "### PARTIAL (RAW present or phase=COMPLETED but closest missing)
"
        "Remediation:
"
        "```bash
"
        "# rebuild closest + QC for each listed chunk (idempotent)
"
        "for c in %s/%s; do
"
        "  base="${c%%.csv}"; raw="${base/_chunk_/}_raw.csv"; closest="${base/_chunk_/}_closest.csv";
"
        "  [[ -s "$raw" ]] && python ./scripts/closest_per_row_id.py "$raw" "$closest" && \
  python ./scripts/qc_chunk_summary.py "$closest" > "${closest%%.csv}.qc.txt" 2>&1 || true
"
        "done
"
        "```

"
        "### NEED_RESUBMIT (error or unreachable phase, no closest)
"
        "Remediation:
"
        "```bash
"
        "# delete meta to force re-submit, then run single-chunk async
"
        "for c in %s/%s; do
"
        "  base="${c%%.csv}"; meta="${base/_chunk_/}_tap.meta.json"; closest="${base/_chunk_/}_closest.csv";
"
        "  [[ ! -s "$closest" ]] && rm -f "$meta"; done
"
        "# Re-run (example for one chunk):
"
        "bash ./scripts/tap_async_one.sh %s/new/positions_chunk_00001.csv ./scripts/adql_neowise_se_SIMPLE.sql
"
        "```

"
        "### COMPLETED (closest present)
%s

"
        "---
"
        "CSV: `%s`  |  Markdown: `%s`
"
    ) % (
        counts.get('NEW',0), counts.get('IN_FLIGHT',0), counts.get('PARTIAL',0), counts.get('NEED_RESUBMIT',0), counts.get('COMPLETED',0),
        list_items('NEW'),
        list_items('IN_FLIGHT'),
        args.positions_dir, args.glob,
        args.positions_dir, args.glob,
        args.positions_dir,
        list_items('COMPLETED'),
        str(out_csv), str(out_md)
    )
    out_md.write_text(md)

    print(f"[OK] Wrote CSV -> {out_csv}")
    print(f"[OK] Wrote MD  -> {out_md}")

if __name__ == '__main__':
    main()
