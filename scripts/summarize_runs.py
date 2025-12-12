
#!/usr/bin/env python3
"""
Summarize VASCO runs (matched / unmatched) and write **well-formatted Markdown** + CSV.
This version adds extra blank lines and a heading before the percentages table to
render cleanly in VS Code's Markdown preview.

Cross-run outputs:
- <data-dir>/run_summary.md
- <data-dir>/run_summary.csv

Single-run outputs:
- <run>/RUN_SUMMARY_MATCHED_UNMATCHED.md
- <run>/RUN_SUMMARY_MATCHED_UNMATCHED.csv
"""
import argparse, os, glob, json, csv
from pathlib import Path

def rows_minus_header(path: str) -> int:
    try:
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            n = sum(1 for _ in f)
            return max(0, n - 1)
    except Exception:
        return 0

def glob_sum(pattern: str) -> int:
    total = 0
    for f in glob.glob(pattern):
        total += rows_minus_header(f)
    return total

def summarize_run(run_dir: str) -> dict:
    run_dir = Path(run_dir)
    planned = downloaded = processed = 0
    counts_path = run_dir / 'RUN_COUNTS.json'
    if counts_path.exists():
        try:
            d = json.loads(counts_path.read_text(encoding='utf-8'))
            planned = int(d.get('planned', 0) or 0)
            downloaded = int(d.get('downloaded', 0) or 0)
            processed = int(d.get('processed', 0) or 0)
        except Exception:
            pass
    tiles = run_dir / 'tiles'
    det = glob_sum(str(tiles / '*' / 'catalogs' / 'sextractor_pass2.csv'))
    # Matched (prefer CDS within5; fallback to local)
    gaia_match = glob_sum(str(tiles / '*' / 'xmatch' / 'sex_gaia_xmatch_cdss_within5arcsec.csv'))
    if gaia_match == 0:
        gaia_match = glob_sum(str(tiles / '*' / 'xmatch' / 'sex_gaia_xmatch.csv'))
    ps1_match = glob_sum(str(tiles / '*' / 'xmatch' / 'sex_ps1_xmatch_cdss_within5arcsec.csv'))
    if ps1_match == 0:
        ps1_match = glob_sum(str(tiles / '*' / 'xmatch' / 'sex_ps1_xmatch.csv'))
    # Unmatched (CDS vs local)
    gaia_unmatch_cdss = glob_sum(str(tiles / '*' / 'xmatch' / 'sex_gaia_unmatched_cdss.csv'))
    ps1_unmatch_cdss  = glob_sum(str(tiles / '*' / 'xmatch' / 'sex_ps1_unmatched_cdss.csv'))
    gaia_unmatch_local = glob_sum(str(tiles / '*' / 'xmatch' / 'sex_gaia_unmatched.csv'))
    ps1_unmatch_local  = glob_sum(str(tiles / '*' / 'xmatch' / 'sex_ps1_unmatched.csv'))
    usnob_unmatch      = glob_sum(str(tiles / '*' / 'xmatch' / 'sex_usnob_unmatched.csv'))
    def pct(n,d):
        return (100.0*n/d) if (d>0 and n>=0) else 0.0
    return {
        'run': run_dir.name,
        'planned': planned,
        'downloaded': downloaded,
        'processed': processed,
        'detections': det,
        'gaia_matched': gaia_match,
        'ps1_matched': ps1_match,
        'gaia_unmatched_cdss': gaia_unmatch_cdss,
        'ps1_unmatched_cdss':  ps1_unmatch_cdss,
        'gaia_unmatched_local': gaia_unmatch_local,
        'ps1_unmatched_local':  ps1_unmatch_local,
        'usnob_unmatched':      usnob_unmatch,
        'gaia_matched_pct': pct(gaia_match, det),
        'ps1_matched_pct':  pct(ps1_match, det),
        'gaia_unmatched_cdss_pct': pct(gaia_unmatch_cdss, det),
        'ps1_unmatched_cdss_pct':  pct(ps1_unmatch_cdss,  det),
        'gaia_unmatched_local_pct': pct(gaia_unmatch_local, det),
        'ps1_unmatched_local_pct':  pct(ps1_unmatch_local,  det),
        'usnob_unmatched_pct':      pct(usnob_unmatch,      det),
    }

def write_cross_run_md(data_dir: str, rows: list[dict]) -> str:
    out_path = Path(data_dir) / 'run_summary.md'
    lines = []
    # initial spacer helps some renderers
    lines.append('')
    lines.append('# VASCO Run Summary (matched / unmatched)')
    lines.append('')
    # Table 1: counts
    lines.append('| run | planned | downloaded | processed | detections | GAIA≤5" | PS1≤5" | GAIA unmatched (CDS) | PS1 unmatched (CDS) | GAIA unmatched (local) | PS1 unmatched (local) | USNOB unmatched |')
    lines.append('|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|')
    for r in rows:
        lines.append('| {run} | {planned} | {downloaded} | {processed} | {detections} | {gaia_matched} | {ps1_matched} | {gaia_unmatched_cdss} | {ps1_unmatched_cdss} | {gaia_unmatched_local} | {ps1_unmatched_local} | {usnob_unmatched} |'.format(**r))
    lines.append('')
    # Heading + Table 2: percentages
    lines.append('## Percentages relative to detections')
    lines.append('')
    lines.append('| run | GAIA matched % | PS1 matched % | GAIA unmatched (CDS) % | PS1 unmatched (CDS) % | GAIA unmatched (local) % | PS1 unmatched (local) % | USNOB unmatched % |')
    lines.append('|---|---:|---:|---:|---:|---:|---:|---:|')
    for r in rows:
        lines.append('| {run} | {gaia_matched_pct:.2f} | {ps1_matched_pct:.2f} | {gaia_unmatched_cdss_pct:.2f} | {ps1_unmatched_cdss_pct:.2f} | {gaia_unmatched_local_pct:.2f} | {ps1_unmatched_local_pct:.2f} | {usnob_unmatched_pct:.2f} |'.format(**r))
    lines.append('')
    out_path.write_text(''.join(lines) + '', encoding='utf-8')
    return str(out_path)

def write_cross_run_csv(data_dir: str, rows: list[dict]) -> str:
    out_path = Path(data_dir) / 'run_summary.csv'
    cols = [
        'run','planned','downloaded','processed','detections',
        'gaia_matched','ps1_matched',
        'gaia_unmatched_cdss','ps1_unmatched_cdss',
        'gaia_unmatched_local','ps1_unmatched_local',
        'usnob_unmatched',
        'gaia_matched_pct','ps1_matched_pct',
        'gaia_unmatched_cdss_pct','ps1_unmatched_cdss_pct',
        'gaia_unmatched_local_pct','ps1_unmatched_local_pct',
        'usnob_unmatched_pct'
    ]
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in cols})
    return str(out_path)

def write_per_run_md(run_dir: str, r: dict) -> str:
    out_path = Path(run_dir) / 'RUN_SUMMARY_MATCHED_UNMATCHED.md'
    lines = []
    lines.append('')
    lines.append('# Run Summary (matched / unmatched)')
    lines.append('')
    lines.append(f'- **planned**: {r["planned"]}')
    lines.append(f'- **downloaded**: {r["downloaded"]}')
    lines.append(f'- **processed**: {r["processed"]}')
    lines.append(f'- **detections (PASS2)**: {r["detections"]}')
    lines.append('')
    lines.append('## Counts (CDS vs local)')
    lines.append('')
    lines.append('| catalog | matched (≤5") | unmatched (CDS) | unmatched (local) | matched % | unmatched (CDS) % | unmatched (local) % |')
    lines.append('|---|---:|---:|---:|---:|---:|---:|')
    lines.append('| GAIA | {gaia_matched} | {gaia_unmatched_cdss} | {gaia_unmatched_local} | {gaia_matched_pct:.2f} | {gaia_unmatched_cdss_pct:.2f} | {gaia_unmatched_local_pct:.2f} |'.format(**r))
    lines.append('| PS1  | {ps1_matched} | {ps1_unmatched_cdss} | {ps1_unmatched_local} | {ps1_matched_pct:.2f} | {ps1_unmatched_cdss_pct:.2f} | {ps1_unmatched_local_pct:.2f} |'.format(**r))
    lines.append('| USNOB (unmatched only) | — | — | {usnob_unmatched} | — | — | {usnob_unmatched_pct:.2f} |'.format(**r))
    lines.append('')
    out_path.write_text(''.join(lines) + '', encoding='utf-8')
    return str(out_path)

def write_per_run_csv(run_dir: str, r: dict) -> str:
    out_path = Path(run_dir) / 'RUN_SUMMARY_MATCHED_UNMATCHED.csv'
    cols = [
        'run','planned','downloaded','processed','detections',
        'gaia_matched','ps1_matched',
        'gaia_unmatched_cdss','ps1_unmatched_cdss',
        'gaia_unmatched_local','ps1_unmatched_local',
        'usnob_unmatched',
        'gaia_matched_pct','ps1_matched_pct',
        'gaia_unmatched_cdss_pct','ps1_unmatched_cdss_pct',
        'gaia_unmatched_local_pct','ps1_unmatched_local_pct',
        'usnob_unmatched_pct'
    ]
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerow({k: r.get(k) for k in cols})
    return str(out_path)

def main():
    ap = argparse.ArgumentParser(description='Summarize VASCO runs (matched/unmatched) to Markdown and CSV (VS Code-friendly formatting).')
    ap.add_argument('--data-dir', default='./data')
    ap.add_argument('--run', default=None)
    args = ap.parse_args()
    if args.run:
        r = summarize_run(args.run)
        print(json.dumps(r, indent=2))
        md_path = write_per_run_md(args.run, r)
        csv_path = write_per_run_csv(args.run, r)
        print('Wrote', md_path)
        print('Wrote', csv_path)
        return 0
    
    data = Path(args.data_dir)
    tiles_root = data / 'tiles'
    rows = []
    if tiles_root.exists():
        # Synthesize a single "run" from flattened tiles
        # (or group by some naming scheme if you have one)
        fake_run = str(data / 'FLATTENED')
        rows.append(summarize_run(fake_run))  # you may adapt summarize_run to accept data/tiles
    else:
        runs_root = data / 'runs'
        runs = sorted(glob.glob(str(runs_root / 'run-*')))
        rows = [summarize_run(run) for run in runs]

    md_path = write_cross_run_md(args.data_dir, rows)
    csv_path = write_cross_run_csv(args.data_dir, rows)
    print('Wrote', md_path)
    print('Wrote', csv_path)
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
