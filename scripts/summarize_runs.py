
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Summarize VASCO runs to **compact Markdown lines** (no tables) + CSV and write two tile CSVs:
- `run_summary_tiles.csv` (tile_id only)
- `run_summary_tiles_counts.csv` (per-tile counts)

Assumes modern folder layout under `<DATA_DIR>/tiles/**`. No "flattened" wording is used.
Markdown uses exact single newlines, matching the user's preferred style.

Outputs in <DATA_DIR>:
- run_summary.md
- run_summary.csv
- run_summary_tiles.csv
- run_summary_tiles_counts.csv

Legacy `--run <RUN_DIR>` is still supported with neutral headings.
"""
import argparse
import glob
import csv
from pathlib import Path

# ----------------------- helpers -----------------------
def rows_minus_header(path: str) -> int:
    try:
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            n = sum(1 for _ in f)
        return max(0, n - 1)
    except Exception:
        return 0

def glob_sum(patterns):
    if isinstance(patterns, str):
        patterns = [patterns]
    total = 0
    for pat in patterns:
        for p in glob.glob(pat):
            total += rows_minus_header(p)
    return total

# ----------------------- per-tile counters -----------------------
def summarize_tile(tile_dir: Path) -> dict:
    catalogs = tile_dir / 'catalogs'
    xmatch   = tile_dir / 'xmatch'

    det = glob_sum(str(catalogs / 'sextractor_pass2.csv'))

    gaia_match = glob_sum([
        str(xmatch / 'sex_gaia_xmatch_cdss_within5arcsec.csv'),
        str(xmatch / 'sex_gaia_xmatch_cdss.csv'),
        str(xmatch / 'sex_gaia_xmatch.csv'),
    ])
    ps1_match = glob_sum([
        str(xmatch / 'sex_ps1_xmatch_cdss_within5arcsec.csv'),
        str(xmatch / 'sex_ps1_xmatch_cdss.csv'),
        str(xmatch / 'sex_ps1_xmatch.csv'),
    ])

    gaia_unmatch_cdss = glob_sum(str(xmatch / 'sex_gaia_unmatched_cdss.csv'))
    ps1_unmatch_cdss  = glob_sum(str(xmatch / 'sex_ps1_unmatched_cdss.csv'))
    gaia_unmatch_local = glob_sum(str(xmatch / 'sex_gaia_unmatched.csv'))
    ps1_unmatch_local  = glob_sum(str(xmatch / 'sex_ps1_unmatched.csv'))
    usnob_unmatch      = glob_sum(str(xmatch / 'sex_usnob_unmatched.csv'))

    return {
        'tile_id': tile_dir.name,
        'detections': det,
        'gaia_matched': gaia_match,
        'ps1_matched': ps1_match,
        'gaia_unmatched_cdss': gaia_unmatch_cdss,
        'ps1_unmatched_cdss': ps1_unmatch_cdss,
        'gaia_unmatched_local': gaia_unmatch_local,
        'ps1_unmatched_local': ps1_unmatch_local,
        'usnob_unmatched': usnob_unmatch,
    }

# ----------------------- aggregate counters -----------------------
def summarize_tiles_root(tiles_root: Path):
    tile_dirs = sorted([p for p in tiles_root.glob('*') if p.is_dir()])
    per_tile_counts = [summarize_tile(td) for td in tile_dirs]
    tile_names = [r['tile_id'] for r in per_tile_counts]

    agg = {
        'detections': 0,
        'gaia_matched': 0,
        'ps1_matched': 0,
        'gaia_unmatched_cdss': 0,
        'ps1_unmatched_cdss': 0,
        'gaia_unmatched_local': 0,
        'ps1_unmatched_local': 0,
        'usnob_unmatched': 0,
        # Tile-derived metrics
        'tiles_total': len(tile_dirs),
        'tiles_with_catalogs': 0,
        'tiles_with_xmatch': 0,
    }

    for td in tile_dirs:
        # tile metrics
        has_catalog = (td / 'catalogs' / 'sextractor_pass2.csv').exists()
        # any xmatch file present
        has_xmatch = False
        xm_dir = td / 'xmatch'
        if xm_dir.exists():
            for pat in [
                'sex_*_xmatch*.csv',
                'sex_*_unmatched*.csv',
                'sex_usnob_unmatched.csv'
            ]:
                if list(xm_dir.glob(pat)):
                    has_xmatch = True
                    break
        if has_catalog:
            agg['tiles_with_catalogs'] += 1
        if has_xmatch:
            agg['tiles_with_xmatch'] += 1

    for r in per_tile_counts:
        for k in ['detections','gaia_matched','ps1_matched','gaia_unmatched_cdss','ps1_unmatched_cdss','gaia_unmatched_local','ps1_unmatched_local','usnob_unmatched']:
            agg[k] += r[k]

    def pct(n, d):
        return (100.0 * n / d) if (d > 0 and n >= 0) else 0.0

    agg.update({
        'gaia_matched_pct': pct(agg['gaia_matched'], agg['detections']),
        'ps1_matched_pct': pct(agg['ps1_matched'], agg['detections']),
        'gaia_unmatched_cdss_pct': pct(agg['gaia_unmatched_cdss'], agg['detections']),
        'ps1_unmatched_cdss_pct': pct(agg['ps1_unmatched_cdss'], agg['detections']),
        'gaia_unmatched_local_pct': pct(agg['gaia_unmatched_local'], agg['detections']),
        'ps1_unmatched_local_pct': pct(agg['ps1_unmatched_local'], agg['detections']),
        'usnob_unmatched_pct': pct(agg['usnob_unmatched'], agg['detections']),
    })

    return agg, tile_names, per_tile_counts

# ----------------------- frontends -----------------------
def summarize_current(data_dir: str):
    tiles_root = Path(data_dir) / 'tiles'
    core, tile_names, per_tile_counts = summarize_tiles_root(tiles_root)
    core.update({'label': Path(data_dir).name})
    return core, tile_names, per_tile_counts

# Legacy per-run
def summarize_run(run_dir: str):
    tiles_root = Path(run_dir) / 'tiles'
    core, tile_names, per_tile_counts = summarize_tiles_root(tiles_root)
    core.update({'label': Path(run_dir).name})
    return core, tile_names, per_tile_counts

# ----------------------- writers -----------------------
def write_compact_lines_md(base_dir: str, sections: list[dict]) -> str:
    out = Path(base_dir) / 'run_summary.md'
    lines = []
    lines.append('# VASCO Run Summary\n\n')
    for r in sections:
        lines.append(f'## {r["label"]}\n')
        # tile-derived metrics first
        lines.append(f'- tiles_total: {r.get("tiles_total", 0)}\n')
        lines.append(f'- tiles_with_catalogs: {r.get("tiles_with_catalogs", 0)}\n')
        lines.append(f'- tiles_with_xmatch: {r.get("tiles_with_xmatch", 0)}\n')
        # detections and matched/unmatched
        lines.append(f'- detections (PASS2): {r["detections"]}\n')
        lines.append(f'- GAIA matched (≤5"): {r["gaia_matched"]}\n')
        lines.append(f'- PS1 matched (≤5"): {r["ps1_matched"]}\n')
        lines.append(f'- GAIA unmatched (CDS): {r["gaia_unmatched_cdss"]}\n')
        lines.append(f'- PS1 unmatched (CDS): {r["ps1_unmatched_cdss"]}\n')
        lines.append(f'- GAIA unmatched (local): {r["gaia_unmatched_local"]}\n')
        lines.append(f'- PS1 unmatched (local): {r["ps1_unmatched_local"]}\n')
        lines.append(f'- USNOB unmatched: {r["usnob_unmatched"]}\n')
        # percentages
        lines.append(f'- GAIA matched %: {r["gaia_matched_pct"]:.2f}\n')
        lines.append(f'- PS1 matched %: {r["ps1_matched_pct"]:.2f}\n')
        lines.append(f'- GAIA unmatched (CDS) %: {r["gaia_unmatched_cdss_pct"]:.2f}\n')
        lines.append(f'- PS1 unmatched (CDS) %: {r["ps1_unmatched_cdss_pct"]:.2f}\n')
        lines.append(f'- GAIA unmatched (local) %: {r["gaia_unmatched_local_pct"]:.2f}\n')
        lines.append(f'- PS1 unmatched (local) %: {r["ps1_unmatched_local_pct"]:.2f}\n')
        lines.append(f'- USNOB unmatched %: {r["usnob_unmatched_pct"]:.2f}\n')
    out.write_text(''.join(lines), encoding='utf-8')
    return str(out)


def write_summary_csv(base_dir: str, rows: list[dict]) -> str:
    out = Path(base_dir) / 'run_summary.csv'
    cols = [
        'label',
        # tile-derived metrics
        'tiles_total','tiles_with_catalogs','tiles_with_xmatch',
        # detections/matches
        'detections','gaia_matched','ps1_matched',
        'gaia_unmatched_cdss','ps1_unmatched_cdss',
        'gaia_unmatched_local','ps1_unmatched_local','usnob_unmatched',
        # percentages
        'gaia_matched_pct','ps1_matched_pct',
        'gaia_unmatched_cdss_pct','ps1_unmatched_cdss_pct',
        'gaia_unmatched_local_pct','ps1_unmatched_local_pct','usnob_unmatched_pct'
    ]
    with open(out, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, 0) for k in cols})
    return str(out)


def write_tiles_names_csv(base_dir: str, tile_names: list[str]) -> str:
    out = Path(base_dir) / 'run_summary_tiles.csv'
    with open(out, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['tile_id'])
        for t in tile_names:
            w.writerow([t])
    return str(out)


def write_tiles_counts_csv(base_dir: str, per_tile_counts: list[dict]) -> str:
    out = Path(base_dir) / 'run_summary_tiles_counts.csv'
    cols = [
        'tile_id','detections','gaia_matched','ps1_matched',
        'gaia_unmatched_cdss','ps1_unmatched_cdss',
        'gaia_unmatched_local','ps1_unmatched_local','usnob_unmatched'
    ]
    with open(out, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in per_tile_counts:
            w.writerow({k: r.get(k, 0) for k in cols})
    return str(out)

# ----------------------- main -----------------------
def main():
    ap = argparse.ArgumentParser(description='Summarize VASCO runs to lines-based MD + CSV + tile-name and tile-count CSVs (no "flattened" wording).')
    ap.add_argument('--data-dir', default='./data')
    ap.add_argument('--run', default=None)
    args = ap.parse_args()

    if args.run is None:
        core, tile_names, per_tile_counts = summarize_current(args.data_dir)
        md_path = write_compact_lines_md(args.data_dir, [core])
        csv_path = write_summary_csv(args.data_dir, [core])
        tiles_csv = write_tiles_names_csv(args.data_dir, tile_names)
        tiles_counts_csv = write_tiles_counts_csv(args.data_dir, per_tile_counts)
        print('Wrote', md_path)
        print('Wrote', csv_path)
        print('Wrote', tiles_csv)
        print('Wrote', tiles_counts_csv)
        return 0

    # Legacy per-run (neutral wording)
    core, tile_names, per_tile_counts = summarize_run(args.run)
    md_path = write_compact_lines_md(args.run, [core])
    csv_path = write_summary_csv(args.run, [core])
    tiles_csv = write_tiles_names_csv(args.run, tile_names)
    tiles_counts_csv = write_tiles_counts_csv(args.run, per_tile_counts)
    print('Wrote', md_path)
    print('Wrote', csv_path)
    print('Wrote', tiles_csv)
    print('Wrote', tiles_counts_csv)
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
