
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Render README key figures from run_summary.md with adaptive text fitting.
Generates light and dark PNG banners side-by-side.

Usage:
  python scripts/render_readme_key_figures.py
  # expects run_summary.md in the current directory (or adjust SRC)
"""

import re
from pathlib import Path

import matplotlib
matplotlib.use('Agg')                     # set backend first
matplotlib.rcParams['svg.fonttype'] = 'none'  # keep text as text in SVG
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch


SRC = Path('./data/run_summary.md')

def extract(md_text, label):
    pattern = rf"^\-\s*{re.escape(label)}\s*:\s*([0-9.]+)"
    m = re.search(pattern, md_text, flags=re.MULTILINE)
    return m.group(1) if m else None

def fmt_int(s):
    return f"{int(float(s)):,}" if s else '—'

def fmt_pct(s):
    try:
        return f"{float(s):.2f}%"
    except:
        return '—'

def fit_text(ax, text_str, x, y, max_px, fontsize, **kwargs):
    t = ax.text(x, y, text_str, fontsize=fontsize, **kwargs)
    renderer = plt.gcf().canvas.get_renderer()
    while t.get_window_extent(renderer=renderer).width > max_px and fontsize > 14:
        fontsize -= 1
        t.set_fontsize(fontsize)
    return t

def draw_banner(md_text, theme='light', out_path='readme-key-figures-light.png'):
    vals = {
        'tiles_total':                extract(md_text, 'tiles_total'),
        'tiles_with_catalogs':        extract(md_text, 'tiles_with_catalogs'),
        'tiles_with_xmatch':          extract(md_text, 'tiles_with_xmatch'),
        'detections':                 extract(md_text, 'detections (PASS2)'),
        'canonical':                  extract(md_text, 'matched_any_ids_unique (canonical)'),
        'canonical_pct':              extract(md_text, 'matched_any_ids_unique %'),
        'final_no_opt':               extract(md_text, 'final_no_optical_counterparts'),
        'final_no_opt_pct':           extract(md_text, 'final_no_optical_counterparts %'),
    }

    summary_line = f"Tiles: {fmt_int(vals['tiles_total'])}  •  Catalogs: {fmt_int(vals['tiles_with_catalogs'])}  •  Xmatch: {fmt_int(vals['tiles_with_xmatch'])}"
    cards = [
        ("Detections (PASS2)", fmt_int(vals['detections']), ''),
        ("Canonical matches", fmt_int(vals['canonical']), fmt_pct(vals['canonical_pct'])),
        ("Final no optical counterparts", fmt_int(vals['final_no_opt']), fmt_pct(vals['final_no_opt_pct'])),
    ]

    if theme=='light':
        bg, card_bg, border, prim, sec, acc = '#ffffff', '#f5f7fb', '#e2e8f0', '#0f172a', '#475569', '#0d9488'
    else:
        bg, card_bg, border, prim, sec, acc = '#0b1e2d', '#12283a', '#1f3b53', '#e6edf3', '#9fb3c8', '#61dafb'

    fig_w_px, fig_h_px = 1200, 460
    fig = plt.figure(figsize=(fig_w_px/100, fig_h_px/100), dpi=100)
    ax = plt.gca(); ax.set_axis_off(); ax.set_xlim(0, fig_w_px); ax.set_ylim(0, fig_h_px)

    ax.add_patch(FancyBboxPatch((0,0), fig_w_px, fig_h_px, boxstyle='square', fc=bg, ec=bg))

    # Title & subtitle
    ax.text(40, fig_h_px-60, 'VASCO – Key Figures', fontsize=16, color=prim, va='top', ha='left', weight='bold')
    ax.text(40, fig_h_px-110, summary_line, fontsize=14, color=sec, va='top', ha='left')

    # Cards
    card_gap = 30
    card_w = (fig_w_px - 80 - 2*card_gap) / 3
    card_h = 140
    x0, y0 = 40, 150

    for i,(label,big,pct) in enumerate(cards):
        x = x0 + i*(card_w + card_gap); y = y0
        ax.add_patch(FancyBboxPatch((x,y), card_w, card_h, boxstyle='round,pad=0.02,rounding_size=12', fc=card_bg, ec=border))
        ax.text(x+20, y+card_h-28, label, fontsize=12, color=sec, va='top', ha='left')
        max_px = card_w - 40
        fit_text(ax, big, x+20, y+card_h-64, max_px, fontsize=12, color=acc, va='top', ha='left', weight='bold')
        if pct:
            fit_text(ax, pct, x+20, y+25,  max_px, fontsize=10, color=sec, va='bottom', ha='left')

    footer = 'Source: summarize_runs.py  •  CDS xmatch ≤5″  •  Parquet (5° bins)'
    ax.text(40, 40, footer, fontsize=12, color=sec, va='bottom', ha='left')

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, format='svg', bbox_inches='tight', facecolor=bg)

def main():
    md_text = SRC.read_text(encoding='utf-8')
    OUT_DIR = Path('./images'); OUT_DIR.mkdir(parents=True, exist_ok=True)
    draw_banner(md_text, 'light', str(OUT_DIR / 'readme-key-figures-light.svg'))
    draw_banner(md_text, 'dark',  str(OUT_DIR / 'readme-key-figures-dark.svg'))

if __name__ == '__main__':
    raise SystemExit(main())