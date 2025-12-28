
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
matplotlib.rcParams['svg.fonttype'] = 'none'
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch
from matplotlib.font_manager import FontProperties

SRC = Path('./data/run_summary.md')

def extract(md_text, label):
    pattern = rf"^\-\s*{re.escape(label)}\s*:\s*([0-9\.\+Ee\-]+)"
    m = re.search(pattern, md_text, flags=re.MULTILINE)
    return m.group(1) if m else None

def extract_line(md_text, label_prefix):
    pattern = rf"^\-\s*{re.escape(label_prefix)}\s*(.*)$"
    m = re.search(pattern, md_text, flags=re.MULTILINE)
    return m.group(1).strip() if m else None

def fmt_int(s):
    return f"{int(float(s)):,}" if s else '—'

def fmt_pct_num(num, den):
    try:
        num = float(num); den = float(den)
        return f"{(100.0 * num / max(den, 1.0)):.1f}%"
    except Exception:
        return '—'

def fmt_pct(s):
    try:
        return f"{float(s):.2f}%"
    except Exception:
        return '—'

def fit_text(ax, text_str, x, y, max_px, fontsize, **kwargs):
    """Shrink font until text fits max_px width (min 12pt)."""
    t = ax.text(x, y, text_str, fontsize=fontsize, **kwargs)
    renderer = plt.gcf().canvas.get_renderer()
    while t.get_window_extent(renderer=renderer).width > max_px and fontsize > 12:
        fontsize -= 1
        t.set_fontsize(fontsize)
    return t

def get_vals(md_text):
    vals = {
        'tiles_total': extract(md_text, 'tiles_total'),
        'tiles_with_catalogs': extract(md_text, 'tiles_with_catalogs'),
        'tiles_with_xmatch': extract(md_text, 'tiles_with_xmatch'),
        'tiles_with_final': extract(md_text, 'tiles_with_final'),
        'detections': extract(md_text, 'detections (PASS2)'),
        'canonical': extract(md_text, 'matched_any_ids_unique (canonical)'),
        'canonical_pct': extract(md_text, 'matched_any_ids_unique %'),
        'final_no_opt': extract(md_text, 'final_no_optical_counterparts'),
        'final_no_opt_pct': extract(md_text, 'final_no_optical_counterparts %'),
        'ir_strict': extract(md_text, 'IR strict matches (≤ 5.0")'),
        'ir_sep_med': extract(md_text, 'IR sep_arcsec median'),
        'ir_sep_p95': extract(md_text, 'IR sep_arcsec p95'),
    }
    snr_line = extract_line(md_text, 'IR SNR bands:')
    vals['w1_ge5'] = vals['w2_ge5'] = vals['any_ge5'] = None
    if snr_line:
        m1 = re.search(r"W1≥5\s*=\s*([\d_]+)", snr_line)
        m2 = re.search(r"W2≥5\s*=\s*([\d_]+)", snr_line)
        m3 = re.search(r"any≥5\s*=\s*([\d_]+)", snr_line)
        vals['w1_ge5'] = m1.group(1) if m1 else None
        vals['w2_ge5'] = m2.group(1) if m2 else None
        vals['any_ge5'] = m3.group(1) if m3 else None
    return vals

def draw_badge(ax, text, x, y, pad_x, pad_y, fg, bg, border):
    """Small rounded badge for labels."""
    renderer = plt.gcf().canvas.get_renderer()
    # Temporary text to get width
    t = ax.text(x, y, text, fontsize=11, color=fg, va='center', ha='left')
    bb = t.get_window_extent(renderer=renderer)
    t.remove()
    # Approximate width in pixels -> use a fixed width calc
    w = max(120, len(text) * 6.2) + 2*pad_x
    h = 22 + 2*pad_y
    ax.add_patch(FancyBboxPatch((x, y - h/2), w, h,
                    boxstyle='round,pad=0.02,rounding_size=8',
                    fc=bg, ec=border, lw=1.0))
    ax.text(x + pad_x, y, text, fontsize=11, color=fg, va='center', ha='left')

def draw_progress(ax, vals, x, y, w, h, colors, border, prim, sec):
    """Readable stacked progress with labels below the bar."""
    tot = vals['tiles_total']; cat = vals['tiles_with_catalogs']; xm = vals['tiles_with_xmatch']; fin = vals['tiles_with_final']
    if not tot or not cat or not xm:
        return
    tot = float(tot); cat = float(cat); xm = float(xm)
    fin = float(fin) if fin else 0.0
    cat = min(cat, tot); xm = min(xm, tot); fin = min(fin, tot)

    # Bar frame
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle='round,pad=0.02,rounding_size=10', fc='none', ec=border, lw=1.2))

    def seg(x0, frac, color):
        sw = w * max(min(frac, 1.0), 0.0)
        ax.add_patch(FancyBboxPatch((x0, y), sw, h, boxstyle='round,pad=0.02,rounding_size=10', fc=color, ec=color))
        return x0 + sw

    xptr = x
    xptr = seg(xptr, cat/tot, colors[0])
    xptr = seg(xptr, max((xm - cat)/tot, 0.0), colors[1])
    if vals['tiles_with_final']:
        seg(xptr, max((fin - xm)/tot, 0.0), colors[2])

    # Labels as badges BELOW the bar
    cat_pct = fmt_pct_num(cat, tot); xm_pct = fmt_pct_num(xm, tot)
    fin_pct = fmt_pct_num(fin, tot) if vals['tiles_with_final'] else None
    # Compute positions (left, center, right)
    draw_badge(ax, f"Catalogs {int(cat):,} ({cat_pct})", x, y - 14, 10, 2, prim, '#ffffff22', border)
    draw_badge(ax, f"X-match {int(xm):,} ({xm_pct})", x + w*0.36, y - 14, 10, 2, prim, '#ffffff22', border)
    if fin_pct:
        draw_badge(ax, f"Final {int(fin):,} ({fin_pct})", x + w*0.72, y - 14, 10, 2, prim, '#ffffff22', border)

def draw_banner(md_text, theme='light', out_path='readme-key-figures-light.svg'):
    vals = get_vals(md_text)

    # Theme colors (higher contrast)
    if theme == 'light':
        bg, card_bg, border, prim, sec = '#ffffff', '#eef2f7', '#d1d5db', '#0f172a', '#4b5563'
        acc, acc2 = '#0ea5a1', '#334155'
        prog = ('#93c5fd', '#60a5fa', '#2563eb')  # catalogs, xmatch, final
        badge_border = '#cbd5e1'
    else:
        bg, card_bg, border, prim, sec = '#0b1e2d', '#0f2637', '#1f3b53', '#e6edf3', '#9fb3c8'
        acc, acc2 = '#61dafb', '#a7b6c8'
        prog = ('#075985', '#0284c7', '#38bdf8')
        badge_border = '#1f3b53'

    # Monospaced font for big numbers (fallback if not installed)
    mono = FontProperties(family='DejaVu Sans Mono')

    fig_w_px, fig_h_px = 1200, 640
    fig = plt.figure(figsize=(fig_w_px/100, fig_h_px/100), dpi=100)
    ax = plt.gca(); ax.set_axis_off(); ax.set_xlim(0, fig_w_px); ax.set_ylim(0, fig_h_px)
    ax.add_patch(FancyBboxPatch((0, 0), fig_w_px, fig_h_px, boxstyle='square', fc=bg, ec=bg))

    # Title + subtitle (wrap subtitle if needed—print concise metrics only)
    ax.text(40, fig_h_px-60, 'VASCO – Key Figures', fontsize=14, color=prim, va='top', ha='left', weight='bold')
    subtitle = f"Tiles {fmt_int(vals['tiles_total'])} • Catalogs {fmt_int(vals['tiles_with_catalogs'])} • X-match {fmt_int(vals['tiles_with_xmatch'])}"
    fit_text(ax, subtitle, 40, fig_h_px-100, max_px=fig_w_px-80, fontsize=14, color=sec, va='top', ha='left')

    # Progress bar (thicker, labels below)
    #draw_progress(ax, vals, x=40, y=fig_h_px-150, w=fig_w_px-80, h=28, colors=prog, border=badge_border, prim=prim, sec=sec)

    # Card data
    cards = [
        ("Detections (PASS2)", fmt_int(vals['detections']), ''),
        ("Canonical matches", fmt_int(vals['canonical']), fmt_pct(vals['canonical_pct'])),
        ("Final no optical counterparts", fmt_int(vals['final_no_opt']), fmt_pct(vals['final_no_opt_pct'])),
        ("NEOWISE strict (≤5″)", fmt_int(vals['ir_strict']), ''),  # big number
        ("NEOWISE quality",  # compact line
         f"W1≥5 {fmt_int(vals['w1_ge5'])} • W2≥5 {fmt_int(vals['w2_ge5'])}",
         f"any≥5 {fmt_int(vals['any_ge5'])} • med/p95 {vals['ir_sep_med'] or '—'}/{vals['ir_sep_p95'] or '—'}″"),
    ]

    # Grid: 2 rows (3 + 2)
    top_cards = cards[:3]
    bottom_cards = cards[3:]

    # Layout parameters
    card_gap_x = 24
    card_gap_y = 18
    columns_top = 3
    columns_bottom = 2
    card_w_top = (fig_w_px - 80 - (columns_top-1)*card_gap_x) / columns_top
    card_w_bottom = (fig_w_px - 80 - (columns_bottom-1)*card_gap_x) / columns_bottom
    card_h = 150
    x0, y0_top = 40, 320
    x0_bottom, y0_bottom = 40, 150

    # Render top row
    for i, (label, big, pct) in enumerate(top_cards):
        x = x0 + i*(card_w_top + card_gap_x); y = y0_top
        ax.add_patch(FancyBboxPatch((x, y), card_w_top, card_h,
                    boxstyle='round,pad=0.02,rounding_size=12', fc=card_bg, ec=border))
        ax.text(x+20, y+card_h-28, label, fontsize=12, color=sec, va='top', ha='left')
        fit_text(ax, big, x+20, y+card_h-68, card_w_top-40, fontsize=14,
                 color=acc, va='top', ha='left', weight='bold', fontproperties=mono)
        if pct:
            fit_text(ax, pct, x+20, y+26, card_w_top-40, fontsize=12, color=acc2, va='bottom', ha='left')

    # Render bottom row
    for i, (label, big, pct) in enumerate(bottom_cards):
        x = x0_bottom + i*(card_w_bottom + card_gap_x); y = y0_bottom
        ax.add_patch(FancyBboxPatch((x, y), card_w_bottom, card_h,
                    boxstyle='round,pad=0.02,rounding_size=12', fc=card_bg, ec=border))
        ax.text(x+20, y+card_h-28, label, fontsize=12, color=sec, va='top', ha='left')
        fit_text(ax, big, x+20, y+card_h-68, card_w_bottom-40, fontsize=14,
                 color=acc, va='top', ha='left', weight='bold', fontproperties=mono)
        if pct:
            fit_text(ax, pct, x+20, y+26, card_w_bottom-40, fontsize=12, color=acc2, va='bottom', ha='left')

    footer = 'Source: summarize_runs.py • CDS xmatch ≤5″ • NEOWISE strict ≤5″ • SVG export'
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
