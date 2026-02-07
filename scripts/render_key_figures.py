#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Render README key figures (SVG) from run_summary.md.

What’s new (2026-02-07):
- Robust Python regex (no Vim-style tokens): proper groups (+), anchors, and character classes.
- IR strict line parsed independent of the numeric threshold (handles ≤2.0" and ≤5.0", also '<=' form).
- Dynamic card label: shows the parsed threshold value.
- Fail-fast guard: exits with nonzero status if required values are missing (prevents silent zeros).
- Auto-discovery of latest ./data/metadata/qc/<YYYYMMDD>/run_summary.md still supported.
"""

import re
import sys
import argparse
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
matplotlib.rcParams['svg.fonttype'] = 'none'
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch
from matplotlib.font_manager import FontProperties


def discover_latest_md() -> Path:
    qc_root = Path("./data/metadata/qc")
    if not qc_root.exists():
        raise SystemExit("[ERROR] ./data/metadata/qc not found and --src-md not provided")
    candidates = []
    for p in qc_root.glob("*"):
        if not p.is_dir():
            continue
        md = p / "run_summary.md"
        if md.exists():
            try:
                candidates.append((int(p.name), md))
            except ValueError:
                pass
    if not candidates:
        raise SystemExit("[ERROR] No run_summary.md found under ./data/metadata/qc/*/")
    candidates.sort(key=lambda t: t[0], reverse=True)
    return candidates[0][1]


def _extract_exact(md_text: str, label: str):
    """
    Strict extractor for lines like:
      - detections (PASS2): 13650677
    """
    # ^- label: value$
    pattern = rf"^-\s*{re.escape(label)}\s*:\s*([0-9][0-9,._Ee+\-]*)\s*$"
    m = re.search(pattern, md_text, flags=re.MULTILINE)
    return m.group(1).strip() if m else None


def _extract_line(md_text: str, label_prefix: str):
    """
    Returns the full text after the label_prefix for lines like:
      - IR SNR bands: W1≥5=0 W2≥5=0 any≥5=0
    """
    pattern = rf"^-\s*{re.escape(label_prefix)}\s*(.*)$"
    m = re.search(pattern, md_text, flags=re.MULTILINE)
    return m.group(1).strip() if m else None


def _extract_ir_strict(md_text: str):
    """
    Parse 'IR strict matches' tolerantly:
      - IR strict matches (≤ 2.0"): 11057050
      - IR strict matches (<= 5.0"): 11057050
    Returns (value_str, threshold_str) or (None, None).
    """
    pat = re.compile(
        r'^-\s*IR strict matches\s*\((?:<=|≤)\s*([0-9.]+)"\)\s*:\s*([0-9][0-9,._Ee+\-]*)\s*$',
        re.MULTILINE,
    )
    m = pat.search(md_text)
    if not m:
        return None, None
    threshold = m.group(1).strip()
    value = m.group(2).strip()
    return value, threshold


def fmt_int(s):
    try:
        v = int(float(str(s).replace(",", "")))
        return f"{v:,}"
    except Exception:
        return "—"


def fit_text(ax, text_str, x, y, max_px, fontsize, **kwargs):
    """Shrink font until text fits max_px width (min 12pt)."""
    t = ax.text(x, y, text_str, fontsize=fontsize, **kwargs)
    renderer = plt.gcf().canvas.get_renderer()
    while t.get_window_extent(renderer=renderer).width > max_px and fontsize > 12:
        fontsize -= 1
        t.set_fontsize(fontsize)
    return t


def parse_vals(md_text: str):
    vals = {
        "tiles_total": _extract_exact(md_text, "tiles_total"),
        "tiles_with_catalogs": _extract_exact(md_text, "tiles_with_catalogs"),
        "tiles_with_xmatch": _extract_exact(md_text, "tiles_with_xmatch"),
        "tiles_with_final": _extract_exact(md_text, "tiles_with_final"),
        "detections": _extract_exact(md_text, "detections (PASS2)"),
        "canonical": _extract_exact(md_text, "matched_any_ids_unique (canonical)"),
        "canonical_pct": _extract_exact(md_text, "matched_any_ids_unique %"),
        "final_no_opt": _extract_exact(md_text, "final_no_optical_counterparts"),
        "final_no_opt_pct": _extract_exact(md_text, "final_no_optical_counterparts %"),
        "ir_sep_med": _extract_exact(md_text, "IR sep_arcsec median"),
        "ir_sep_p95": _extract_exact(md_text, "IR sep_arcsec p95"),
        "ir_match_rate": _extract_exact(md_text, "IR strict match rate"),
    }

    # IR strict matches + threshold (tolerant)
    ir_value, ir_threshold = _extract_ir_strict(md_text)
    vals["ir_strict"] = ir_value
    vals["ir_threshold"] = ir_threshold or "5.0"  # default for label, if absent

    # SNR line
    snr = _extract_line(md_text, "IR SNR bands:")
    vals["w1_ge5"] = vals["w2_ge5"] = vals["any_ge5"] = None
    if snr:
        m1 = re.search(r"W1≥5\s*=\s*([0-9_]+)", snr)
        m2 = re.search(r"W2≥5\s*=\s*([0-9_]+)", snr)
        m3 = re.search(r"any≥5\s*=\s*([0-9_]+)", snr)
        vals["w1_ge5"] = m1.group(1) if m1 else None
        vals["w2_ge5"] = m2.group(1) if m2 else None
        vals["any_ge5"] = m3.group(1) if m3 else None

    return vals


def guard_required(vals):
    """Fail fast if core values are missing to avoid silent 0/— on the banner."""
    required = {
        "detections",
        "canonical",
        "final_no_opt",
        "ir_strict",
    }
    missing = [k for k in required if not vals.get(k)]
    if missing:
        msg = "[ERROR] Missing keys in run_summary.md: " + ", ".join(missing)
        raise SystemExit(msg)


def draw_banner(md_text, theme="light", out_path="images/readme-key-figures-light.svg", title="VASCO – Key Figures"):
    vals = parse_vals(md_text)
    guard_required(vals)

    # Theme colors
    if theme == "light":
        bg, card_bg, border, prim, sec = "#ffffff", "#eef2f7", "#d1d5db", "#0f172a", "#4b5563"
        acc, acc2 = "#0ea5a1", "#334155"
    else:
        bg, card_bg, border, prim, sec = "#0b1e2d", "#0f2637", "#1f3b53", "#e6edf3", "#9fb3c8"
        acc, acc2 = "#61dafb", "#a7b6c8"

    mono = FontProperties(family="DejaVu Sans Mono")
    fig_w_px, fig_h_px = 1200, 640
    fig = plt.figure(figsize=(fig_w_px / 100, fig_h_px / 100), dpi=100)
    ax = plt.gca()
    ax.set_axis_off()
    ax.set_xlim(0, fig_w_px)
    ax.set_ylim(0, fig_h_px)
    ax.add_patch(FancyBboxPatch((0, 0), fig_w_px, fig_h_px, boxstyle="square", fc=bg, ec=bg))

    # Title + subtitle
    ax.text(40, fig_h_px - 60, title, fontsize=14, color=prim, va="top", ha="left", weight="bold")
    subtitle = f"Tiles {fmt_int(vals['tiles_total'])} • Catalogs {fmt_int(vals['tiles_with_catalogs'])} • X-match {fmt_int(vals['tiles_with_xmatch'])}"
    fit_text(ax, subtitle, 40, fig_h_px - 100, max_px=fig_w_px - 80, fontsize=14, color=sec, va="top", ha="left")

    # Cards
    strict_label = f"NEOWISE strict (≤{vals['ir_threshold']}″)"
    cards = [
        ("Detections (PASS2)", fmt_int(vals["detections"]), ""),
        ("Canonical matches", fmt_int(vals["canonical"]), vals["canonical_pct"] or ""),
        ("Final no optical counterparts", fmt_int(vals["final_no_opt"]), vals["final_no_opt_pct"] or ""),
        (strict_label, fmt_int(vals["ir_strict"]), vals["ir_match_rate"] or ""),
        (
            "NEOWISE quality",
            f"W1≥5 {fmt_int(vals['w1_ge5'])} • W2≥5 {fmt_int(vals['w2_ge5'])}",
            f"any≥5 {fmt_int(vals['any_ge5'])} • med/p95 {vals['ir_sep_med'] or '—'}/{vals['ir_sep_p95'] or '—'}″",
        ),
    ]

    top_cards = cards[:3]
    bottom_cards = cards[3:]

    card_gap_x = 24
    columns_top = 3
    columns_bottom = 2
    card_w_top = (fig_w_px - 80 - (columns_top - 1) * card_gap_x) / columns_top
    card_w_bottom = (fig_w_px - 80 - (columns_bottom - 1) * card_gap_x) / columns_bottom
    card_h = 150

    x0, y0_top = 40, 320
    x0_bottom, y0_bottom = 40, 150

    # Top row
    for i, (label, big, pct) in enumerate(top_cards):
        x = x0 + i * (card_w_top + card_gap_x)
        y = y0_top
        ax.add_patch(FancyBboxPatch((x, y), card_w_top, card_h, boxstyle="round,pad=0.02,rounding_size=12", fc=card_bg, ec=border))
        ax.text(x + 20, y + card_h - 28, label, fontsize=12, color=sec, va="top", ha="left")
        fit_text(ax, big, x + 20, y + card_h - 68, card_w_top - 40, fontsize=14, color=acc, va="top", ha="left", weight="bold", fontproperties=mono)
        if pct:
            fit_text(ax, pct, x + 20, y + 26, card_w_top - 40, fontsize=12, color=acc2, va="bottom", ha="left")

    # Bottom row
    for i, (label, big, pct) in enumerate(bottom_cards):
        x = x0_bottom + i * (card_w_bottom + card_gap_x)
        y = y0_bottom
        ax.add_patch(FancyBboxPatch((x, y), card_w_bottom, card_h, boxstyle="round,pad=0.02,rounding_size=12", fc=card_bg, ec=border))
        ax.text(x + 20, y + card_h - 28, label, fontsize=12, color=sec, va="top", ha="left")
        fit_text(ax, big, x + 20, y + card_h - 68, card_w_bottom - 40, fontsize=14, color=acc, va="top", ha="left", weight="bold", fontproperties=mono)
        if pct:
            fit_text(ax, pct, x + 20, y + 26, card_w_bottom - 40, fontsize=12, color=acc2, va="bottom", ha="left")

    footer = 'Source: summarize_runs.py • CDS xmatch ≤5″ • NEOWISE strict ≤threshold″ • SVG export'
    ax.text(40, 40, footer, fontsize=12, color=sec, va="bottom", ha="left")

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, format="svg", bbox_inches="tight", facecolor=bg)


def main():
    ap = argparse.ArgumentParser(description="Render README key figures as SVG.")
    ap.add_argument('--src-md', default='', help='Path to run_summary.md (defaults to latest under ./data/metadata/qc/<DATE>/run_summary.md)')
    ap.add_argument('--out-dir', default='./images', help='Output directory for SVGs')
    ap.add_argument('--title', default='VASCO – Key Figures')
    ap.add_argument('--both', action='store_true', help='Render both themes (light and dark)')
    args = ap.parse_args()

    src_md = Path(args.src_md) if args.src_md else discover_latest_md()
    md_text = src_md.read_text(encoding='utf-8')

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    draw_banner(md_text, 'light', str(out_dir / 'readme-key-figures-light.svg'), title=args.title)
    if args.both:
        draw_banner(md_text, 'dark', str(out_dir / 'readme-key-figures-dark.svg'), title=args.title)


if __name__ == '__main__':
    raise SystemExit(main())