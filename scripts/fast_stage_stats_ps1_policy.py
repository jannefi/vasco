#!/usr/bin/env python3
"""
FAST stage stats with PS1 coverage policy support.

Outputs:
  (A) All processed tiles (baseline)
  (B) PS1-eligible subset (primary funnel), using allowlist file
  (C) PS1-excluded subset counts (reported, not included in primary funnel)

Notes / limitations:
- This is PRE-DEDUP and PRE-PLATE-EDGE filtering. It reports counts as-is.
- It can report exact duplicate tile centers (same RA/Dec in tile name) as a hint,
  but does not attempt science dedupe or plate-edge exclusion.
"""

import argparse, glob, json, re
from pathlib import Path
import numpy as np


def load_json(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def parse_tile_center(tile_dir: Path):
    """
    Parse RA/Dec from tile directory name like: tile-RA23.738-DEC-31.435
    Returns (ra, dec) floats or (None, None).
    """
    name = tile_dir.name
    m = re.match(r"tile-RA([0-9.]+)-DEC([+-]?[0-9.]+)$", name)
    if not m:
        return None, None
    try:
        return float(m.group(1)), float(m.group(2))
    except Exception:
        return None, None


def read_tile_list(path: Path):
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        return None
    lines = [ln.strip() for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]
    # normalize to Path strings
    return set(lines)


def stats(a: np.ndarray):
    if a.size == 0:
        return dict(min=None, p50=None, p90=None, max=None, mean=None)
    return dict(
        min=int(a.min()),
        p50=float(np.median(a)),
        p90=float(np.quantile(a, 0.9, method="linear")),
        max=int(a.max()),
        mean=float(a.mean()),
    )


def compute_scope(tiles, label, eligible_set=None, excluded_set=None):
    """
    Compute stats for a tile scope.
    If eligible_set is provided, only tiles whose str(path) is in that set are included.
    If excluded_set is provided, tiles whose str(path) is in that set are excluded.
    """
    vals_start, vals_opt, vals_late = [], [], []
    used_paths = []
    processed = 0
    missing_late = 0

    for t in tiles:
        t_str = str(t)
        if eligible_set is not None and t_str not in eligible_set:
            continue
        if excluded_set is not None and t_str in excluded_set:
            continue

        ms = t / "MNRAS_SUMMARY.json"
        if not ms.exists():
            continue
        d = load_json(ms)
        if not isinstance(d, dict):
            continue

        start = d.get("veto_start_rows")
        opt = d.get("veto_after_usnob_rows")

        if not isinstance(start, int) or start <= 0:
            continue
        if not isinstance(opt, int) or opt < 0:
            continue

        # Late survivors (fast): prefer total_after_filters only if it's positive,
        # otherwise use late_kept_hard_gates; fallback to 0.
        late = d.get("total_after_filters")
        if isinstance(late, int) and late > 0:
            pass
        else:
            late2 = d.get("late_kept_hard_gates")
            if isinstance(late2, int) and late2 >= 0:
                late = late2
            else:
                late = 0
                missing_late += 1

        vals_start.append(start)
        vals_opt.append(opt)
        vals_late.append(late)
        used_paths.append(t)
        processed += 1

    arr_start = np.array(vals_start, dtype=int)
    arr_opt = np.array(vals_opt, dtype=int)
    arr_late = np.array(vals_late, dtype=int)

    tot_start = int(arr_start.sum()) if arr_start.size else 0
    tot_opt = int(arr_opt.sum()) if arr_opt.size else 0
    tot_late = int(arr_late.sum()) if arr_late.size else 0

    print("")
    print(f"=== {label} ===")
    print(f"processed_tiles: {processed}")
    print(f"potential_sources (veto_start_rows): total={tot_start} stats={stats(arr_start)}")
    if tot_start:
        print(f"optical_veto_survivors (veto_after_usnob_rows): total={tot_opt} stats={stats(arr_opt)} rate={(tot_opt/tot_start):.4%}")
        print(f"late_survivors (JSON fast): total={tot_late} stats={stats(arr_late)} rate={(tot_late/tot_start):.4%} late/opt={(tot_late/tot_opt if tot_opt else 0):.4%}")
    else:
        print(f"optical_veto_survivors (veto_after_usnob_rows): total={tot_opt} stats={stats(arr_opt)} rate=N/A")
        print(f"late_survivors (JSON fast): total={tot_late} stats={stats(arr_late)} rate=N/A late/opt=N/A")

    print("")
    print("late counter source breakdown:")
    # this script treats total_after_filters as authoritative only if >0
    print("  used total_after_filters (>0): 0 (expected, writer currently stale)")
    print(f"  used late_kept_hard_gates:     {processed - missing_late}")
    print(f"  missing (forced 0):            {missing_late}")

    # Top-10 by late survivors (within this scope)
    if arr_late.size:
        idx = np.argsort(-arr_late)[:10]
        print("")
        print("top10 tiles by late_survivors (JSON fast):")
        for i in idx:
            td = used_paths[i]
            d = load_json(td / "MNRAS_SUMMARY.json") or {}
            late = d.get("late_kept_hard_gates")
            if not isinstance(late, int):
                late = 0
            print(f"  {td} late={late} opt={d.get('veto_after_usnob_rows')} pot={d.get('veto_start_rows')}")

    return used_paths


def report_exact_duplicate_centers(tile_paths, max_show=20):
    """
    Report exact duplicate tile centers (same tile-RA...-DEC... name).
    This is only a hint about overlaps; does not detect near-duplicates.
    """
    from collections import defaultdict
    buckets = defaultdict(list)
    for td in tile_paths:
        ra, dec = parse_tile_center(Path(td))
        if ra is None:
            continue
        key = (ra, dec)
        buckets[key].append(str(td))

    dups = [(k, v) for k, v in buckets.items() if len(v) > 1]
    dups.sort(key=lambda kv: len(kv[1]), reverse=True)

    print("")
    print("=== Duplicate tile-center hint (exact RA/Dec match only) ===")
    print(f"exact-duplicate centers: {len(dups)}")
    if not dups:
        return
    shown = 0
    for (ra, dec), paths in dups:
        print(f"- RA={ra:.6f} DEC={dec:.6f} count={len(paths)}")
        for p in paths[:5]:
            print(f"    {p}")
        if len(paths) > 5:
            print("    ...")
        shown += 1
        if shown >= max_show:
            break


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tiles-root", default="data/tiles_by_sky", help="root containing ra_bin/dec_bin/tile-RA*-DEC* trees")
    ap.add_argument("--eligible-list", default="work/triage/tiles_ps1_eligible.txt",
                    help="tile allowlist file (PS1-eligible). If missing, PS1-eligible section is skipped.")
    ap.add_argument("--excluded-list", default="work/triage/tiles_ps1_excluded.txt",
                    help="tile excluded list file (PS1-unavailable). Used for reporting only.")
    ap.add_argument("--report-duplicate-centers", action="store_true",
                    help="report exact duplicate tile centers (hint only; not true dedupe)")
    args = ap.parse_args()

    root = Path(args.tiles_root)
    tiles = [Path(p) for p in glob.glob(str(root / "**/tile-RA*-DEC*"), recursive=True)]
    tiles = [t for t in tiles if t.is_dir()]

    eligible_set = read_tile_list(Path(args.eligible_list)) if args.eligible_list else None
    excluded_set = read_tile_list(Path(args.excluded_list)) if args.excluded_list else None

    # Baseline: all tiles
    all_used = compute_scope(tiles, "ALL tiles (pre-dedup, pre-edge filtering)")

    # Report PS1 excluded tiles count (as provided)
    if excluded_set is not None:
        print("")
        print("=== PS1 coverage / availability exclusion (reported, not included in PS1-eligible funnel) ===")
        print(f"PS1 excluded tile paths in list: {len(excluded_set)}")
        # Show a few examples
        ex = list(excluded_set)[:10]
        if ex:
            print("examples:")
            for x in ex:
                print(f"  {x}")

    # PS1-eligible: only if allowlist exists
    if eligible_set is not None:
        ps1_used = compute_scope(tiles, "PS1-ELIGIBLE tiles only (primary funnel)",
                                 eligible_set=eligible_set)

        # sanity: how many processed tile dirs overlap between the two views
        print("")
        print("=== Scope sanity ===")
        print(f"eligible list size: {len(eligible_set)}")
        print(f"processed in PS1-eligible scope: {len(ps1_used)}")
        print(f"processed in ALL scope: {len(all_used)}")

        if args.report_duplicate_centers:
            report_exact_duplicate_centers([str(p) for p in ps1_used])
    else:
        print("")
        print("NOTE: eligible list not found; skipping PS1-eligible funnel section.")
        if args.report_duplicate_centers:
            report_exact_duplicate_centers([str(p) for p in all_used])


if __name__ == "__main__":
    main()
