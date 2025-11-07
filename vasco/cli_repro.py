from __future__ import annotations
import argparse, json
from pathlib import Path
from typing import Dict, Any, List
import numpy as np
from astropy.table import Table

from .mnras.filters_mnras import apply_extract_filters, apply_morphology_filters
from .mnras.xmatch import ps1_match, gaia_match
from .mnras.hpm import backprop_gaia_row
from .mnras.buckets import init_buckets, finalize
from .mnras.report import write_summary
from .mnras.spikes import read_ecsv, SpikeConfig, fetch_bright_ps1, apply_spike_cuts, BrightStar


def _to_table(rows: List[Dict[str, Any]]) -> Table:
    if not rows:
        return Table()
    cols = sorted(rows[0].keys())
    return Table(rows=rows, names=cols)


def _center_from_rows(rows: List[Dict[str, Any]]) -> tuple[float, float]:
    if not rows:
        return float('nan'), float('nan')
    # try several column pairs
    for ra_k, dec_k in [('ALPHA_J2000','DELTA_J2000'), ('X_WORLD','Y_WORLD'), ('RA','DEC')]:
        if ra_k in rows[0] and dec_k in rows[0]:
            ras = np.array([float(r[ra_k]) for r in rows], dtype=float)
            decs = np.array([float(r[dec_k]) for r in rows], dtype=float)
            return float(np.nanmedian(ras)), float(np.nanmedian(decs))
    return float('nan'), float('nan')


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog='vasco.cli_repro', description='MNRAS 2022 reproduction (Section 2→3)')
    p.add_argument('--run-dir', required=True)
    p.add_argument('--yaml', default='mnras-repro.yaml')
    p.add_argument('--max-tiles', type=int, default=0)
    args = p.parse_args(argv)

    run = Path(args.run_dir)
    tiles_dir = run / 'tiles'
    if not tiles_dir.exists():
        print(f"[ERROR] Tiles directory missing: {tiles_dir}")
        return 2

    # config
    cfg = json.loads(json.dumps(_load_yaml(Path(args.yaml))))  # plain dict

    buckets = init_buckets()

    # iterate tiles with catalogs
    tiles = sorted([p for p in tiles_dir.iterdir() if p.is_dir()])
    processed = 0

    for td in tiles:
        if args.max_tiles and processed >= args.max_tiles:
            break
        cat = None
        if (td/'final_catalog.ecsv').exists():
            cat = td/'final_catalog.ecsv'
        elif (td/'pass2.ldac').exists():
            cat = td/'pass2.ldac'
        else:
            continue

        rows = read_ecsv(cat)
        if not rows:
            continue

        # compute tile center
        ra_c, dec_c = _center_from_rows(rows)
        # build bright list (PS1) for spikes
        scfg = SpikeConfig.from_yaml(Path(args.yaml))
        bright_list = fetch_bright_ps1(ra_c, dec_c,
                                       radius_arcmin=scfg.search_radius_arcmin,
                                       rmag_max=scfg.rmag_max_catalog,
                                       mindetections=2)

        # spikes first (works on list-of-dicts)
        kept, rej = apply_spike_cuts(rows, [BrightStar(b.ra, b.dec, b.rmag) for b in bright_list], scfg)
        buckets['spikes_rejected'] += len(rej)

        # to table for morphology
        tab = _to_table(kept)
        if len(tab) == 0:
            continue

        # apply Section 2 filters
        tab = apply_extract_filters(tab, cfg['filters']['extract'])
        # morphology
        before = len(tab)
        tab = apply_morphology_filters(tab, cfg['filters']['morphology'])
        buckets['morphology_rejected'] += max(0, before - len(tab))

        buckets['total_after_filters'] += len(tab)
        if len(tab) == 0:
            processed += 1
            continue

        # X-match (PS1 + Gaia at 5")
        matched_any = 0
        no_match_coords: List[tuple[float, float]] = []
        for row in tab:
            ra = float(row.get('ALPHA_J2000', row.get('X_WORLD', row.get('RA', float('nan')))))
            dec = float(row.get('DELTA_J2000', row.get('Y_WORLD', row.get('DEC', float('nan')))))
            if np.isnan(ra) or np.isnan(dec):
                continue
            m1 = ps1_match(ra, dec, cfg['xmatch']['radius_arcsec'])
            m2, ga = gaia_match(ra, dec, cfg['xmatch']['radius_arcsec'])
            if m1 or m2:
                matched_any += 1
            else:
                no_match_coords.append((ra, dec))

        buckets['matched_ps1_or_gaia'] += matched_any

        # HPM pass: for nomatch, look for a Gaia source within 30" and back-propagate
        hpm_count = 0
        for ra, dec in no_match_coords:
            m2, ga = gaia_match(ra, dec, r_arcsec=cfg['hpm']['search_radius_arcsec'])
            if not m2 or not ga:
                continue
            era, edec = backprop_gaia_row(ga, cfg['hpm']['poss_epoch_year'])
            if np.isnan(era) or np.isnan(edec):
                continue
            # if back-propagated position is within 5" of POSS detection, classify as HPM
            sep = np.hypot((era - ra) * np.cos(np.deg2rad(dec)), (edec - dec)) * 3600.0
            if sep <= cfg['xmatch']['radius_arcsec']:
                hpm_count += 1
        buckets['hpm_objects'] += hpm_count

        # unidentified = remainder after matches & HPM from total_after_filters for this tile
        unidentified_tile = max(0, len(tab) - matched_any - hpm_count)
        buckets['unidentified'] += unidentified_tile

        processed += 1

    write_summary(str(run), finalize(buckets),
                  cfg['outputs']['summary_md'], cfg['outputs']['summary_json'])
    print(f"[SUMMARY] processed_tiles={processed}  → {cfg['outputs']['summary_md']} / {cfg['outputs']['summary_json']}")
    return 0


def _load_yaml(path: Path) -> Dict[str, Any]:
    import yaml
    return yaml.safe_load(path.read_text(encoding='utf-8'))


if __name__ == '__main__':
    raise SystemExit(main())
