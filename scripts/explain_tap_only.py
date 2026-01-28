#!/usr/bin/env python3
import argparse, math, numpy as np, pandas as pd, pyarrow as pa
import pyarrow.dataset as pds, pyarrow.compute as pc
from pyarrow import fs as pafs

def arcsec2rad(a): return a / 206264.806

def bbox(ra0, dec0, r_arcsec):
    ddeg = math.degrees(arcsec2rad(r_arcsec))
    ra = pc.field('ra'); dec = pc.field('dec')
    ra_min, ra_max = (ra0 - ddeg) % 360.0, (ra0 + ddeg) % 360.0
    if ra_min <= ra_max: fra = (ra >= ra_min) & (ra <= ra_max)
    else:                fra = (ra >= ra_min) | (ra <= ra_max)
    fdec = (dec >= dec0 - ddeg) & (dec <= dec0 + ddeg)
    return fra & fdec

def years_all():
    return [f"year{i}" for i in range(1,12)] + ["addendum"]

def leaf(year, k5):
    k0 = k5 // 1024
    return f"nasa-irsa-wise/wise/neowiser/catalogs/p1bs_psd/healpix_k5/{year}/neowiser-healpix_k5-{year}.parquet/healpix_k0={k0}/healpix_k5={k5}/"

def k5_index(ra_deg, dec_deg):
    import healpy as hp
    nside=2**5
    theta = np.deg2rad(90.0 - dec_deg)
    phi   = np.deg2rad(ra_deg % 360.0)
    return int(hp.ang2pix(nside, theta, phi, nest=True))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--tap', required=True)
    ap.add_argument('--aws', required=True)
    ap.add_argument('--cntrs', required=True)
    ap.add_argument('--radius', type=float, default=5.0)
    a = ap.parse_args()

    tap = pd.read_csv(a.tap)
    aws = pd.read_csv(a.aws)
    missing = [int(x) for x in a.cntrs.replace(',', ' ').split()]
    tap_sub = tap[tap['cntr'].isin(missing)].copy()
    print('TAP rows for cntr(s):
', tap_sub[["row_id","in_ra","in_dec","cntr"]])

    fs = pafs.S3FileSystem(anonymous=True, region='us-west-2')
    for _, r in tap_sub.iterrows():
        ra0, dec0, cntr = float(r['in_ra']) % 360.0, float(r['in_dec']), int(r['cntr'])
        k5 = k5_index(ra0, dec0)
        print(f"
cntr={cntr} seed=({ra0:.9f},{dec0:.9f})  k5={k5}")
        hits=0
        for yr in years_all():
            path = leaf(yr, k5)
            try:
                ds = pds.dataset(path, format='parquet', filesystem=fs, partitioning='hive', exclude_invalid_files=True)
            except Exception:
                continue
            gates = ((pc.field('qual_frame')>0) & (pc.field('qi_fact')>0.0) &
                     (pc.field('saa_sep')>0.0) & (pc.field('w1snr')>=5.0) &
                     (pc.field('mjd')<=59198.0))
            tbl = ds.to_table(filter=bbox(ra0, dec0, a.radius) & gates,
                              columns=['cntr','ra','dec','mjd','w1snr','w2snr','moon_masked'])
            if tbl.num_rows == 0: continue
            mm = tbl['moon_masked']
            keep = pc.equal(pc.cast(mm, pa.utf8(), safe=False), pa.scalar('00'))
            keep = pc.or_(keep, pc.equal(pc.cast(mm, pa.utf8(), safe=False), pa.scalar('0')))
            try: keep = pc.or_(keep, pc.equal(pc.cast(mm, pa.int64(), safe=False), pa.scalar(0, pa.int64())))
            except Exception: pass
            tbl = tbl.filter(keep)
            if tbl.num_rows == 0: continue
            df = tbl.to_pandas()
            if (df['cntr']==cntr).any():
                hits += 1
                print(f"  {yr}: present (rows={len(df)})")
        if hits == 0:
            print('  not present in any year/addendum leaf (post-fix)')
        else:
            print(f'  present in {hits} year leaf(s)')
    print('
Present in AWS closest CSV:', sorted(set(aws[aws['cntr'].isin(missing)]['cntr'].tolist())))

if __name__ == '__main__':
    main()
