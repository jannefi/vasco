
#!/usr/bin/env bash
# ./scripts/fetch_supercosmos_stilts.sh <positions.vot> <flags_out_root>
set -euo pipefail
POS=${1:?positions.vot required}
OUTROOT=${2:?flags out root required}
mkdir -p "$OUTROOT"

TMPDIR="$(mktemp -d -t super_XXXX)"
trap 'rm -rf "$TMPDIR"' EXIT
CSV="$TMPDIR/upload.csv"
VOT="$TMPDIR/upload.vot"
OUTCSV="$OUTROOT/flags_supercosmos.csv"
OUTPARQ="$OUTROOT/flags_supercosmos.parquet"

stilts tcopy in="$POS" ifmt=votable out="$CSV" ofmt=csv
stilts tcopy in="$CSV" ifmt=csv     out="$VOT" ofmt=votable

ADQL="SELECT u.row_id, COUNT(*) AS nmatch
FROM TAP_UPLOAD.t1 AS u
JOIN supercosmos.sources AS s
  ON 1 = CONTAINS(
       POINT('ICRS', s.raj2000, s.dej2000),
       CIRCLE('ICRS', u.ra, u.dec, 5.0/3600.0)
     )
GROUP BY u.row_id"

stilts tapquery \
  tapurl=https://dc.g-vo.org/__system__/tap/run \
  nupload=1 upload1="$VOT" upname1=t1 ufmt1=votable \
  adql="$ADQL" \
  out="$OUTCSV" ofmt=csv

python - <<'PY' "$OUTCSV" "$OUTPARQ"
import sys, pandas as pd, pyarrow as pa, pyarrow.parquet as pq
csv, outp = sys.argv[1], sys.argv[2]
df = pd.read_csv(csv)
if not df.empty and 'row_id' in df.columns:
    df['row_id'] = df['row_id'].astype(str)
    flag = df[['row_id']].drop_duplicates().assign(is_supercosmos_artifact=True)
else:
    flag = pd.DataFrame(columns=['row_id','is_supercosmos_artifact'])
pq.write_table(pa.Table.from_pandas(flag, preserve_index=False), outp)
print('[OK] SuperCOSMOS flags ->', outp, 'rows=', len(flag))
PY

