
#!/usr/bin/env bash
# ./scripts/fetch_ptf_objects_stilts.sh <positions.vot> <flags_out_root>
set -euo pipefail
POS=${1:?positions.vot required}
OUTROOT=${2:?flags out root required}
mkdir -p "$OUTROOT"

TMPDIR="$(mktemp -d -t ptf_XXXX)"
trap 'rm -rf "$TMPDIR"' EXIT
CSV="$TMPDIR/upload.csv"
VOT="$TMPDIR/upload.vot"
OUTCSV="$OUTROOT/flags_ptf_objects.csv"
OUTPARQ="$OUTROOT/flags_ptf_objects.parquet"

stilts tcopy in="$POS" ifmt=votable out="$CSV" ofmt=csv
stilts tcopy in="$CSV" ifmt=csv     out="$VOT" ofmt=votable

ADQL="SELECT u.NUMBER, COUNT(*) AS nmatch
FROM TAP_UPLOAD.t1 AS u
JOIN ptf_objects AS p
  ON 1 = CONTAINS(
       POINT('ICRS', p.ra, p.dec),
       CIRCLE('ICRS', u.ra, u.dec, 5.0/3600.0)
     )
GROUP BY u.NUMBER"

stilts tapquery \
  tapurl=https://irsa.ipac.caltech.edu/TAP \
  nupload=1 upload1="$VOT" upname1=t1 ufmt1=votable \
  adql="$ADQL" \
  out="$OUTCSV" ofmt=csv

python - <<'PY' "$OUTCSV" "$OUTPARQ"
import sys, pandas as pd, pyarrow as pa, pyarrow.parquet as pq
csv, outp = sys.argv[1], sys.argv[2]
df = pd.read_csv(csv)
if not df.empty and 'NUMBER' in df.columns:
    df['NUMBER'] = df['NUMBER'].astype(str)
    flag = df[['NUMBER']].drop_duplicates().assign(has_other_archive_match=True)
else:
    flag = pd.DataFrame(columns=['NUMBER','has_other_archive_match'])
pq.write_table(pa.Table.from_pandas(flag, preserve_index=False), outp)
print('[OK] PTF flags ->', outp, 'rows=', len(flag))
PY

