#!/usr/bin/env bash
# ./scripts/fetch_vsx_stilts.sh <positions.vot> <flags_out_root>
set -euo pipefail
POS=${1:?positions.vot required}
OUTROOT=${2:?flags out root required}
mkdir -p "$OUTROOT"

# Re-encode: VOT -> CSV -> VOT (STILTS-built VOTable expected by VizieR)
TMPDIR="$(mktemp -d -t vsx_XXXX)"
trap 'rm -rf "$TMPDIR"' EXIT
CSV="$TMPDIR/upload.csv"
VOT="$TMPDIR/upload.vot"
OUTCSV="$OUTROOT/flags_vsx.csv"
OUTPARQ="$OUTROOT/flags_vsx.parquet"

stilts tcopy in="$POS" ifmt=votable out="$CSV" ofmt=csv
stilts tcopy in="$CSV" ifmt=csv     out="$VOT" ofmt=votable

# Inline ADQL (note: inner "..." are escaped; 'ICRS' are simple single-quotes)
ADQL="SELECT u.NUMBER, COUNT(*) AS nmatch
FROM TAP_UPLOAD.t1 AS u
JOIN \"B/vsx/vsx\" AS v
  ON 1 = CONTAINS(
       POINT('ICRS', v.RAJ2000, v.DEJ2000),
       CIRCLE('ICRS', u.ra, u.dec, 5.0/3600.0)
     )
GROUP BY u.NUMBER"

stilts tapquery \
  tapurl=https://tapvizier.cds.unistra.fr/TAPVizieR/tap \
  nupload=1 upload1="$VOT" upname1=t1 ufmt1=votable \
  adql="$ADQL" \
  out="$OUTCSV" ofmt=csv

python - <<'PY' "$OUTCSV" "$OUTPARQ"
import sys, pandas as pd, pyarrow as pa, pyarrow.parquet as pq
csv, outp = sys.argv[1], sys.argv[2]
df = pd.read_csv(csv)
if not df.empty and 'NUMBER' in df.columns:
    df['NUMBER'] = df['NUMBER'].astype(str)
    flag = df[['NUMBER']].drop_duplicates().assign(is_known_variable_or_transient=True)
else:
    import pandas as pd
    flag = pd.DataFrame(columns=['NUMBER','is_known_variable_or_transient'])
pq.write_table(pa.Table.from_pandas(flag, preserve_index=False), outp)
print('[OK] VSX flags ->', outp, 'rows=', len(flag))
PY
