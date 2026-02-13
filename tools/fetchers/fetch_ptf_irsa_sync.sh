
#!/usr/bin/env bash
# PTF flags via IRSA TAP (/sync) with upload as VOTable.
# - Renames reserved NUMBER->objectnumber in the upload (VOTable)
# - Runs single-line ADQL with a circle (default 5")
# - Writes CSV + Parquet flags
# - Logs ADQL + HTTP headers + row counts
# - Produces a mini-report verifying matches by numeric ID normalization
#
# Usage:
#   ./scripts/fetch_ptf_irsa_sync.sh <positions.csv> <out_dir> [radius_arcsec] [ptf_table]
# Example:
#   ./scripts/fetch_ptf_irsa_sync.sh ./work/positions_upload_50.csv \
#       ./data/local-cats/_master_optical_parquet_flags 5 ptf_objects
#
# Verbose mode:
#   VERBOSE=1 ./scripts/fetch_ptf_irsa_sync.sh ...
set -euo pipefail

POS="${1:?positions.csv required}"           # input CSV: NUMBER,ra,dec (ICRS deg)
OUTDIR="${2:?output directory required}"
R_AS="${3:-5}"                               # search radius in arcsec
PTF_TABLE="${4:-ptf_objects}"                # or ptf_sources
VERBOSE="${VERBOSE:-0}"

mkdir -p "$OUTDIR"

# Derived paths
BASENAME="$(basename "$PTF_TABLE")"
VOT="/tmp/positions_upload.$$.$BASENAME.vot"
CSV="$OUTDIR/flags_${BASENAME}.csv"
PARQ="$OUTDIR/flags_${BASENAME}.parquet"
HDR="$OUTDIR/flags_${BASENAME}.headers.txt"
LOG="$OUTDIR/flags_${BASENAME}.log"
RPT="$OUTDIR/flags_${BASENAME}_report.txt"
DIAG="$OUTDIR/flags_${BASENAME}_pairs_sample.csv"

# Verbosity flag for curl (string, not array — avoids nounset issues)
CURLV=""
if [ "$VERBOSE" = "1" ]; then
  set -x
  CURLV="-v"
fi

# 0) Input stats
IN_TOTAL=$(wc -l < "$POS" | awk '{print $1}')
IN_DATA=$(( IN_TOTAL > 0 ? IN_TOTAL - 1 : 0 ))
echo "[info] input: $POS  rows(with header)=$IN_TOTAL  data_rows=$IN_DATA" | tee "$LOG"

# 1) CSV -> VOTable; rename reserved NUMBER->objectnumber (no type coercion)
#    Matches the structure that worked for you.
stilts tpipe \
  in="$POS" ifmt=csv \
  cmd='colmeta -name objectnumber NUMBER' \
  out="$VOT" ofmt=votable

echo "[info] wrote VOTable upload: $VOT" | tee -a "$LOG"

# 2) IRSA-friendly ADQL (single line, no aggregates)
ADQL="SELECT DISTINCT u.objectnumber FROM TAP_UPLOAD.my_table AS u, ${PTF_TABLE} AS p WHERE CONTAINS(POINT(p.ra,p.dec),CIRCLE(u.ra,u.dec, ${R_AS}/3600.0))=1"
echo "[adql] ${ADQL}" | tee -a "$LOG"

# 3) IRSA TAP /sync multipart upload
HTTP_CODE=$(curl -sS $CURLV -X POST 'https://irsa.ipac.caltech.edu/TAP/sync' \
  -F 'REQUEST=doQuery' \
  -F 'LANG=ADQL' \
  -F 'FORMAT=csv' \
  -F 'UPLOAD=my_table,param:table' \
  -F "table=@${VOT};type=application/x-votable+xml" \
  -F "QUERY=${ADQL}" \
  -D "$HDR" \
  -w '%{http_code}' \
  -o "$CSV")

echo "[http] status=$HTTP_CODE  headers=$HDR  body=$CSV" | tee -a "$LOG"

# 3b) Fail fast if IRSA returned an ERROR VOTable in the body
if grep -q 'QUERY_STATUS" value="ERROR"' "$CSV" 2>/dev/null; then
  echo "[error] IRSA returned VOTable ERROR. First lines:" | tee -a "$LOG"
  head -n 40 "$CSV" | tee -a "$LOG"
  exit 2
fi

# 4) Output stats
OUT_TOTAL=$(wc -l < "$CSV" | awk '{print $1}')
OUT_DATA=$(( OUT_TOTAL > 0 ? OUT_TOTAL - 1 : 0 ))
OUT_DISTINCT=$(awk 'NR>1 && $1!="" {print $1}' "$CSV" | sort | uniq | wc -l | awk '{print $1}')
echo "[info] output CSV rows: total=$OUT_TOTAL data_rows=$OUT_DATA distinct_objectnumber=$OUT_DISTINCT" | tee -a "$LOG"

# 5) CSV -> Parquet flags (map objectnumber -> NUMBER)
python - <<'PY' "$CSV" "$PARQ"
import sys, pandas as pd, pyarrow.parquet as pq, pyarrow as pa
csv, parq = sys.argv[1], sys.argv[2]
df = pd.read_csv(csv)
if not df.empty and 'objectnumber' in df.columns:
    flags = df[['objectnumber']].drop_duplicates().rename(columns={'objectnumber':'NUMBER'})
    flags['has_other_archive_match'] = True
else:
    flags = pd.DataFrame(columns=['NUMBER','has_other_archive_match'])
pq.write_table(pa.Table.from_pandas(flags, preserve_index=False), parq)
print("[OK] Wrote", parq, "rows=", len(flags))
PY

# 6) Mini‑report: verify matches numerically ("1.0" vs "1" normalization)
python - <<'PY' "$POS" "$CSV" "$RPT"
import sys, pandas as pd
pos, csv, rpt = sys.argv[1], sys.argv[2], sys.argv[3]
src = pd.read_csv(pos)  # expects NUMBER,ra,dec
hit = pd.read_csv(csv)  # column: objectnumber
src['NUMBER_norm'] = pd.to_numeric(src['NUMBER'], errors='coerce').astype('Int64')
hit['NUMBER_norm'] = pd.to_numeric(hit['objectnumber'], errors='coerce').astype('Int64')
merged = src.merge(hit[['NUMBER_norm']], on='NUMBER_norm', how='left', indicator=True)
tot = len(src); matched = (merged['_merge']=='both').sum(); unmatched = (merged['_merge']=='left_only').sum()
lines = [
    f"Input: {pos}",
    f"Results CSV: {csv}",
    f"Total={tot}  Matched={matched}  Unmatched={unmatched}",
    "",
    "Sample unmatched rows (up to 10):",
    merged.loc[merged['_merge']=='left_only', ['NUMBER','ra','dec']].head(10).to_csv(index=False)
]
with open(rpt, 'w') as f:
    f.write("\n".join(lines))
print("[report]", rpt)
PY

# 7) Optional diagnostic: small sample of nearest pairs (forces numeric in ADQL via *1.0)
ADQL_DIAG="SELECT TOP 20 u.objectnumber,(1.0*u.ra) AS u_ra,(1.0*u.dec) AS u_dec,p.ra AS p_ra,p.dec AS p_dec, DISTANCE(POINT(1.0*u.ra,1.0*u.dec),POINT(p.ra,p.dec)) AS sep_deg FROM TAP_UPLOAD.my_table AS u, ${PTF_TABLE} AS p WHERE DISTANCE(POINT(1.0*u.ra,1.0*u.dec),POINT(p.ra,p.dec)) < (${R_AS}/3600.0) ORDER BY u.objectnumber, sep_deg"
curl -sS $CURLV -X POST 'https://irsa.ipac.caltech.edu/TAP/sync' \
  -F 'REQUEST=doQuery' \
  -F 'LANG=ADQL' \
  -F 'FORMAT=csv' \
  -F 'UPLOAD=my_table,param:table' \
  -F "table=@${VOT};type=application/x-votable+xml" \
  -F "QUERY=${ADQL_DIAG}" \
  -o "$DIAG" || true

echo "[DONE] CSV=$CSV  PARQUET=$PARQ  REPORT=$RPT  DIAG=$DIAG" | tee -a "$LOG"
