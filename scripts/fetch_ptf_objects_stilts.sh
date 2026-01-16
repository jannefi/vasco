
#!/usr/bin/env bash
# PTF flags via IRSA TAP (/sync) using STILTS only for CSV->VOTable.
# - Renames reserved NUMBER -> objectnumber in the upload
# - Single-line ADQL with 5" circle by default (no aggregates)
# - Writes CSV + Parquet flags, with a small run log
#
# Usage:
#   ./scripts/fetch_ptf_objects_stilts.sh <positions.(csv|vot|xml)> <out_dir> [radius_arcsec] [ptf_table]
# Example:
#   ./scripts/fetch_ptf_objects_stilts.sh ./work/positions_upload.csv \
#       ./data/local-cats/_master_optical_parquet_flags 5 ptf_objects
set -euo pipefail

POS="${1:?positions file required (csv/vot/xml)}"
OUTDIR="${2:?output directory required}"
R_AS="${3:-5}"
PTF_TABLE="${4:-ptf_objects}"

mkdir -p "$OUTDIR"

BASENAME="$(basename "$PTF_TABLE")"
VOT="/tmp/ptf_upload.$$.$BASENAME.vot"
CSV="$OUTDIR/flags_${BASENAME}.csv"
PARQ="$OUTDIR/flags_${BASENAME}.parquet"
HDR="$OUTDIR/flags_${BASENAME}.headers.txt"
LOG="$OUTDIR/flags_${BASENAME}.stilts.log"

# 0) Input stats
IN_TOTAL=$(wc -l < "$POS" 2>/dev/null || echo 0)
echo "[info] input: $POS  rows(with header?)=$IN_TOTAL" | tee "$LOG"

# 1) Normalize to VOTable & rename reserved NUMBER->objectnumber
case "${POS##*.}" in
  csv|CSV)
    stilts tpipe in="$POS" ifmt=csv \
      cmd='colmeta -name objectnumber NUMBER' \
      out="$VOT" ofmt=votable
    ;;
  vot|xml|VOT|XML)
    # If already VOTable, still ensure objectnumber exists; if not, try to rename NUMBER
    stilts tpipe in="$POS" ifmt=votable \
      cmd='colmeta -name objectnumber NUMBER' \
      out="$VOT" ofmt=votable
    ;;
  *)
    echo "[error] Unsupported positions format: $POS" | tee -a "$LOG"
    exit 2
    ;;
esac
echo "[info] upload VOTable: $VOT" | tee -a "$LOG"

# 2) IRSA-friendly ADQL (single-line, no aggregates)
ADQL="SELECT DISTINCT u.objectnumber FROM TAP_UPLOAD.my_table AS u, ${PTF_TABLE} AS p WHERE CONTAINS(POINT(p.ra,p.dec),CIRCLE(u.ra,u.dec, ${R_AS}/3600.0))=1"
echo "[adql] ${ADQL}" | tee -a "$LOG"

# 3) IRSA TAP /sync with multipart upload
HTTP_CODE=$(curl -sS -X POST 'https://irsa.ipac.caltech.edu/TAP/sync' \
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

# 3b) Fail fast if IRSA returned a VOTable ERROR payload
if grep -q 'QUERY_STATUS" value="ERROR"' "$CSV" 2>/dev/null; then
  echo "[error] IRSA returned VOTable ERROR. First lines:" | tee -a "$LOG"
  head -n 40 "$CSV" | tee -a "$LOG"
  exit 2
fi

# 4) Output stats
OUT_TOTAL=$(wc -l < "$CSV" | awk '{print $1}')
OUT_DATA=$(( OUT_TOTAL > 0 ? OUT_TOTAL - 1 : 0 ))
echo "[info] output CSV rows: total=$OUT_TOTAL data_rows=$OUT_DATA" | tee -a "$LOG"

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

echo "[DONE] CSV=$CSV  PARQUET=$PARQ" | tee -a "$LOG"
