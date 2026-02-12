#!/usr/bin/env bash
# PTF flags via IRSA TAP (/sync) using STILTS only for CSV->VOTable.
# Chunk-safe: writes one Parquet/CSV per input chunk, atomic tmp->final.
# Baseline policy: spatial cross-match only (paper-faithful). No epoch here.
# Usage:
#   ./scripts/fetch_ptf_objects_stilts.sh <positions.(csv|vot|xml)> <out_dir> [radius_arcsec] [ptf_table]
# Example:
#   ./scripts/fetch_ptf_objects_stilts.sh ./work/ptf_chunks/chunk_0001.csv \
#     ./data/local-cats/_master_optical_parquet_flags/ptf/parts 5 ptf_objects
set -euo pipefail

POS="${1:?positions file required (csv/vot/xml)}"
OUTDIR="${2:?output directory required}"
R_AS="${3:-5}"
PTF_TABLE="${4:-ptf_objects}"

mkdir -p "${OUTDIR}"

# Derive chunk key from input file
CHUNK="$(basename "$POS")"
CHUNK="${CHUNK%.*}"  # strip extension
STAMP="$(date -u +%FT%TZ)"

# Per-chunk paths
BASENAME="$(basename "${PTF_TABLE}")"
VOT="/tmp/ptf_upload.$$.${BASENAME}__${CHUNK}.vot"
CSV="${OUTDIR}/flags_${BASENAME}__${CHUNK}.csv"
PARQ_TMP="${OUTDIR}/flags_${BASENAME}__${CHUNK}.parquet.tmp"
PARQ="${OUTDIR}/flags_${BASENAME}__${CHUNK}.parquet"
HDR="${OUTDIR}/flags_${BASENAME}__${CHUNK}.headers.txt"
LOG="${OUTDIR}/flags_${BASENAME}__${CHUNK}.stilts.log"

# Skip if done (idempotent re-run)
if [ -s "${PARQ}" ]; then
  echo "[skip] ${CHUNK} already exists: ${PARQ}"
  exit 0
fi

# 0) Input stats
IN_TOTAL=$( (wc -l < "${POS}" 2>/dev/null) || echo 0 )
echo "[info] input: ${POS} rows(with header?)=${IN_TOTAL}" | tee "${LOG}"

# 1) Normalize to VOTable & rename reserved NUMBER->objectnumber (IRSA-friendly)
case "${POS##*.}" in
  csv|CSV)
    stilts tpipe in="${POS}" ifmt=csv \
      cmd='colmeta -name objectnumber NUMBER' \
      out="${VOT}" ofmt=votable
    ;;
  vot|xml|VOT|XML)
    stilts tpipe in="${POS}" ifmt=votable \
      cmd='colmeta -name objectnumber NUMBER' \
      out="${VOT}" ofmt=votable
    ;;
  *)
    echo "[error] Unsupported positions format: ${POS}" | tee -a "${LOG}"
    exit 2
    ;;
esac
echo "[info] upload VOTable: ${VOT}" | tee -a "${LOG}"

# 2) ADQL (single-line, 5\" radius default)
ADQL="SELECT DISTINCT u.objectnumber
      FROM TAP_UPLOAD.my_table AS u, ${PTF_TABLE} AS p
      WHERE CONTAINS(POINT(p.ra,p.dec), CIRCLE(u.ra,u.dec, ${R_AS}/3600.0))=1"
echo "[adql] ${ADQL}" | tee -a "${LOG}"

# 3) IRSA TAP /sync, multipart upload
HTTP_CODE=$(curl -sS -X POST 'https://irsa.ipac.caltech.edu/TAP/sync' \
  -F 'REQUEST=doQuery' \
  -F 'LANG=ADQL' \
  -F 'FORMAT=csv' \
  -F 'UPLOAD=my_table,param:table' \
  -F "table=@${VOT};type=application/x-votable+xml" \
  -F "QUERY=${ADQL}" \
  -D "${HDR}" \
  -w '%{http_code}' \
  -o "${CSV}")
echo "[http] status=${HTTP_CODE} headers=${HDR} body=${CSV}" | tee -a "${LOG}"

# 3b) Fail fast on VOTable ERROR payload
if grep -q 'QUERY_STATUS" value="ERROR"' "${CSV}" 2>/dev/null; then
  echo "[error] IRSA returned VOTable ERROR. First lines:" | tee -a "${LOG}"
  head -n 40 "${CSV}" | tee -a "${LOG}"
  exit 2
fi

# 4) CSV -> Parquet flags (map objectnumber->NUMBER) + provenance
python - <<'PY' "${CSV}" "${PARQ_TMP}" "${CHUNK}" "${R_AS}" "${PTF_TABLE}" "${STAMP}"
import sys, pandas as pd, pyarrow.parquet as pq, pyarrow as pa
csv, parq_tmp, chunk, r_as, ptf_table, stamp = sys.argv[1:7]
df = pd.read_csv(csv)
if not df.empty and 'objectnumber' in df.columns:
    flags = (df[['objectnumber']]
             .drop_duplicates()
             .rename(columns={'objectnumber':'NUMBER'}))
    flags['has_other_archive_match'] = True
else:
    flags = pd.DataFrame(columns=['NUMBER','has_other_archive_match'])
# Provenance columns (optional but useful)
flags['source_chunk'] = chunk
flags['query_radius_arcsec'] = float(r_as)
flags['ptf_table'] = ptf_table
flags['queried_at_utc'] = stamp
pq.write_table(pa.Table.from_pandas(flags, preserve_index=False), parq_tmp)
print("[OK] Wrote", parq_tmp, "rows=", len(flags))
PY

# 5) Atomic move into place
mv -f "${PARQ_TMP}" "${PARQ}"
echo "[DONE] CSV=${CSV} PARQUET=${PARQ}" | tee -a "${LOG}"
