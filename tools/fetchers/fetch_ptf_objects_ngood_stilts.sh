#!/usr/bin/env bash
# PTF flags (ngood-gated) via IRSA TAP (/sync) using STILTS for CSV->VOTable.
# Quality gate: COALESCE(p.ngoodobs,0) > 0  (PTF OBJECTS table only)
# Idempotent & network-resilient.
set -euo pipefail

POS="${1:?positions file required (csv/vot/xml)}"
OUTDIR="${2:?output directory required}"
R_AS="${3:-5}"
PTF_TABLE="${4:-ptf_objects}"

mkdir -p "${OUTDIR}"

CHUNK="$(basename "$POS")"; CHUNK="${CHUNK%.*}"
STAMP="$(date -u +%FT%TZ)"
BASENAME="$(basename "${PTF_TABLE}")"
VOT="/tmp/ptf_upload.$$.${BASENAME}__${CHUNK}.vot"
CSV="${OUTDIR}/flags_${BASENAME}__${CHUNK}.csv"
PARQ_TMP="${OUTDIR}/flags_${BASENAME}__${CHUNK}.parquet.tmp"
PARQ="${OUTDIR}/flags_${BASENAME}__${CHUNK}.parquet"
HDR="${OUTDIR}/flags_${BASENAME}__${CHUNK}.headers.txt"
LOG="${OUTDIR}/flags_${BASENAME}__${CHUNK}.stilts.log"

trap 'rm -f "${VOT}" "${PARQ_TMP}"' EXIT

# Skip if already done
[ -s "${PARQ}" ] && { echo "[skip] ${CHUNK} exists: ${PARQ}"; exit 0; }

# 0) Input stats
IN_TOTAL=$( (wc -l < "${POS}" 2>/dev/null) || echo 0 )
echo "[info] input: ${POS} rows=${IN_TOTAL}" | tee "${LOG}"

# 1) Normalize to VOTable & rename reserved NUMBER->objectnumber
case "${POS##*.}" in
  csv|CSV) stilts tpipe in="${POS}" ifmt=csv \
             cmd='colmeta -name objectnumber NUMBER' \
             out="${VOT}" ofmt=votable ;;
  vot|xml|VOT|XML) stilts tpipe in="${POS}" ifmt=votable \
             cmd='colmeta -name objectnumber NUMBER' \
             out="${VOT}" ofmt=votable ;;
  *) echo "[error] Unsupported positions format: ${POS}" | tee -a "${LOG}"; exit 2 ;;
esac
echo "[info] upload VOTable: ${VOT}" | tee -a "${LOG}"

# 2) ADQL with 5″ cone AND quality gate ngoodobs>0 (paper-like)
ADQL="SELECT DISTINCT u.objectnumber
      FROM TAP_UPLOAD.my_table AS u, ${PTF_TABLE} AS p
      WHERE CONTAINS(POINT(p.ra,p.dec), CIRCLE(u.ra,u.dec, ${R_AS}/3600.0)) = 1
        AND COALESCE(p.ngoodobs,0) > 0"
echo "[adql] ${ADQL}" | tee -a "${LOG}"

# 3) IRSA TAP /sync with retries (DNS & transient-safe)
attempt=0; max_attempts=8
while :; do
  attempt=$((attempt+1))
  HTTP_CODE=$(curl -sS -X POST 'https://irsa.ipac.caltech.edu/TAP/sync' \
    --retry 6 --retry-delay 2 --retry-max-time 180 --retry-all-errors \
    --connect-timeout 20 --max-time 300 \
    -F 'REQUEST=doQuery' -F 'LANG=ADQL' -F 'FORMAT=csv' \
    -F 'UPLOAD=my_table,param:table' \
    -F "table=@${VOT};type=application/x-votable+xml" \
    -F "QUERY=${ADQL}" -D "${HDR}" -w '%{http_code}' -o "${CSV}" || echo "curl_failed")
  rc=$?

  if [ "$rc" = "curl_failed" ] || [ "$rc" -ne 0 ]; then
    echo "[warn] curl error rc=${rc} (attempt ${attempt}/${max_attempts})" | tee -a "${LOG}"
  elif grep -q 'QUERY_STATUS" value="ERROR"' "${CSV}" 2>/dev/null; then
    echo "[warn] IRSA VOTable ERROR (attempt ${attempt}/${max_attempts}); first lines:" | tee -a "${LOG}"
    head -n 40 "${CSV}" | tee -a "${LOG}"
  else
    echo "[http] status=${HTTP_CODE} headers=${HDR} body=${CSV}" | tee -a "${LOG}"
    break
  fi

  [ "${attempt}" -ge "${max_attempts}" ] && { echo "[error] giving up after ${max_attempts} attempts"; exit 3; }
  # jittered backoff (helps DNS storms)
  sleep $(( (RANDOM % 3) + attempt ))
done

# 4) CSV -> Parquet flags + provenance
python - <<'PY' "${CSV}" "${PARQ_TMP}" "${CHUNK}" "${R_AS}" "${PTF_TABLE}" "${STAMP}"
import sys, pandas as pd, pyarrow.parquet as pq, pyarrow as pa
csv, parq_tmp, chunk, r_as, ptf_table, stamp = sys.argv[1:7]
df = pd.read_csv(csv)
if not df.empty and 'objectnumber' in df.columns:
    flags = (df[['objectnumber']].drop_duplicates()
             .rename(columns={'objectnumber':'NUMBER'}))
    flags['ptf_match_ngood'] = True
else:
    flags = pd.DataFrame(columns=['NUMBER','ptf_match_ngood'])
flags['source_chunk'] = chunk
flags['query_radius_arcsec'] = float(r_as)
flags['ptf_table'] = ptf_table
flags['queried_at_utc'] = stamp
pq.write_table(pa.Table.from_pandas(flags, preserve_index=False), parq_tmp)
print("[OK] Wrote", parq_tmp, "rows=", len(flags))
PY

mv -f "${PARQ_TMP}" "${PARQ}"
echo "[DONE] CSV=${CSV} PARQUET=${PARQ}" | tee -a "${LOG}"
