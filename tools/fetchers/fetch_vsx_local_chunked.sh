#!/usr/bin/env bash
# Local VSX match for one chunk using STILTS tskymatch2, then write a Parquet flag table.
# Usage:
#   ./tools/fetchers/fetch_vsx_local_chunked.sh <positions.(csv|vot|xml)> <out_dir> [radius_arcsec] [vsx_fits] [id_col]
set -euo pipefail

POS=${1:?positions file required (csv/vot/xml)}
OUTDIR=${2:?output directory required}
R_AS="${3:-5}"
VSX_FITS="${4:-./data/local-cats/_external_catalogs/vsx/vsx_master_slim.fits}"
ID_COL="${5:-${ID_COL:-NUMBER}}"    # env override supported

mkdir -p "${OUTDIR}" "${OUTDIR}/logs"
CHUNK="$(basename "$POS")"; CHUNK="${CHUNK%.*}"
STAMP="$(date -u +%FT%TZ)"

OUTCSV="${OUTDIR}/flags_vsx__${CHUNK}.csv"
PARQ_TMP="${OUTDIR}/flags_vsx__${CHUNK}.parquet.tmp"
PARQ="${OUTDIR}/flags_vsx__${CHUNK}.parquet"
LOG="${OUTDIR}/logs/vsx_local__${CHUNK}.log"

# Idempotent: skip if final exists
if [[ -s "${PARQ}" ]]; then
  echo "[skip] ${CHUNK} already exists: ${PARQ}"
  exit 0
fi

# Input → CSV (ensure ra/dec columns are visible to STILTS)
TMPDIR="$(mktemp -d -t vsxloc_XXXX)"; trap 'rm -rf "$TMPDIR"' EXIT
CSV="${TMPDIR}/in.csv"

case "${POS##*.}" in
  csv|CSV) cp -f "${POS}" "${CSV}" ;;
  vot|xml|VOT|XML) stilts tcopy in="${POS}" ifmt=votable out="${CSV}" ofmt=csv >>"${LOG}" 2>&1 ;;
  *) echo "[error] unsupported input: ${POS}" | tee -a "${LOG}"; exit 2 ;;
esac

# Defensive checks
command -v stilts >/dev/null 2>&1 || { echo "[error] stilts not found in PATH" | tee -a "${LOG}"; exit 2; }
[[ -s "${VSX_FITS}" ]] || { echo "[error] VSX FITS not found: ${VSX_FITS}" | tee -a "${LOG}"; exit 2; }

echo "[run] tskymatch2 error=${R_AS} arcsec, chunk=${CHUNK}" | tee -a "${LOG}"
stilts tskymatch2 \
  in1="${CSV}" ra1=ra dec1=dec \
  in2="${VSX_FITS}" ra2=RAdeg dec2=DEdeg \
  error="${R_AS}" join=1and2 find=best out="${OUTCSV}" ofmt=csv >>"${LOG}" 2>&1

# CSV → Parquet (robust ID detection + rowcount logging)
python - "$OUTCSV" "$PARQ_TMP" "$CHUNK" "$R_AS" "$STAMP" "$ID_COL" <<'PY'
import sys, pandas as pd, pyarrow.parquet as pq, pyarrow as pa
csv, outp, chunk, r_as, stamp, id_pref = sys.argv[1:7]

try:
    df = pd.read_csv(csv)
except Exception:
    df = pd.DataFrame()

cands = [id_pref, 'NUMBER', 'number', 'Number', 'NUMBER_1', 'ID', 'Id', 'objID', 'source_id']
id_col = next((c for c in cands if c in df.columns), None)

rows = 0 if df is None or df.empty else len(df)
print(f"[INFO] post-match rows={rows}, searching id in {cands}", flush=True)

if id_col:
    flags = (df[[id_col]].drop_duplicates()
             .rename(columns={id_col: 'NUMBER'})
             .assign(is_known_variable_or_transient=True))
else:
    # Nothing to flag if we cannot map back to the candidate ID used upstream
    flags = pd.DataFrame(columns=['NUMBER','is_known_variable_or_transient'])

flags['source_chunk'] = chunk
flags['query_radius_arcsec'] = float(r_as)
flags['backend'] = 'LOCAL VSX (slim FITS)'
flags['queried_at_utc'] = stamp
flags['id_column_used'] = id_col if id_col else ''

pq.write_table(pa.Table.from_pandas(flags, preserve_index=False), outp)
print(f"[OK] write -> {outp} rows={len(flags)} id_col={id_col}", flush=True)
PY

mv -f "${PARQ_TMP}" "${PARQ}"
echo "[DONE] LOCAL chunk=${CHUNK} -> ${PARQ}" | tee -a "${LOG}"
