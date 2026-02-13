#!/usr/bin/env bash
# Local VSX match for one chunk (CSV or VOT) using STILTS tskymatch2 with ra1/dec1, ra2/dec2, error=<arcsec>.
# No TAP; uses a local slim FITS (OID, Name, RAdeg, DEdeg, Type).
# Usage:
#   ./tools/fetchers/fetch_vsx_local_chunked.sh <positions.(csv|vot|xml)> <out_dir> [radius_arcsec] [vsx_fits]
# Example:
#   ./tools/fetchers/fetch_vsx_local_chunked.sh \
#     ./work/scos_chunks/chunk_0000002.vot \
#     ./data/local-cats/_master_optical_parquet_flags/vsx/parts 5 \
#     ./data/local-cats/_external_catalogs/vsx/vsx_master_slim.fits

set -euo pipefail

POS=${1:?positions file required (csv/vot/xml)}
OUTDIR=${2:?output directory required}
R_AS="${3:-5}"
VSX_FITS="${4:-./data/local-cats/_external_catalogs/vsx/vsx_master_slim.fits}"

mkdir -p "${OUTDIR}" "${OUTDIR}/logs"

# Derive chunk name (no extension)
CHUNK="$(basename "$POS")"; CHUNK="${CHUNK%.*}"
STAMP="$(date -u +%FT%TZ)"

# Outputs
OUTCSV="${OUTDIR}/flags_vsx__${CHUNK}.csv"
PARQ_TMP="${OUTDIR}/flags_vsx__${CHUNK}.parquet.tmp"
PARQ="${OUTDIR}/flags_vsx__${CHUNK}.parquet"
LOG="${OUTDIR}/logs/vsx_local__${CHUNK}.log"

# Idempotent: skip if final exists
if [ -s "${PARQ}" ]; then
  echo "[skip] ${CHUNK} already exists: ${PARQ}"
  exit 0
fi

# Normalize input to CSV with NUMBER, ra, dec (degrees)
TMPDIR="$(mktemp -d -t vsxloc_XXXX)"; trap 'rm -rf "$TMPDIR"' EXIT
CSV="${TMPDIR}/in.csv"

case "${POS##*.}" in
  csv|CSV)
    cp -f "${POS}" "${CSV}"
    ;;
  vot|xml|VOT|XML)
    stilts tcopy in="${POS}" ifmt=votable out="${CSV}" ofmt=csv >>"${LOG}" 2>&1
    ;;
  *)
    echo "[error] unsupported input: ${POS}" | tee -a "${LOG}"
    exit 2
    ;;
esac

# Defensive: confirm slim FITS path exists
if [ ! -s "${VSX_FITS}" ]; then
  echo "[error] VSX FITS not found: ${VSX_FITS}" | tee -a "${LOG}"
  exit 2
fi

# LOCAL sky match with tskymatch2 (PROD-friendly syntax).
# IMPORTANT: use ASCII digits only for error (no ” or ″).
echo "[run] tskymatch2 error=${R_AS} arcsec, chunk=${CHUNK}" | tee -a "${LOG}"
stilts tskymatch2 \
  in1="${CSV}" ra1=ra dec1=dec \
  in2="${VSX_FITS}" ra2=RAdeg dec2=DEdeg \
  error="${R_AS}" \
  join=1and2 find=best \
  out="${OUTCSV}" ofmt=csv

# CSV -> Parquet (boolean flag per NUMBER)
python - <<'PY' "${OUTCSV}" "${PARQ_TMP}" "${CHUNK}" "${R_AS}" "${STAMP}"
import sys, pandas as pd, pyarrow.parquet as pq, pyarrow as pa
csv, outp, chunk, r_as, stamp = sys.argv[1:6]
try:
    df = pd.read_csv(csv)
except Exception:
    df = pd.DataFrame(columns=['NUMBER'])
if not df.empty and 'NUMBER' in df.columns:
    df['NUMBER'] = df['NUMBER'].astype(str)
    flags = df[['NUMBER']].drop_duplicates().assign(is_known_variable_or_transient=True)
else:
    flags = pd.DataFrame(columns=['NUMBER','is_known_variable_or_transient'])
flags['source_chunk'] = chunk
flags['query_radius_arcsec'] = float(r_as)
flags['backend'] = 'LOCAL VSX (slim FITS)'
flags['queried_at_utc'] = stamp
pq.write_table(pa.Table.from_pandas(flags, preserve_index=False), outp)
print('[OK] VSX-local flags ->', outp, 'rows=', len(flags))
PY

# Atomic move into place
mv -f "${PARQ_TMP}" "${PARQ}"
echo "[DONE] LOCAL chunk=${CHUNK} -> ${PARQ}" | tee -a "${LOG}"
