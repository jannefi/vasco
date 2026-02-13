#!/usr/bin/env bash
# VSX flags via TAPVizieR (VOT-only), preserving the January-tested flow:
#   VOT -> CSV -> STILTS-built VOT -> tapquery -> CSV -> Parquet
# Enhancements:
#   - Per-chunk log file
#   - Capture TAP async job URL
#   - Auto-abort with `stilts tapresume delete=now` on timeout/retry and on Ctrl-C/SIGTERM
# Usage:
#   ./tools/fetchers/fetch_vsx_stilts_chunked.sh <positions.vot> <out_dir>
set -euo pipefail

POS=${1:?positions.vot required}
OUTDIR=${2:?output directory required}
mkdir -p "$OUTDIR"

CHUNK="$(basename "$POS")"; CHUNK="${CHUNK%.*}"
STAMP="$(date -u +%FT%TZ)"

OUTCSV="$OUTDIR/flags_vsx__${CHUNK}.csv"
PARQ_TMP="$OUTDIR/flags_vsx__${CHUNK}.parquet.tmp"
PARQ="$OUTDIR/flags_vsx__${CHUNK}.parquet"

LOG_DIR="${OUTDIR}/logs"; mkdir -p "${LOG_DIR}"
LOG="${LOG_DIR}/vsx__${CHUNK}.log"

# Idempotent re-run: skip if final parquet exists
if [ -s "$PARQ" ]; then
  echo "[skip] $CHUNK already done: $PARQ"
  exit 0
fi

# TAP settings (can be overridden via env)
TAPURL="${TAPURL:-https://tapvizier.cds.unistra.fr/TAPVizieR/tap}"
TIMEOUT_SECS="${TIMEOUT_SECS:-900}"     # per-attempt cap
RETRIES="${RETRIES:-4}"                  # total attempts incl. first
BASE_BACKOFF="${BASE_BACKOFF:-3}"        # 3, 9, 27, 81...

# Scratch + job tracking
TMPDIR="$(mktemp -d -t vsx_XXXX)"; trap 'rm -rf "$TMPDIR"' EXIT
CSV="$TMPDIR/upload.csv"; VOT="$TMPDIR/upload.vot"
TAP_OUT="$TMPDIR/tapquery.stdout"        # capture tapquery stdout
JOBURL=""                                 # filled from tapquery output

# Ensure we abort in-flight job on Ctrl-C/TERM
_cleanup() {
  if [ -n "$JOBURL" ]; then
    echo "[abort] cancelling TAP job: $JOBURL" | tee -a "$LOG"
    stilts tapresume delete=now joburl="$JOBURL" >>"$LOG" 2>&1 || true
    JOBURL=""
  fi
}
trap '_cleanup; exit 130' INT TERM

echo "[start] ${CHUNK} at ${STAMP}" | tee -a "$LOG"

# Re-encode exactly like your original worker: VOT -> CSV -> VOT
stilts tcopy in="$POS" ifmt=votable out="$CSV" ofmt=csv            >>"$LOG" 2>&1
stilts tcopy in="$CSV" ifmt=csv     out="$VOT" ofmt=votable         >>"$LOG" 2>&1

# --- ADQL (unchanged) ---
ADQL="SELECT u.NUMBER, COUNT(*) AS nmatch
FROM TAP_UPLOAD.t1 AS u
JOIN \"B/vsx/vsx\" AS v
  ON 1 = CONTAINS(
       POINT('ICRS', v.RAJ2000, v.DEJ2000),
       CIRCLE('ICRS', u.ra, u.dec, 5.0/3600.0)
     )
GROUP BY u.NUMBER"

attempt=0
set +e
while :; do
  attempt=$((attempt+1))
  echo "[tap] attempt=${attempt} timeout=${TIMEOUT_SECS}s url=${TAPURL}" | tee -a "$LOG"
  # Run with timeout, capturing stdout to parse SUBMITTED/EXECUTING/COMPLETED lines
  : > "$TAP_OUT"
  if timeout -s INT "${TIMEOUT_SECS}" \
     stilts tapquery \
       tapurl="${TAPURL}" \
       nupload=1 upload1="$VOT" upname1=t1 ufmt1=votable \
       adql="$ADQL" out="$OUTCSV" ofmt=csv \
       >>"$TAP_OUT" 2>>"$LOG"; then
    rc=0
  else
    rc=$?
  fi

  # Extract async job URL if present (first URL with /async/)
  if [ -z "$JOBURL" ]; then
    JOBURL="$(awk '/https?:\/\/[^ ]*\/async\/[0-9]+/ {print $0; exit}' "$TAP_OUT" | tr -d '\r')"
    [ -n "$JOBURL" ] && echo "[tap] joburl=${JOBURL}" | tee -a "$LOG"
  fi

  # Mirror tapquery stdout to the log for visibility
  cat "$TAP_OUT" >>"$LOG"

  if [ $rc -eq 0 ]; then
    # STILTS usually deletes the job on success; clear our pointer
    JOBURL=""
    break
  fi

  # On failure/timeout: abort the server job before retrying
  if [ -n "$JOBURL" ]; then
    echo "[retry] aborting in-flight job before backoff: $JOBURL" | tee -a "$LOG"
    stilts tapresume delete=now joburl="$JOBURL" >>"$LOG" 2>&1 || true
    JOBURL=""
  fi

  if [ $attempt -ge $RETRIES ]; then
    echo "[error] tapquery failed after ${RETRIES} attempts (rc=${rc})" | tee -a "$LOG"
    exit 3
  fi

  sleep_for=$(( BASE_BACKOFF ** attempt ))
  echo "[warn] retrying in ${sleep_for}s" | tee -a "$LOG"
  sleep "${sleep_for}"
done
set -e

# CSV -> Parquet (unchanged)
python - <<'PY' "$OUTCSV" "$PARQ_TMP" "$CHUNK" "$STAMP"
import sys, pandas as pd, pyarrow as pa, pyarrow.parquet as pq
csv, parq_tmp, chunk, stamp = sys.argv[1:5]
try:
    df = pd.read_csv(csv)
except Exception:
    df = pd.DataFrame(columns=['NUMBER','nmatch'])

if not df.empty and 'NUMBER' in df.columns:
    df['NUMBER'] = df['NUMBER'].astype(str)
    flags = (df[['NUMBER']]
             .drop_duplicates()
             .assign(is_known_variable_or_transient=True))
else:
    flags = pd.DataFrame(columns=['NUMBER','is_known_variable_or_transient'])

flags['source_chunk'] = chunk
flags['queried_at_utc'] = stamp
pq.write_table(pa.Table.from_pandas(flags, preserve_index=False), parq_tmp)
print('[OK] VSX flags ->', parq_tmp, 'rows=', len(flags))
PY

mv -f "$PARQ_TMP" "$PARQ"
echo "[DONE] chunk=${CHUNK} -> ${PARQ}" | tee -a "$LOG"
