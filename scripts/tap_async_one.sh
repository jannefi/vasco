
#!/usr/bin/env bash
# tap_async_one.sh (hardened: ABORT handling, async retries, /sync fallback, circuit-friendly)
set -euo pipefail

CHUNK_CSV="$1"        # input chunk CSV
ADQL="$2"             # ADQL file path (can be multi-line; we canonicalize to single line)

CHUNK_DIR="$(dirname "$CHUNK_CSV")"
CHUNK_BASE="$(basename "$CHUNK_CSV" .csv)"
VOT="$CHUNK_DIR/$CHUNK_BASE.vot"
RAW="$CHUNK_DIR/${CHUNK_BASE/_chunk_/}_raw.csv"
CLOSEST="$CHUNK_DIR/${CHUNK_BASE/_chunk_/}_closest.csv"
META="$CHUNK_DIR/${CHUNK_BASE/_chunk_/}_tap.meta.json"

HB_DIR="./logs/post15/heartbeats"; mkdir -p "$HB_DIR"
HB="$HB_DIR/$(basename "$CHUNK_CSV").running"
OUT="./logs/post15/$(basename "$CHUNK_CSV").out.$(date +%s).log"
ERR="./logs/post15/$(basename "$CHUNK_CSV").err.$(date +%s).log"
: >"$OUT"; : >"$ERR"; : >"$HB"

# Tunables (override via env)
MAX_ASYNC_RETRIES="${MAX_ASYNC_RETRIES:-3}"
BACKOFF_BASE="${BACKOFF_BASE:-10}"           # seconds
BACKOFF_CAP="${BACKOFF_CAP:-180}"            # seconds
MAX_EXEC_SECS="${MAX_EXEC_SECS:-3600}"       # 1 hour wall per async job
SYNC_ON_FAIL="${SYNC_ON_FAIL:-1}"            # 1 = try /sync after async retries
JITTER_MAX="${JITTER_MAX:-1}"                # 1 second jitter
TAP_BASE="${TAP_BASE:-https://irsa.ipac.caltech.edu/TAP}"

cleanup() { rm -f "$HB"; }
trap cleanup EXIT INT TERM HUP

log()   { echo "[$(date +%T)] $*"        | tee -a "$OUT"; }
elog()  { echo "[$(date +%T)] $*" >&2    | tee -a "$ERR" >&2; }
phase() { curl -s "${1}/phase" || true;  }

canonical_adql() {
  tr ' \r\n' ' ' < "$ADQL" | tr -s ' '
}

submit_async() {
  local hdr joburl adql_one
  adql_one="$(canonical_adql)"
  hdr="$(mktemp)"
  for attempt in 1 2 3; do
    curl -s -i --http1.1 -H 'Expect:' \
      -F "QUERY=${adql_one}" \
      -F "FORMAT=CSV" \
      -F "UPLOAD=my_positions,param:my_positions" \
      -F "my_positions=@${VOT};type=application/x-votable+xml" \
      "${TAP_BASE}/async" >"$hdr" && break || true
    elog "[WARN] async submit failed (attempt $attempt)"
    sleep 3
  done
  joburl="$(grep -i '^Location:' "$hdr" | awk '{print $2}' | tr -d ' \r\n')"
  rm -f "$hdr"
  echo "$joburl"
}

download_result() {
  local joburl="$1"
  curl -s -L -f -o "$RAW" "${joburl}/results/result" \
    || { elog "[ERROR] download failed from $joburl/results/result"; return 1; }
  [[ -s "$RAW" ]] || { elog "[ERROR] empty RAW"; return 1; }
  log "[OK] RAW -> $RAW"
}

poll_async() {
  local joburl="$1" start_ts now_ts ph
  start_ts="$(date +%s)"
  while true; do
    ph="$(phase "$joburl")"
    date +%s >"$HB"
    case "$ph" in
      COMPLETED)
        log "[INFO] phase=COMPLETED"
        return 0
        ;;
      ERROR)
        elog "[ERROR] phase=ERROR"; curl -s "${joburl}/error" >>"$ERR" || true
        return 2
        ;;
      ABORT|ABORTED)
        elog "[ERROR] phase=$ph (server aborted job)"
        curl -s "${joburl}/error" >>"$ERR" || true
        return 3
        ;;
      QUEUED|EXECUTING|"")
        now_ts="$(date +%s)"
        if (( now_ts - start_ts > MAX_EXEC_SECS )); then
          elog "[ERROR] phase=$ph exceeded MAX_EXEC_SECS=$MAX_EXEC_SECS; treating as failure"
          return 4
        fi
        sleep 2
        ;;
      *)
        elog "[WARN] unexpected phase='$ph'; keep polling"
        sleep 2
        ;;
    esac
  done
}

do_sync_once() {
  local adql_one
  adql_one="$(canonical_adql)"
  curl -s -X POST "${TAP_BASE}/sync" \
    -F 'REQUEST=doQuery' \
    -F 'LANG=ADQL' \
    -F 'FORMAT=csv' \
    -F 'UPLOAD=my_positions,param:my_positions' \
    -F "my_positions=@${VOT};type=application/x-votable+xml" \
    -F "QUERY=${adql_one}" \
    -o "$RAW" || return 1

  if grep -q 'QUERY_STATUS" value="ERROR"' "$RAW" 2>/dev/null; then
    elog "[ERROR] /sync returned VOTable ERROR"; head -n 40 "$RAW" | tee -a "$ERR" >&2
    return 1
  fi
  [[ -s "$RAW" ]] || { elog "[ERROR] /sync returned empty body"; return 1; }
  log "[OK] /sync RAW -> $RAW"
}

postprocess_closest() {
  python ./scripts/closest_per_row_id.py "$RAW" "$CLOSEST" >>"$OUT" 2>>"$ERR" \
    || { elog "[ERROR] closest_per_row_id failed"; return 1; }
  [[ -s "$CLOSEST" ]] || { elog "[ERROR] closest missing"; return 1; }
  log "[OK] CLOSEST -> $CLOSEST"
  python ./scripts/qc_chunk_summary.py "$CLOSEST" > "${CLOSEST%.csv}.qc.txt" 2>&1 || true
}

# 0) CSV -> VOTable (use your existing converter to guarantee clean headers)
python ./scripts/csv_to_votable_positions.py "$CHUNK_CSV" "$VOT" >>"$OUT" 2>>"$ERR"

# 1) Async with retries; treat ABORT like ERROR
attempt=0
while true; do
  attempt=$((attempt+1))
  joburl=""
  if [[ -s "$META" ]]; then
    joburl="$(python - "$META" <<'PY'
import json,sys
try:
  print(json.load(open(sys.argv[1],'r')).get('job_url',''))
except: pass
PY
)"
  fi
  if [[ -z "${joburl:-}" ]]; then
    joburl="$(submit_async)" || true
    [[ -n "$joburl" ]] || { elog "[ERROR] submit failed (no Location)"; rc=1; break; }
    python - "$META" "$joburl" <<'PY'
import json,sys,time
json.dump({"job_url":sys.argv[2],"created_at":time.time()}, open(sys.argv[1],"w"))
PY
  fi

  log "[INFO] polling $joburl (attempt $attempt/$MAX_ASYNC_RETRIES)"
  rc=0
  if poll_async "$joburl"; then
    if download_result "$joburl"; then
      break
    else
      rc=1
    fi
  else
    rc=$?   # 2=ERROR, 3=ABORT/ABORTED, 4=timeout
  fi

  # Retry guard
  if (( attempt >= MAX_ASYNC_RETRIES )); then
    elog "[WARN] async attempts exhausted ($attempt)."
    if [[ "$SYNC_ON_FAIL" == "1" ]]; then
      log "[INFO] falling back to /sync for this chunk"
      if do_sync_once; then
        rc=0; break
      else
        rc=1; break
      fi
    else
      break
    fi
  fi

  # Exponential backoff with jitter
  sleep_for=$(( BACKOFF_BASE * (2 ** (attempt-1)) ))
  (( sleep_for > BACKOFF_CAP )) && sleep_for="$BACKOFF_CAP"
  jitter=$(( RANDOM % (JITTER_MAX+1) ))
  wait_s=$(( sleep_for + jitter ))
  log "[INFO] retrying async in ${wait_s}s (rc=$rc)"
  sleep "$wait_s"

  # Re-submit next time (clear job url)
  : > "$META"
done

# Final postprocessing (if we have RAW)
if [[ "${rc:-0}" -eq 0 ]]; then
  postprocess_closest || exit 1
  log "[OK] chunk done"
  exit 0
fi

elog "[ERROR] chunk failed (rc=${rc:-1})"
exit "${rc:-1}"
