
#!/usr/bin/env bash
# tap_async_one.sh (hardened: keep-alive heartbeat + cleanup trap)
set -euo pipefail

CHUNK_CSV="$1"
ADQL="$2"
CHUNK_DIR="$(dirname "$CHUNK_CSV")"
CHUNK_BASE="$(basename "$CHUNK_CSV" .csv)"
VOT="$CHUNK_DIR/$CHUNK_BASE.vot"
RAW="$CHUNK_DIR/${CHUNK_BASE/_chunk_/}_raw.csv"
CLOSEST="$CHUNK_DIR/${CHUNK_BASE/_chunk_/}_closest.csv"
META="$CHUNK_DIR/${CHUNK_BASE/_chunk_/}_tap.meta.json"

# Heartbeat & logs (note: .running name is stable, no timestamp)
HB_DIR="./logs/post15/heartbeats"; mkdir -p "$HB_DIR"
HB="$HB_DIR/$(basename "$CHUNK_CSV").running"
OUT="./logs/post15/$(basename "$CHUNK_CSV").out.$(date +%s).log"
ERR="./logs/post15/$(basename "$CHUNK_CSV").err.$(date +%s).log"

# Ensure heartbeat removed on any exit
cleanup() { rm -f "$HB"; }
trap cleanup EXIT INT TERM HUP

: >"$OUT"; : >"$ERR"; : >"$HB"

# 1) CSV -> VOTable
python ./scripts/csv_to_votable_positions.py "$CHUNK_CSV" "$VOT" >>"$OUT" 2>>"$ERR"

# 2) Single-line ADQL + submit async TAP
ADQL_ONE="$(tr ' \r\n' ' ' < "$ADQL" | tr -s ' ')"
HDR="$(mktemp)"; JOBURL=""
if [[ -s "$META" ]]; then
  JOBURL="$(python - "$META" <<'PY'
import json,sys
try: print(json.load(open(sys.argv[1],'r')).get('job_url',''))
except: pass
PY
)"
fi

if [[ -z "${JOBURL:-}" ]]; then
  for attempt in 1 2 3; do
    curl -s -i --http1.1 -H 'Expect:' \
      -F "QUERY=$ADQL_ONE" \
      -F "FORMAT=CSV" \
      -F "UPLOAD=my_positions,param:my_positions" \
      -F "my_positions=@$VOT;type=application/x-votable+xml" \
      "https://irsa.ipac.caltech.edu/TAP/async" >"$HDR" && break || true
    echo "[WARN] Async submit failed (attempt $attempt) for $CHUNK_CSV" >>"$ERR"
    sleep 3
  done
  JOBURL="$(grep -i '^Location:' "$HDR" | awk '{print $2}' | tr -d ' \r\n')"
  [[ -n "$JOBURL" ]] || { echo "[ERROR] submit failed" >>"$ERR"; exit 1; }
  python - "$META" "$JOBURL" <<'PY'
import json,sys,time
json.dump({"job_url":sys.argv[2],"created_at":time.time()}, open(sys.argv[1],"w"))
PY
fi
echo "[INFO] $(basename "$CHUNK_CSV") -> $JOBURL" >>"$OUT"

# 3) Poll & fetch (heartbeat keep-alive)
while true; do
  PHASE="$(curl -s "$JOBURL/phase" || true)"
  date +%s >"$HB"            # <- refresh heartbeat every iteration
  [[ "$PHASE" == "COMPLETED" ]] && break
  if [[ "$PHASE" == "ERROR" ]]; then
    echo "[ERROR] TAP error:" >>"$ERR"; curl -s "$JOBURL/error" >>"$ERR" || true
    exit 1
  fi
  sleep 2
done

curl -s -L -f -o "$RAW" "$JOBURL/results/result" || { echo "[ERROR] download failed" >>"$ERR"; exit 1; }
[[ -s "$RAW" ]] || { echo "[ERROR] empty RAW" >>"$ERR"; exit 1; }
echo "[OK] RAW -> $RAW" >>"$OUT"

# 4) Closest per row_id
python ./scripts/closest_per_row_id.py "$RAW" "$CLOSEST" >>"$OUT" 2>>"$ERR"
[[ -s "$CLOSEST" ]] || { echo "[ERROR] closest missing" >>"$ERR"; exit 1; }
echo "[OK] CLOSEST -> $CLOSEST" >>"$OUT"

# 5) Best-effort QC
python ./scripts/qc_chunk_summary.py "$CLOSEST" > "${CLOSEST%.csv}.qc.txt" 2>&1 || true
echo "[INFO] Summary complete for $CLOSEST" >>"$OUT"
