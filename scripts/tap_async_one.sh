
#!/usr/bin/env bash
# tap_async_one.sh (fixed ADQL normalization)
set -euo pipefail
# Usage: tap_async_one.sh <positions_chunk_X.csv> <adql.sql>

CHUNK_CSV="$1"
ADQL="$2"
CHUNK_DIR="$(dirname "$CHUNK_CSV")"
CHUNK_BASE="$(basename "$CHUNK_CSV" .csv)"
VOT="$CHUNK_DIR/$CHUNK_BASE.vot"
RAW="$CHUNK_DIR/${CHUNK_BASE/_chunk_/}_raw.csv"
CLOSEST="$CHUNK_DIR/${CHUNK_BASE/_chunk_/}_closest.csv"
META="$CHUNK_DIR/${CHUNK_BASE/_chunk_/}_tap.meta.json"

# Skip if we already have a good CLOSEST
if [[ -s "$CLOSEST" ]]; then
  echo "[SKIP] $CHUNK_BASE -> $CLOSEST present"
  exit 0
fi

# 1) CSV -> VOTable
python ./scripts/csv_to_votable_positions.py "$CHUNK_CSV" "$VOT"

# 2) Prepare ADQL (single line) & submit async TAP
ADQL_ONE="$(tr ' \r\n' ' ' < "$ADQL" | tr -s ' ')"

HDR="$(mktemp)"
JOBURL=""
if [[ -s "$META" ]]; then
  JOBURL="$(python - "$META" <<'PY'
import json, sys
try:
  d = json.load(open(sys.argv[1],'r'))
  print(d.get('job_url',''))
except Exception:
  pass
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
      "https://irsa.ipac.caltech.edu/TAP/async" > "$HDR"
    JOBURL="$(grep -i '^Location:' "$HDR" | awk '{print $2}' | tr -d ' \r\n')"
    [[ -n "$JOBURL" ]] && break
    echo "[WARN] Async submit failed (attempt $attempt) for $CHUNK_CSV; retrying…"
    sleep 3
  done
  if [[ -z "${JOBURL:-}" ]]; then
    echo "[ERROR] Async submit failed for $CHUNK_CSV"
    sed -n '1,80p' "$HDR"
    exit 1
  fi
  python - "$META" "$JOBURL" <<'PY'
import json,sys,time
meta={"job_url":sys.argv[2],"created_at":time.time()}
json.dump(meta, open(sys.argv[1],"w"))
PY
fi

echo "[INFO] $CHUNK_BASE -> $JOBURL"

# 3) Poll and fetch
while true; do
  PHASE="$(curl -s "$JOBURL/phase")"
  [[ "$PHASE" = "COMPLETED" ]] && break
  if [[ "$PHASE" = "ERROR" ]]; then
    echo "[ERROR] TAP error for $CHUNK_CSV:"
    curl -s "$JOBURL/error" || true
    exit 1
  fi
  sleep 2
done


curl -s -L -f -o "$RAW" "$JOBURL/results/result" \
  || { echo "[ERROR] Download failed (HTTP) for $CHUNK_BASE"; exit 1; }

[[ -s "$RAW" ]] || { echo "[ERROR] Empty RAW for $CHUNK_BASE"; exit 1; }
echo "[OK] RAW -> $RAW"

# 4) Keep closest per row_id
python ./scripts/closest_per_row_id.py "$RAW" "$CLOSEST"
[[ -s "$CLOSEST" ]] || { echo "[ERROR] Failed to write $CLOSEST"; exit 1; }
echo "[OK] CLOSEST -> $CLOSEST"

# 5) QC summary (best-effort)
python ./scripts/qc_chunk_summary.py "$CLOSEST" > "${CLOSEST%.csv}.qc.txt" 2>&1 || true
echo "[INFO] Summary for $CLOSEST completed"
