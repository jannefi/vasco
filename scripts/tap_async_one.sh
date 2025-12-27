
#!/usr/bin/env bash
set -euo pipefail
# Usage: tap_async_one.sh <positions_chunk_X.csv> <adql.sql>
CHUNK_CSV="$1"
ADQL="$2"

CHUNK_DIR="$(dirname "$CHUNK_CSV")"
CHUNK_BASE="$(basename "$CHUNK_CSV" .csv)"
VOT="$CHUNK_DIR/${CHUNK_BASE}.vot"
RAW="$CHUNK_DIR/${CHUNK_BASE/_chunk_/}_raw.csv"
CLOSEST="$CHUNK_DIR/${CHUNK_BASE/_chunk_/}_closest.csv"

# 1) CSV -> VOTable (row_id as ASCII char; ra/dec double)
python ./scripts/csv_to_votable_positions.py "$CHUNK_CSV" "$VOT"

# 2) Submit async TAP
ADQL_ONE=$(tr '\n' ' ' < "$ADQL" | tr -s ' ')
HDR="$(mktemp)"
for attempt in 1 2 3; do
  curl -s -i --http1.1 -H 'Expect:' \
    -F "QUERY=$ADQL_ONE" \
    -F "FORMAT=CSV" \
    -F "UPLOAD=my_positions,param:my_positions" \
    -F "my_positions=@$VOT;type=application/x-votable+xml" \
    "https://irsa.ipac.caltech.edu/TAP/async" > "$HDR"
  JOBURL=$(grep -i '^Location:' "$HDR" | awk '{print $2}' | tr -d '\r')
  [ -n "$JOBURL" ] && break
  echo "[WARN] Async submit failed (attempt $attempt) for $CHUNK_CSV; retrying…"
  sleep 3
done

if [ -z "${JOBURL:-}" ]; then
  echo "[ERROR] Async submit failed for $CHUNK_CSV"
  sed -n '1,80p' "$HDR"
  exit 1
fi
echo "[INFO] $CHUNK_BASE  -> $JOBURL"

# 3) Poll and fetch
while true; do
  PHASE=$(curl -s "$JOBURL/phase")
  [ "$PHASE" = "COMPLETED" ] && break
  if [ "$PHASE" = "ERROR" ]; then
    echo "[ERROR] TAP error for $CHUNK_CSV:"
    curl -s "$JOBURL/error"
    exit 1
  fi
  sleep 2
done

curl -s -o "$RAW" "$JOBURL/results/result"
echo "[OK] RAW -> $RAW"

# 4) Keep closest per row_id
python ./scripts/closest_per_row_id.py "$RAW" "$CLOSEST"
echo "[OK] CLOSEST -> $CLOSEST"


# 5) QC summary
python ./scripts/qc_chunk_summary.py "$CLOSEST" || true
echo "[INFO] Summary for $CLOSEST completed"


