
#!/usr/bin/env bash
# Show state per chunk for a directory pattern.
# Usage:
#   ./scripts/status_chunks.sh "./data/local-cats/tmp/positions/new/positions_chunk_*.csv"
set -euo pipefail
PATTERN="${1:?glob required}"
TAP_BASE="${TAP_BASE:-https://irsa.ipac.caltech.edu/TAP}"

printf "%-36s  %-12s  %s\n" "chunk" "state" "detail"
for f in $PATTERN; do
  base="$(basename "$f" .csv)"
  dir="$(dirname "$f")"
  closest="$dir/${base/_chunk_/}_closest.csv"
  raw="$dir/${base/_chunk_/}_raw.csv"
  meta="$dir/${base/_chunk_/}_tap.meta.json"
  hb="logs/post15/heartbeats/${base}.csv.running"

  if [[ -s "$closest" ]]; then
    printf "%-36s  %-12s  %s\n" "$base" "DONE" "$closest"
    continue
  fi
  if [[ -f "$hb" ]]; then
    printf "%-36s  %-12s  %s\n" "$base" "RUNNING" "heartbeat=$(tail -n1 "$hb" 2>/dev/null)"
    continue
  fi
  if [[ -s "$raw" ]]; then
    printf "%-36s  %-12s  %s\n" "$base" "RAW" "$raw (postprocess missing?)"
    continue
  fi
  if [[ -s "$meta" ]]; then
    joburl="$(python - <<'PY' "$meta"
import sys, json
try: print(json.load(open(sys.argv[1])).get("job_url",""))
except: pass
PY
)"
    if [[ -n "$joburl" ]]; then
      phase="$(curl -s "${joburl}/phase" || true)"
      printf "%-36s  %-12s  %s\n" "$base" "$phase" "$joburl"
      continue
    fi
  fi
  printf "%-36s  %-12s  %s\n" "$base" "PENDING" "$f"
done

