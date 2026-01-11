
#!/usr/bin/env bash
# tap_async_batch.sh (portable, talkative, no xargs)
# Usage: tap_async_batch.sh <adql.sql> <chunks_glob> [parallel]
# Optional env:
#   VERBOSE_CONSOLE=1  to mirror chunk output on console via tee
#   DEBUG_TRACE=1      to enable bash -x tracing
#   USE_FIFO=1|0       force FIFO semaphore (default 1); falls back to jobs-limited if FIFO fails

set -euo pipefail
[[ "${DEBUG_TRACE:-0}" == "1" ]] && set -x

ADQL="${1:?ADQL sql path required}"
GLOB="${2:?chunk glob required}"   # e.g., ./data/local-cats/tmp/positions/new/positions_chunk_*.csv
PAR="${3:-8}"
VERBOSE_CONSOLE="${VERBOSE_CONSOLE:-0}"
USE_FIFO_DEFAULT="${USE_FIFO:-1}"

# Validate inputs
[[ -f "$ADQL" ]] || { echo "[ERROR] ADQL not found: $ADQL"; exit 1; }
[[ -f ./scripts/tap_async_one.sh ]] || { echo "[ERROR] Missing ./scripts/tap_async_one.sh"; exit 1; }

# Expand glob dir/pattern
dir="$(dirname "$GLOB")"
pat="$(basename "$GLOB")"

# Info: how many matches exist
matches="$(find "$dir" -type f -name "$pat" | wc -l | tr -d '[:space:]')"
echo "[INFO] Batch: pattern='$GLOB' matches=${matches} parallel=${PAR}"
(( matches > 0 )) || { echo "[ERROR] No files match: $GLOB"; exit 2; }

mkdir -p ./logs/post15 ./logs/post15/heartbeats
echo "[INFO] Logs: ./logs/post15  Heartbeats: ./logs/post15/heartbeats"

# Function to run a single chunk
run_one() {
  local chunk="$1" adql="$2" verbose="$3"
  local ts name out err hb
  ts="$(date +%s)"
  name="$(basename "$chunk")"
  out="./logs/post15/${name}.out.${ts}.log"
  err="./logs/post15/${name}.err.${ts}.log"
  hb="./logs/post15/heartbeats/${name}.running"

  echo "[LAUNCH] ${name}  -> out=$(basename "$out") err=$(basename "$err")"
  : > "$out"; : > "$err"; : > "$hb"

  if [[ "$verbose" == "1" ]]; then
    bash ./scripts/tap_async_one.sh "$chunk" "$adql" \
      > >(tee -a "$out") 2> >(tee -a "$err" >&2)
  else
    bash ./scripts/tap_async_one.sh "$chunk" "$adql" >> "$out" 2>> "$err"
  fi

  rm -f "$hb"
}

# Build 'need' queue by piping find -> while
need_count=0
declare -a PENDING=()
while IFS= read -r -d '' f; do
  base="$(basename "$f" .csv)"
  d="$(dirname "$f")"
  closest="${d}/${base/_chunk_/}_closest.csv"
  if [[ -s "$closest" ]]; then
    echo "[SKIP] $f -> existing $closest"
    continue
  fi
  PENDING+=("$f")
  need_count=$((need_count+1))
done < <(find "$dir" -type f -name "$pat" -print0)

echo "[INFO] To process: ${need_count} chunk(s)"
(( need_count > 0 )) || { echo "[OK] Nothing to do"; exit 0; }

# Try FIFO semaphore first; fall back to jobs-limited if anything fails
use_fifo="$USE_FIFO_DEFAULT"
if [[ "$use_fifo" == "1" ]]; then
  sem="/tmp/vasco.sem.$$"
  if mkfifo "$sem" 2>/dev/null; then
    exec 3<>"$sem" || { echo "[WARN] Could not open FIFO FD; falling back."; use_fifo="0"; }
    rm -f "$sem"
  else
    echo "[WARN] mkfifo failed; falling back to jobs-limited mode."
    use_fifo="0"
  fi
fi

if [[ "$use_fifo" == "1" ]]; then
  # Seed tokens without seq (portable)
  for ((i=1; i<=PAR; i++)); do printf 'x' >&3; done
  for chunk in "${PENDING[@]}"; do
    read -r -u 3 _tok
    { run_one "$chunk" "$ADQL" "$VERBOSE_CONSOLE"; printf 'x' >&3; } &
  done
  wait
  exec 3>&- 3<&- || true
else
  # Jobs-limited fallback: at most PAR concurrent background jobs
  active() { jobs -rp | wc -l | tr -d '[:space:]'; }
  for chunk in "${PENDING[@]}"; do
    while [[ "$(active)" -ge "$PAR" ]]; do sleep 1; done
    { run_one "$chunk" "$ADQL" "$VERBOSE_CONSOLE"; } &
  done
  wait
fi

echo "[OK] All chunks completed successfully"
