
#!/usr/bin/env bash
# tap_async_batch.sh (hardened: reconcile, jobs-limited, respects .STOP)
set -euo pipefail
[[ "${DEBUG_TRACE:-0}" == "1" ]] && set -x

ADQL="${1:?ADQL sql path required}"
GLOB="${2:?chunk glob required}"   # e.g., ./work/chunks/*.csv
PAR="${3:-8}"                      # parallel jobs
VERBOSE_CONSOLE="${VERBOSE_CONSOLE:-0}"

[[ -f "$ADQL" ]] || { echo "[ERROR] ADQL not found: $ADQL"; exit 1; }
[[ -f ./scripts/tap_async_one.sh ]] || { echo "[ERROR] Missing ./scripts/tap_async_one.sh"; exit 1; }

dir="$(dirname "$GLOB")"; pat="$(basename "$GLOB")"
matches="$(find "$dir" -type f -name "$pat" | wc -l | tr -d '[:space:]')"
echo "[INFO] Batch: pattern='$GLOB' matches=${matches} parallel=${PAR}"
(( matches > 0 )) || { echo "[ERROR] No files match: $GLOB"; exit 2; }

mkdir -p ./logs/post15 ./logs/post15/heartbeats
HB_DIR="./logs/post15/heartbeats"

reconcile() {
  shopt -s nullglob
  local removed=0 kept=0 name chunk d base closest
  for hb in "$HB_DIR"/*.running; do
    name="$(basename "$hb" .running)"
    chunk="$(find "$dir" -type f -name "$name" -print -quit)"
    if [[ -z "$chunk" ]]; then rm -f -- "$hb"; ((removed++)); continue; fi
    d="$(dirname "$chunk")"; base="${name%.csv}"
    closest="$d/${base/_chunk_/}_closest.csv"
    if [[ -s "$closest" ]]; then rm -f -- "$hb"; ((removed++)); else ((kept++)); fi
  done
  echo "[RECONCILE] removed=$removed kept=$kept"
}

# Pre-flight reconcile
reconcile

# Build queue (skip chunks already completed)
need=()
while IFS= read -r -d '' f; do
  base="$(basename "$f" .csv)"; d="$(dirname "$f")"
  closest="$d/${base/_chunk_/}_closest.csv"
  [[ -s "$closest" ]] && { echo "[SKIP] $f -> existing closest"; continue; }
  need+=("$f")
done < <(find "$dir" -type f -name "$pat" -print0)
echo "[INFO] To process: ${#need[@]} chunk(s)"
(( ${#need[@]} > 0 )) || { echo "[OK] Nothing to do"; exit 0; }

active() { jobs -rp | wc -l | tr -d '[:space:]'; }

# Optional circuit breaker thresholds
MAX_CONSEC_FAIL="${MAX_CONSEC_FAIL:-5}"
consec_fail=0

run_one() {
  local chunk="$1" adql="$2"
  local ts name out err hb
  ts="$(date +%s)"
  name="$(basename "$chunk")"
  out="./logs/post15/${name}.out.${ts}.log"
  err="./logs/post15/${name}.err.${ts}.log"
  hb="./logs/post15/heartbeats/${name}.running"
  : >"$out"; : >"$err"; : >"$hb"

  if [[ "$VERBOSE_CONSOLE" == "1" ]]; then
    bash ./scripts/tap_async_one.sh "$chunk" "$adql" | tee -a "$out" 2>>"$err"
  else
    if bash ./scripts/tap_async_one.sh "$chunk" "$adql" >>"$out" 2>>"$err"; then
      echo "[OK] $name" >>"$out"
      return 0
    else
      echo "[FAIL] $name" >>"$err"
      return 1
    fi
  fi
}

for chunk in "${need[@]}"; do
  [[ -f ./logs/post15/.STOP ]] && { echo "[STOP] Flag detected"; break; }
  while [[ "$(active)" -ge "$PAR" ]]; do sleep 1; done
  run_one "$chunk" "$ADQL" &
  # Lightweight failure tracking (best-effort)
  # (A more elaborate version could wait -n and inspect rc; here we reconcile later.)
done

wait || true
reconcile
echo "[OK] Batch completed (submitted); check logs for per-chunk status"
