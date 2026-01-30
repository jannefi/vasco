#!/usr/bin/env bash
# ec2_watch_and_process.sh
# Stateless EC2 watcher for per-CID jobs. Safe for nohup/screen.
set -Eeuo pipefail
shopt -s nullglob

export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-west-2}"
BUCKET="${BUCKET:-janne-vasco-usw2}"
FROM_PROD="s3://${BUCKET}/vasco/handshake/from-prod"
FROM_EC2="s3://${BUCKET}/vasco/handshake/from-ec2"

WORK_ROOT="./work"                                   # ephemeral workspace
STATE_DONE="./.processed_cids"                       # processed CID ledger
SLEEP_SEC="${SLEEP_SEC:-30}"                         # poll interval
WORKERS="${WORKERS:-16}"                             # sidecar parallelism

mkdir -p "${WORK_ROOT}"
touch "${STATE_DONE}"

log(){ printf '%s %s\n' "$(date +'%F %T')" "$*"; }
err(){ printf '%s [ERR] %s\n' "$(date +'%F %T')" "$*" >&2; }
trap 'err "Unhandled error at line $LINENO (exit=$?) - continuing watcher";' ERR

cid_done(){ grep -qxF "$1" "${STATE_DONE}"; }

discover_jobs() {
  # List aws-seeds-YYYY-MM-DD-CID-<CID> job folders
  aws s3 ls "${FROM_PROD}/" \
    | sed -n 's/^ *PRE //p' \
    | sed 's:/$::' \
    | grep -E '^aws-seeds-[0-9]{4}-[0-9]{2}-[0-9]{2}-CID-[0-9]{5}$' \
    | sort -u
}

extract_cid() {
  # aws-seeds-2026-01-30-CID-02104 -> 02104
  local job="$1"
  echo "${job##*-CID-}"
}

process_one_job() {
  local job="$1"
  local CID; CID="$(extract_cid "${job}")"

  if cid_done "${CID}"; then
    log "[SKIP] CID ${CID} already processed"
    return 0
  fi

  log "=========="
  log "[START] CID ${CID} (${job})"
  log "=========="

  # Fresh workspace
  local CUR="${WORK_ROOT}/current"
  local SEEDS="${CUR}/seeds"
  local SIDE="${CUR}/sidecar"
  local OUTD="${CUR}/out"
  mkdir -p "${SEEDS}/chunk_${CID}" "${SIDE}" "${OUTD}"

  # Pull only this CID's seeds (mirror workspace allowed to --delete safely)
  log "[PULL] seeds for CID ${CID}"
  aws s3 sync "${FROM_PROD}/${job}/optical_seeds" "${SEEDS}" --only-show-errors --delete

  # Sidecar (tiny ALL.parquet scoped to this CID)
  log "[SIDECAR] CID ${CID} workers=${WORKERS}"
  python scripts/neowise_s3_sidecar.py \
    --optical-root "${SEEDS}" \
    --out-root    "${SIDE}" \
    --radius-arcsec 5.0 \
    --parallel pixel --workers "${WORKERS}"

  # Formatter (single output CSV)
  log "[FORMAT] CID ${CID}"
  python scripts/sidecar_to_closest_chunks.py \
    --sidecar-all "${SIDE}/neowise_se_flags_ALL.parquet" \
    --optical-root "${SEEDS}/chunk_${CID}" \
    --out-dir "${OUTD}" \
    --row-id-float

  # Push result
  local RUNOUT="aws-closest-$(date +%F-%H%M)-CID-${CID}"
  log "[PUSH] ${RUNOUT}"
  aws s3 sync "${OUTD}" "${FROM_EC2}/${RUNOUT}/aws_compare_out/" --only-show-errors --exact-timestamps

  # Mark done + clean workspace
  echo "${CID}" >> "${STATE_DONE}"
  rm -rf "${CUR}"

  log "[DONE] CID ${CID} -> ${RUNOUT}"
}

main_loop() {
  # Verify auth once
  aws sts get-caller-identity >/dev/null 2>&1 || { err "AWS auth failed"; exit 1; }

  while true; do
    mapfile -t JOBS < <(discover_jobs)
    if [[ "${#JOBS[@]}" -eq 0 ]]; then
      log "[IDLE] no jobs; sleeping ${SLEEP_SEC}s"
      sleep "${SLEEP_SEC}"
      continue
    fi

    for job in "${JOBS[@]}"; do
      process_one_job "${job}" || err "Processing failed for ${job}"
    done

    log "[SLEEP] ${SLEEP_SEC}s"
    sleep "${SLEEP_SEC}"
  done
}

main_loop "$@"
