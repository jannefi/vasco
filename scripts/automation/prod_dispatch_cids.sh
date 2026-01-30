#!/usr/bin/env bash
# prod_dispatch_cids.sh
# Dispatch per-CID jobs to S3 for EC2 to process, skipping CIDs that already
# have a TAP closest file in ./data/local-cats/tmp/positions/new/.
set -euo pipefail

export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-west-2}"
BUCKET="${BUCKET:-janne-vasco-usw2}"
FROM_PROD="s3://${BUCKET}/vasco/handshake/from-prod"

NEW_DIR="./data/local-cats/tmp/positions/new"          # both chunk CSVs and TAP closest CSVs live here
SEEDS_BASE="./data/local-cats/optical_seeds"           # local staging for seed parquet (per CID, ephemeral)
STATE_SENT="./.sent_cids"                               # tracks dispatched CIDs so we can resume
SLEEP_SEC="${SLEEP_SEC:-0}"                             # optional pacing between CIDs

mkdir -p "${SEEDS_BASE}"
touch "${STATE_SENT}"

log(){ printf '%s %s\n' "$(date +'%F %T')" "$*"; }
sent(){ grep -qxF "$1" "${STATE_SENT}"; }

list_cids_needing_aws() {
  # Discover CIDs from flat TAP CSVs: positions_chunk_02104.csv -> 02104
  # Exclude those that already have TAP closest in the SAME folder: positions02104_closest.csv
  # Output: one CID per line (5 digits)
  local cid chunk_path closest_path
  while IFS= read -r chunk_path; do
    cid="$(sed -E 's/.*positions_chunk_([0-9]{5})\.csv/\1/' <<<"$chunk_path")"
    closest_path="${NEW_DIR}/positions${cid}_closest.csv"
    if [[ -f "${closest_path}" ]]; then
      # Already handled by NASA IRSA TAP -> skip
      continue
    fi
    printf '%s\n' "${cid}"
  done < <(find "${NEW_DIR}" -maxdepth 1 -type f -name 'positions_chunk_[0-9][0-9][0-9][0-9][0-9].csv' | sort)
}

dispatch_one() {
  local CID="$1"
  local runseeds="aws-seeds-$(date +%F)-CID-${CID}"
  local local_chunk_dir="${SEEDS_BASE}/chunk_${CID}"
  local s3_dest="${FROM_PROD}/${runseeds}/optical_seeds/chunk_${CID}"

  log "[CID ${CID}] building seeds from ${NEW_DIR}/positions_chunk_${CID}.csv"
  mkdir -p "${local_chunk_dir}"
  python scripts/make_optical_seed_from_TAPchunk.py \
    --tap-chunk-csv "${NEW_DIR}/positions_chunk_${CID}.csv" \
    --chunk-id "${CID}" \
    --out-dir "${local_chunk_dir}"

  log "[CID ${CID}] pushing seeds -> ${s3_dest}"
  aws s3 sync "${local_chunk_dir}" "${s3_dest}" --only-show-errors --exact-timestamps

  # Publish a tiny cid.txt to simplify EC2 watcher and auditing
  printf '%s\n' "${CID}" | aws s3 cp - "${FROM_PROD}/${runseeds}/cid.txt" --only-show-errors

  echo "${CID}" >> "${STATE_SENT}"
  log "[CID ${CID}] dispatched as ${runseeds}"
}

main() {
  mapfile -t CIDS < <(list_cids_needing_aws)
  if [[ "${#CIDS[@]}" -eq 0 ]]; then
    log "[INFO] No CIDs require AWS processing (all have TAP closest)."
    exit 0
  fi

  log "[INFO] CIDs needing AWS: ${#CIDS[@]}"
  for CID in "${CIDS[@]}"; do
    if sent "${CID}"; then
      log "[SKIP] ${CID} already dispatched earlier"
      continue
    fi
    dispatch_one "${CID}"
    [[ "${SLEEP_SEC}" != "0" ]] && sleep "${SLEEP_SEC}"
  done

  log "[OK] Dispatch complete."
}

main "$@"
