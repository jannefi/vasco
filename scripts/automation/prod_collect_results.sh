#!/usr/bin/env bash
# prod_collect_results.sh
# Safe collector for per-CID results from EC2.
set -euo pipefail

export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-west-2}"
BUCKET="${BUCKET:-janne-vasco-usw2}"
FROM_EC2="s3://${BUCKET}/vasco/handshake/from-ec2"

INBOX="./data/local-cats/tmp/positions/aws_inbox"        # per-runout staging
CENTRAL="./data/local-cats/tmp/positions/aws_compare_out" # canonical store
STATE_DONE="./.completed_cids"                            # processed CIDs
SLEEP_SEC="${SLEEP_SEC:-0}"

mkdir -p "${INBOX}" "${CENTRAL}"
touch "${STATE_DONE}"

log(){ printf '%s %s\n' "$(date +'%F %T')" "$*"; }
done_cid(){ grep -qxF "$1" "${STATE_DONE}"; }

discover_runouts() {
  # Find aws-closest-YYYY-MM-DD-HHMM-CID-<CID> folders
  aws s3 ls "${FROM_EC2}/" \
    | sed -n 's/^ *PRE //p' \
    | sed 's:/$::' \
    | grep -E '^aws-closest-[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{4}-CID-[0-9]{5}$' \
    | sort -u
}

extract_cid() {
  # aws-closest-2026-01-30-0955-CID-02104 -> 02104
  local r="$1"; echo "${r##*-CID-}"
}

pull_one_runout() {
  local runout="$1"
  local dest="${INBOX}/${runout}"
  mkdir -p "${dest}"
  log "[PULL] ${runout}"
  aws s3 sync "${FROM_EC2}/${runout}/aws_compare_out/" "${dest}/" --only-show-errors --exact-timestamps
}

merge_one_runout() {
  local runout="$1"
  local cid; cid="$(extract_cid "${runout}")"
  local src="${INBOX}/${runout}/"
  local expected="positions${cid}_closest.csv"

  if [[ ! -f "${src}/${expected}" ]]; then
    log "[WARN] ${expected} missing in ${runout}; skipping"
    return 1
  fi

  log "[MERGE] ${expected} -> ${CENTRAL}/ (append-only)"
  rsync -av --ignore-existing "${src}" "${CENTRAL}/" >/dev/null
  echo "${cid}" >> "${STATE_DONE}"
  log "[DONE] CID ${cid} merged"
}

main() {
  mapfile -t RUNOUTS < <(discover_runouts)
  if [[ "${#RUNOUTS[@]}" -eq 0 ]]; then
    log "[INFO] No runouts found under ${FROM_EC2}/"
    exit 0
  fi

  log "[INFO] Found ${#RUNOUTS[@]} runout(s)."
  for r in "${RUNOUTS[@]}"; do
    cid="$(extract_cid "${r}")"
    if done_cid "${cid}"; then
      log "[SKIP] CID ${cid} already merged"
      continue
    fi
    pull_one_runout "${r}"
    merge_one_runout "${r}" || log "[WARN] Merge issue for ${r}"
    [[ "${SLEEP_SEC}" != "0" ]] && sleep "${SLEEP_SEC}"
  done

  log "[OK] Collection complete. Central: ${CENTRAL}"
}

main "$@"
