#!/usr/bin/env bash
# scripts/common/stop_flag.sh
# Graceful stop flag helper for long-running watchers.
# Exit code 43 == manual/flagged stop; keep this consistent across scripts.

: "${STOP_FILE:=.STOP}"   # default relative to watcher working dir

stop_if_requested() {
  if [[ -f "${STOP_FILE}" ]]; then
    ts="$(date '+%F %T')"
    echo "${ts} [STOP] Stop flag detected at: ${STOP_FILE}" >&2
    # Optional: leave a breadcrumb
    echo "${ts} MANUAL_STOP via ${STOP_FILE}" > ./STOPPED
    exit 43
  fi
}

# (Optional) Treat TERM/INT like a manual stop (clean, same exit code)
graceful_exit() {
  ts="$(date '+%F %T')"
  echo "${ts} [STOP] Caught signal; exiting gracefully (code 43)" >&2
  exit 43
}

# Export functions if this file is sourced in a subshell-heavy script
export -f stop_if_requested graceful_exit || true
