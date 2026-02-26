#!/usr/bin/env bash
set -euo pipefail

LOGS_DIR="./logs"
mkdir -p "$LOGS_DIR"

PIDFILE="$LOGS_DIR/prewarm_ps1_neighbourhood.pid"
OUTFILE="$LOGS_DIR/prewarm_ps1_neighbourhood.nohup.out"

cmd_start () {
  if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
    echo "[start] already running: pid=$(cat "$PIDFILE")"
    exit 0
  fi
  rm -f "$LOGS_DIR/PREWARM_PS1_NEIGH_STOP"
  echo "[start] launching background PS1 neighbourhood prewarm"

  nohup python -u scripts/prewarm_ps1_neighbourhood_cache.py \
    --tiles-root ./data/tiles_by_sky \
    --logs-dir "$LOGS_DIR" \
    --workers 4 \
    --radius-arcmin 35 \
    --retry 3 \
    --progress-every 100 \
    > "$OUTFILE" 2>&1 &

  echo $! > "$PIDFILE"
  echo "[start] pid=$(cat "$PIDFILE")"
  echo "[start] tail: tail -f $LOGS_DIR/prewarm_ps1_neighbourhood_cache.log"
}

cmd_stop () {
  echo "[stop] requesting graceful stop"
  touch "$LOGS_DIR/PREWARM_PS1_NEIGH_STOP"
  if [[ -f "$PIDFILE" ]]; then
    echo "[stop] pid=$(cat "$PIDFILE") (will stop after in-flight tasks complete)"
  fi
}

cmd_kill () {
  if [[ -f "$PIDFILE" ]]; then
    pid="$(cat "$PIDFILE")"
    echo "[kill] SIGTERM pid=$pid"
    kill "$pid" || true
  else
    echo "[kill] no pidfile"
  fi
}

cmd_status () {
  echo "[status] pidfile: $PIDFILE"
  if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
    echo "[status] running pid=$(cat "$PIDFILE")"
  else
    echo "[status] not running"
  fi
  echo "---- tail prewarm_ps1_neighbourhood_cache.log ----"
  tail -n 30 "$LOGS_DIR/prewarm_ps1_neighbourhood_cache.log" 2>/dev/null || true
  echo "---- progress json ----"
  cat "$LOGS_DIR/prewarm_ps1_neighbourhood_progress.json" 2>/dev/null || true
}

case "${1:-}" in
  start) cmd_start ;;
  stop) cmd_stop ;;
  kill) cmd_kill ;;
  status) cmd_status ;;
  *) echo "usage: $0 {start|stop|kill|status}" ; exit 2 ;;
 esac
