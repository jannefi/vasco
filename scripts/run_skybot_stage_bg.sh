#!/usr/bin/env bash
# run_skybot_stage_bg.sh
# Background-friendly wrapper for stage_skybot_post.py.
# Usage:
#   RUN=./work/runs/<run-id> STAGE=S6 INPUT='stages/stage_S5_VSX.csv' bash tools/run_skybot_stage_bg.sh start
#   bash tools/run_skybot_stage_bg.sh status
#   bash tools/run_skybot_stage_bg.sh stop
#   bash tools/run_skybot_stage_bg.sh kill
set -euo pipefail

RUN=${RUN:-}
STAGE=${STAGE:-S1}
INPUT=${INPUT:-stages/stage_S0_PS1SH.csv}
LOGDIR=${LOGDIR:-./logs}
PY=${PY:-python}

mkdir -p "$LOGDIR"
PIDFILE="$LOGDIR/skybot_stage_${STAGE}.pid"
OUTFILE="$LOGDIR/skybot_stage_${STAGE}.nohup.out"
STOPFILE="$LOGDIR/SKYBOT_STAGE_${STAGE}.STOP"

cmd_start() {
  if [[ -z "$RUN" ]]; then
    echo "[start] RUN env is required (e.g., RUN=./work/runs/run-S1-...)" >&2
    exit 2
  fi
  if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
    echo "[start] already running: pid=$(cat "$PIDFILE")"
    exit 0
  fi
  rm -f "$STOPFILE"
  echo "[start] launching SkyBoT stage in background" 
  nohup "$PY" -u scripts/stage_skybot_post.py \
    --run-dir "$RUN" \
    --input-glob "$INPUT" \
    --stage "$STAGE" \
    > "$OUTFILE" 2>&1 &
  echo $! > "$PIDFILE"
  echo "[start] pid=$(cat "$PIDFILE")"
  echo "[start] tail: tail -f $OUTFILE"
}

cmd_stop() {
  echo "[stop] requesting graceful stop (note: current stage script will stop at next safe point only if it checks STOP file)"
  touch "$STOPFILE"
  if [[ -f "$PIDFILE" ]]; then
    echo "[stop] pid=$(cat "$PIDFILE")"
  fi
}

cmd_kill() {
  if [[ -f "$PIDFILE" ]]; then
    pid="$(cat "$PIDFILE")"
    echo "[kill] SIGTERM pid=$pid"
    kill "$pid" || true
  else
    echo "[kill] no pidfile"
  fi
}

cmd_status() {
  echo "[status] pidfile: $PIDFILE"
  if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
    echo "[status] running pid=$(cat "$PIDFILE")"
  else
    echo "[status] not running"
  fi
  echo "---- tail nohup ----"
  tail -n 40 "$OUTFILE" 2>/dev/null || true
}

case "${1:-}" in
  start) cmd_start ;;
  stop) cmd_stop ;;
  kill) cmd_kill ;;
  status) cmd_status ;;
  *) echo "usage: $0 {start|stop|kill|status}" ; exit 2 ;;
esac
