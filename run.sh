#!/usr/bin/env bash

set -euo pipefail

# --- Sexagesimal RA/Dec normalization (Bash 3.2 compatible) ---
# This block rewrites $@ so that --ra/--dec values can be sexagesimal
# (e.g. 21:02:52.28, +48:34:18.90) or decimal degrees.
# Requires vasco/utils/coords.py (Astropy Angle) to be importable from $PWD.

export PYTHONPATH="${PYTHONPATH:-}:$(pwd)"

normalize_ra() { python - "$1" <<'PY'
from vasco.utils.coords import parse_ra
import sys
print(f"{parse_ra(sys.argv[1]):.9f}")
PY
}
normalize_dec() { python - "$1" <<'PY'
from vasco.utils.coords import parse_dec
import sys
v = parse_dec(sys.argv[1])
print(f"{v:+.9f}")
PY
}

# Build NEW_ARGS by normalizing --ra/--dec occurrences
NEW_ARGS=()
while [ "$#" -gt 0 ]; do
  case "$1" in
    --ra=*)
      val=${1#--ra=}
      NEW_ARGS+=("--ra" "$(normalize_ra "$val")")
      shift ;;
    --dec=*)
      val=${1#--dec=}
      NEW_ARGS+=("--dec" "$(normalize_dec "$val")")
      shift ;;
    --ra)
      [ "$#" -ge 2 ] || { echo "Missing value after --ra" >&2; exit 2; }
      NEW_ARGS+=("--ra" "$(normalize_ra "$2")")
      shift 2 ;;
    --dec)
      [ "$#" -ge 2 ] || { echo "Missing value after --dec" >&2; exit 2; }
      NEW_ARGS+=("--dec" "$(normalize_dec "$2")")
      shift 2 ;;
    *)
      NEW_ARGS+=("$1")
      shift ;;
  esac
done

# Replace positional parameters for the remainder of run.sh
set -- "${NEW_ARGS[@]}"
# --- End sexagesimal normalization ---
set -euo pipefail

# Exit policy
EXIT_ON_SHORTFALL=${EXIT_ON_SHORTFALL:-1}
MIN_OK_RATIO=${MIN_OK_RATIO:-1.0}

MODE="tess"
RA=${RA:-150.1145}
DEC=${DEC:-2.2050}
WIDTH_ARCMIN=${WIDTH_ARCMIN:-60}
HEIGHT_ARCMIN=${HEIGHT_ARCMIN:-60}
TILE_RADIUS_ARCMIN=${TILE_RADIUS_ARCMIN:-30}
SIZE_ARCMIN=${SIZE_ARCMIN:-60}
SURVEY=${SURVEY:-dss1-red}
PIXEL_SCALE_ARCSEC=${PIXEL_SCALE_ARCSEC:-1.7}
EXPORT=${EXPORT:-both}
WORKDIR=${WORKDIR:-}

# Retry controls
RETRY_MODE=0
RETRY_RUN_DIR=""
RETRY_ATTEMPTS=${RETRY_ATTEMPTS:-4}
RETRY_BACKOFF_BASE=${RETRY_BACKOFF_BASE:-1.0}
RETRY_BACKOFF_CAP=${RETRY_BACKOFF_CAP:-8.0}
RETRY_AFTER=${RETRY_AFTER:-0}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --one) MODE="one"; shift ;;
    --tess) MODE="tess"; shift ;;
    --retry-missing) RETRY_MODE=1; RETRY_RUN_DIR="$2"; shift 2 ;;
    --retry-after) RETRY_AFTER="$2"; shift 2 ;;
    --ra) RA="$2"; shift 2 ;;
    --dec) DEC="$2"; shift 2 ;;
    --center-ra) RA="$2"; shift 2 ;;
    --center-dec) DEC="$2"; shift 2 ;;
    --width-arcmin) WIDTH_ARCMIN="$2"; shift 2 ;;
    --height-arcmin) HEIGHT_ARCMIN="$2"; shift 2 ;;
    --tile-radius-arcmin) TILE_RADIUS_ARCMIN="$2"; shift 2 ;;
    --size-arcmin) SIZE_ARCMIN="$2"; shift 2 ;;
    --survey) SURVEY="$2"; shift 2 ;;
    --pixel-scale-arcsec) PIXEL_SCALE_ARCSEC="$2"; shift 2 ;;
    --export) EXPORT="$2"; shift 2 ;;
    --workdir) WORKDIR="$2"; shift 2 ;;
    -h|--help)
      echo "Usage:";
      echo "  ./run.sh --tess [args] [--retry-after N]";
      echo "  ./run.sh --one  [args]";
      echo "  ./run.sh --retry-missing <run_dir> [env: RETRY_ATTEMPTS, RETRY_BACKOFF_BASE, RETRY_BACKOFF_CAP]";
      exit 0 ;;
    *) echo "[ERROR] Unknown arg: $1"; exit 2 ;;
  esac
done

python -m vasco.cli_export --env-check || true

if ! command -v sex >/dev/null 2>&1 && ! command -v sextractor >/dev/null 2>&1; then
  echo "[WARN] SExtractor not found in PATH (sex/sextractor). On macOS: brew install sextractor" >&2
fi
if ! command -v psfex >/dev/null 2>&1; then
  echo "[WARN] PSFEx not found in PATH. On macOS: brew install psfex" >&2
fi

set -x
TMPLOG=$(mktemp -t vasco_run_XXXX.log)
if [[ "$RETRY_MODE" == "1" ]]; then
  python -m vasco.cli_pipeline retry-missing "$RETRY_RUN_DIR" \
    --survey "$SURVEY" --size-arcmin "$SIZE_ARCMIN" \
    --pixel-scale-arcsec "$PIXEL_SCALE_ARCSEC" \
    --attempts "$RETRY_ATTEMPTS" --backoff-base "$RETRY_BACKOFF_BASE" --backoff-cap "$RETRY_BACKOFF_CAP" \
    --export "$EXPORT" | tee "$TMPLOG"
else
  if [[ "$MODE" == "one" ]]; then
    python -m vasco.cli_pipeline one2pass \
      --ra "$RA" --dec "$DEC" \
      --size-arcmin "$SIZE_ARCMIN" \
      --survey "$SURVEY" \
      --pixel-scale-arcsec "$PIXEL_SCALE_ARCSEC" \
      --export "$EXPORT" \
      ${WORKDIR:+--workdir "$WORKDIR"} | tee "$TMPLOG"
  else
    python -m vasco.cli_pipeline tess2pass \
      --center-ra "$RA" --center-dec "$DEC" \
      --width-arcmin "$WIDTH_ARCMIN" --height-arcmin "$HEIGHT_ARCMIN" \
      --tile-radius-arcmin "$TILE_RADIUS_ARCMIN" \
      --size-arcmin "$SIZE_ARCMIN" \
      --survey "$SURVEY" \
      --pixel-scale-arcsec "$PIXEL_SCALE_ARCSEC" \
      --export "$EXPORT" \
      ${WORKDIR:+--workdir "$WORKDIR"} | tee "$TMPLOG"
  fi
fi
set +x

OUT=$(cat "$TMPLOG"); rm -f "$TMPLOG" || true
RUN_DIR=$(echo "$OUT" | awk '/^Run directory:/ {print $3}' | tail -n1)

if [[ -z "${RUN_DIR:-}" || ! -d "$RUN_DIR" ]]; then
  echo "[WARN] Could not detect run directory from CLI output. See data/runs/ for latest." >&2
  exit 0
fi

read_counts() {
  local f="$1"; local cnt; cnt=""
  if [[ -f "$f" ]]; then
    cnt=$(python -c 'import sys,json; p=sys.argv[1];\ntry:\n d=json.load(open(p))\n print("{} {} {}".format(d.get("planned",""), d.get("downloaded",""), d.get("processed","")))\nexcept Exception:\n pass' "$f" 2>/dev/null || true)
  fi
  echo "$cnt"
}

missing_len() {
  local f="$1"; local n; n=""
  if [[ -f "$f" ]]; then
    n=$(python -c 'import sys,json; p=sys.argv[1];\ntry:\n print(len(json.load(open(p))))\nexcept Exception:\n print(0)' "$f" 2>/dev/null || echo "0")
  else
    n=0
  fi
  echo "$n"
}

RAW_FITS=$(find "$RUN_DIR/raw" -type f -name '*.fits' 2>/dev/null | wc -l | tr -d ' ')
HTML_WARN=$(find "$RUN_DIR/raw" -type f -name '*.html' 2>/dev/null | wc -l | tr -d ' ')
PASS2=$(find "$RUN_DIR/tiles" -type f -name 'pass2.ldac' 2>/dev/null | wc -l | tr -d ' ')
CSV=$(find "$RUN_DIR/tiles" -type f -name 'final_catalog.csv' 2>/dev/null | wc -l | tr -d ' ')
ECSV=$(find "$RUN_DIR/tiles" -type f -name 'final_catalog.ecsv' 2>/dev/null | wc -l | tr -d ' ')
PARQUET=$(find "$RUN_DIR/tiles" -type f -name 'final_catalog.parquet' 2>/dev/null | wc -l | tr -d ' ')

CNT=$(read_counts "$RUN_DIR/RUN_COUNTS.json"); set -- $CNT; PLANNED=${1:-}; DOWNLOADED=${2:-}; PROCESSED=${3:-}
MISSING_BEFORE=$(missing_len "$RUN_DIR/RUN_MISSING.json")

printf "\n=== Post-run summary ===\n"
echo "Run dir:            $RUN_DIR"
if [[ -n "$PLANNED$DOWNLOADED$PROCESSED" ]]; then
  echo "Tessellation:       planned=$PLANNED  downloaded=$DOWNLOADED  processed=$PROCESSED"
fi
echo "Raw FITS downloaded: $RAW_FITS   (HTML warnings: $HTML_WARN)"
echo "Tiles processed:     $PASS2       (pass2.ldac)"
echo "Exports:             CSV=$CSV  ECSV=$ECSV  Parquet=$PARQUET"
if [[ -n "$MISSING_BEFORE" ]]; then
  echo "Missing tiles:       $MISSING_BEFORE  (see RUN_MISSING.json)"
fi
echo "Logs:                $RUN_DIR/logs/download.log"

if [[ "$RETRY_MODE" == "0" && "$MODE" == "tess" && "$RETRY_AFTER" != "0" ]]; then
  if [[ "$MISSING_BEFORE" =~ ^[0-9]+$ && "$MISSING_BEFORE" -gt 0 ]]; then
    echo "\n[INFO] Auto-retry: attempting to recover up to $MISSING_BEFORE tiles (attempts=$RETRY_AFTER) ..."
    set -x
    RLOG=$(mktemp -t vasco_retry_XXXX.log)
    python -m vasco.cli_pipeline retry-missing "$RUN_DIR" \
      --survey "$SURVEY" --size-arcmin "$SIZE_ARCMIN" \
      --pixel-scale-arcsec "$PIXEL_SCALE_ARCSEC" \
      --attempts "$RETRY_AFTER" --backoff-base "$RETRY_BACKOFF_BASE" --backoff-cap "$RETRY_BACKOFF_CAP" \
      --export "$EXPORT" | tee "$RLOG"
    set +x
    rm -f "$RLOG" || true

    RAW_FITS=$(find "$RUN_DIR/raw" -type f -name '*.fits' 2>/dev/null | wc -l | tr -d ' ')
    HTML_WARN=$(find "$RUN_DIR/raw" -type f -name '*.html' 2>/dev/null | wc -l | tr -d ' ')
    PASS2=$(find "$RUN_DIR/tiles" -type f -name 'pass2.ldac' 2>/dev/null | wc -l | tr -d ' ')
    CSV=$(find "$RUN_DIR/tiles" -type f -name 'final_catalog.csv' 2>/dev/null | wc -l | tr -d ' ')
    ECSV=$(find "$RUN_DIR/tiles" -type f -name 'final_catalog.ecsv' 2>/dev/null | wc -l | tr -d ' ')
    PARQUET=$(find "$RUN_DIR/tiles" -type f -name 'final_catalog.parquet' 2>/dev/null | wc -l | tr -d ' ')
    CNT=$(read_counts "$RUN_DIR/RUN_COUNTS.json"); set -- $CNT; PLANNED=${1:-}; DOWNLOADED=${2:-}; PROCESSED=${3:-}
    MISSING_AFTER=$(missing_len "$RUN_DIR/RUN_MISSING.json")

    printf "\n=== After auto-retry ===\n"
    echo "Tessellation:       planned=$PLANNED  downloaded=$DOWNLOADED  processed=$PROCESSED"
    echo "Missing tiles:       $MISSING_AFTER  (see RUN_MISSING.json)"
    echo "Exports:             CSV=$CSV  ECSV=$ECSV  Parquet=$PARQUET"
  fi
fi

if [[ "$EXIT_ON_SHORTFALL" == "1" && -n "$PLANNED" && -n "$PROCESSED" ]]; then
  if [[ "$PLANNED" -gt 0 ]]; then
    RATIO=$(python -c 'import sys; p=float(sys.argv[1]); q=float(sys.argv[2]);\nprint("{:.6f}".format(q/p if p>0 else 1.0))' "$PLANNED" "$PROCESSED")
    PY_OK=$(python -c 'import sys; r=float(sys.argv[1]); thr=float(sys.argv[2]);\nprint(0 if r>=thr else 1)' "$RATIO" "$MIN_OK_RATIO")
    if [[ "$PY_OK" != "0" ]]; then
      echo "[FAIL] processed/planned ratio $RATIO < MIN_OK_RATIO=$MIN_OK_RATIO" >&2
      exit 3
    fi
  fi
fi

exit 0
# Build run-level dashboard (MD + HTML) if run dir was detected
if [[ -n "${RUN_DIR:-}" && -d "$RUN_DIR" ]]; then
  python -m vasco.cli_dashboard build --run-dir "$RUN_DIR" --html true || true
fi

