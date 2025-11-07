#!/usr/bin/env bash
# Insert a sexagesimal RA/Dec normalization block into run.sh (Bash 3.2 compatible)
# - Creates a timestamped backup: run.sh.orig-YYYYmmdd_HHMMSS
# - Idempotent: if the block already exists, it won't insert again
set -euo pipefail

RUNSH="run.sh"
[ -f "$RUNSH" ] || { echo "run.sh not found in current directory" >&2; exit 1; }

TAG_BEGIN="### BEGIN SEXAGESIMAL NORMALIZE"
TAG_END="### END SEXAGESIMAL NORMALIZE"

if grep -q "$TAG_BEGIN" "$RUNSH"; then
  echo "Sexagesimal normalize block already present in run.sh (skipping)"
  exit 0
fi

TS=$(date +%Y%m%d_%H%M%S)
cp -p "$RUNSH" "run.sh.orig-$TS"

tmp="run.sh.tmp-$TS"
{
  # Preserve shebang if present (first line)
  read -r firstline || true
  case "$firstline" in
    "#!/"*) echo "$firstline";;
    *) echo "#!/usr/bin/env bash"; echo "$firstline";;
  esac

  cat <<'BLOCK'

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
BLOCK

  # Append the rest of original file (minus the first line we already handled)
  cat
} < "run.sh.orig-$TS" > "$tmp"

mv "$tmp" "$RUNSH"
chmod +x "$RUNSH"
echo "Inserted sexagesimal normalization into run.sh (backup: run.sh.orig-$TS)"
