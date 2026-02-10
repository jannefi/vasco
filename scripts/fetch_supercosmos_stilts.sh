#!/usr/bin/env bash
# ./scripts/fetch_supercosmos_stilts.sh <positions.vot> <flags_out_root>
set -euo pipefail

POS=${1:?positions.vot required}
OUTROOT=${2:?flags out root required}
mkdir -p "$OUTROOT" "$OUTROOT/_tmp"

TMPDIR="$(mktemp -d -t scos_XXXX)"
trap 'rm -rf "$TMPDIR"' EXIT

CSV="$TMPDIR/upload.csv"
VOT="$TMPDIR/upload.vot"
OUTCSV="$OUTROOT/flags_supercosmos.csv"
OUTPARQ="$OUTROOT/flags_supercosmos.parquet"

# Radius knob (arcsec). Default 5.0 if not set; may set SCOS_RADIUS_ARCSEC=7.0 for smoke.
RARC="${SCOS_RADIUS_ARCSEC:-5.0}"

# Normalize input (ensures numeric ra/dec)
stilts tcopy in="$POS" ifmt=votable out="$CSV" ofmt=csv
stilts tcopy in="$CSV" ifmt=csv     out="$VOT" ofmt=votable

# ADQL: row_id-only (no CAST, no NUMBER to avoid reserved-word issues)
ADQL="SELECT u.row_id AS row_id, COUNT(*) AS nmatch
      FROM TAP_UPLOAD.t1 AS u
      JOIN supercosmos.sources AS s
        ON 1 = CONTAINS(
             POINT('ICRS', s.raj2000, s.dej2000),
             CIRCLE('ICRS', u.ra, u.dec, ${RARC}/3600.0)
           )
      GROUP BY u.row_id"

# TAP query -> CSV
stilts tapquery \
  tapurl=https://dc.g-vo.org/__system__/tap/run \
  nupload=1 upload1="$VOT" upname1=t1 ufmt1=votable \
  adql="$ADQL" \
  out="$OUTCSV" ofmt=csv

# Loud sanity checks
if [[ ! -s "$OUTCSV" ]]; then
  echo "[ERR] TAP produced no CSV: $OUTCSV" >&2
  exit 2
fi
ROWS=$(wc -l < "$OUTCSV" || echo 0)
echo "[INFO] TAP CSV rows (incl header): $ROWS"
if [[ "$ROWS" -le 1 ]]; then
  echo "[ERR] TAP CSV has header only; aborting to avoid silent zero output." >&2
  exit 3
fi

# CSV -> Parquet (requires literal 'row_id')
python - "$OUTCSV" "$OUTPARQ" <<'PY'
import sys, pandas as pd, pyarrow as pa, pyarrow.parquet as pq
csv, outp = sys.argv[1], sys.argv[2]
df = pd.read_csv(csv)
if "row_id" not in df.columns:
    raise SystemExit("[ERR] Expected 'row_id' in TAP output; got: " + ",".join(df.columns))
flag = df[["row_id"]].drop_duplicates().assign(is_supercosmos_artifact=True)
pq.write_table(pa.Table.from_pandas(flag, preserve_index=False), outp)
print(f"[OK] SuperCOSMOS flags -> {outp} rows={len(flag)}")
PY
