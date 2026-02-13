#!/usr/bin/env bash
# VSX flags via TAPVizieR (VOT-only), preserving your January-tested flow:
#   VOT -> CSV -> STILTS-built VOT -> tapquery -> CSV -> Parquet
# Per-chunk outputs named by the input chunk (idempotent, atomic .tmp -> final).
# Usage:
#   ./tools/fetchers/fetch_vsx_stilts_chunked.sh <positions.vot> <out_dir>
# Example:
#   ./tools/fetchers/fetch_vsx_stilts_chunked.sh \
#       ./work/scos_chunks/chunk_0000001.vot ./data/local-cats/_master_optical_parquet_flags/vsx/parts
set -euo pipefail

POS=${1:?positions.vot required}
OUTDIR=${2:?output directory required}
mkdir -p "$OUTDIR"

# Derive chunk key from filename (no extension)
CHUNK="$(basename "$POS")"; CHUNK="${CHUNK%.*}"
STAMP="$(date -u +%FT%TZ)"

# Per-chunk outputs
OUTCSV="$OUTDIR/flags_vsx__${CHUNK}.csv"
PARQ_TMP="$OUTDIR/flags_vsx__${CHUNK}.parquet.tmp"
PARQ="$OUTDIR/flags_vsx__${CHUNK}.parquet"
LOG="$OUTDIR/flags_vsx__${CHUNK}.stilts.log"

# Idempotent re-run: skip if final parquet exists and is non-empty
if [ -s "$PARQ" ]; then
  echo "[skip] $CHUNK already done: $PARQ"
  exit 0
fi

# Scratch
TMPDIR="$(mktemp -d -t vsx_XXXX)"
trap 'rm -rf "$TMPDIR"' EXIT
CSV="$TMPDIR/upload.csv"
VOT="$TMPDIR/upload.vot"

# Your original, verified recode: VOT -> CSV -> STILTS-built VOT
stilts tcopy in="$POS" ifmt=votable out="$CSV" ofmt=csv               2>>"$LOG"
stilts tcopy in="$CSV" ifmt=csv     out="$VOT" ofmt=votable            2>>"$LOG"

# --- ADQL: unchanged (uses u.NUMBER; 5.0 arcsec radius; ICRS) ---
ADQL="SELECT u.NUMBER, COUNT(*) AS nmatch
FROM TAP_UPLOAD.t1 AS u
JOIN \"B/vsx/vsx\" AS v
  ON 1 = CONTAINS(
       POINT('ICRS', v.RAJ2000, v.DEJ2000),
       CIRCLE('ICRS', u.ra, u.dec, 5.0/3600.0)
     )
GROUP BY u.NUMBER"

# TAPVizieR (STILTS manages async/exec/cleanup internally)
stilts tapquery \
  tapurl=https://tapvizier.cds.unistra.fr/TAPVizieR/tap \
  nupload=1 upload1="$VOT" upname1=t1 ufmt1=votable \
  adql="$ADQL" \
  out="$OUTCSV" ofmt=csv                                               2>>"$LOG"

# CSV -> Parquet (flags), one row per NUMBER
python - <<'PY' "$OUTCSV" "$PARQ_TMP" "$CHUNK" "$STAMP"
import sys, pandas as pd, pyarrow as pa, pyarrow.parquet as pq
csv, parq_tmp, chunk, stamp = sys.argv[1:5]
try:
    df = pd.read_csv(csv)
except Exception:
    df = pd.DataFrame(columns=['NUMBER','nmatch'])

if not df.empty and 'NUMBER' in df.columns:
    df['NUMBER'] = df['NUMBER'].astype(str)
    flags = (df[['NUMBER']]
             .drop_duplicates()
             .assign(is_known_variable_or_transient=True))
else:
    flags = pd.DataFrame(columns=['NUMBER','is_known_variable_or_transient'])

# Minimal provenance
flags['source_chunk'] = chunk
flags['queried_at_utc'] = stamp

pq.write_table(pa.Table.from_pandas(flags, preserve_index=False), parq_tmp)
print('[OK] VSX flags ->', parq_tmp, 'rows=', len(flags))
PY

# Atomic move into place
mv -f "$PARQ_TMP" "$PARQ"
echo "[DONE] chunk=${CHUNK} -> ${PARQ}"
