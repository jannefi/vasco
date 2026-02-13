
#!/usr/bin/env bash
# Chunked runner for PTF flags via IRSA TAP (/sync).
# Splits <positions.csv> into N-row chunks, runs fetch_ptf_irsa_sync.sh per chunk,
# unions the results, and creates final Parquet + summary.

set -euo pipefail

POS="${1:?positions.csv required}"        # NUMBER,ra,dec
OUTDIR="${2:?output dir required}"
CHUNK="${3:-2000}"                        # rows per chunk (data rows, header handled separately)
R_AS="${4:-5}"                            # arcsec
PTF_TABLE="${5:-ptf_objects}"             # or ptf_sources

mkdir -p "$OUTDIR"
TMP="$(mktemp -d -t ptf_run_XXXX)"
trap 'rm -rf "$TMP"' EXIT

# Prepare chunks
HDR="$TMP/header.csv"
{ IFS=; read -r h; echo "$h" > "$HDR"; } < "$POS"
tail -n +2 "$POS" | split -l "$CHUNK" - "$TMP/part_"

# Run per chunk
i=0
for part in "$TMP"/part_*; do
  i=$((i+1))
  chunk="$TMP/chunk_${i}.csv"
  cat "$HDR" "$part" > "$chunk"
  echo "[chunk $i] $(wc -l < "$chunk") rows (incl header)"
  VERBOSE=0 ./scripts/fetch_ptf_irsa_sync.sh \
    "$chunk" "$OUTDIR/chunk_$i" "$R_AS" "$PTF_TABLE"
done

# Union CSVs
UNION="$OUTDIR/flags_${PTF_TABLE}_ALL.csv"
echo "objectnumber" > "$UNION"
for d in "$OUTDIR"/chunk_*; do
  [ -d "$d" ] || continue
  tail -n +2 "$d/flags_${PTF_TABLE}.csv" >> "$UNION"
done

# De-dup and write final CSV
sort -u "$UNION" -o "$UNION"

# Build unified Parquet flags (NUMBER, has_other_archive_match=True)
python - <<'PY' "$UNION" "$OUTDIR/flags_${PTF_TABLE}.parquet"
import sys, pandas as pd, pyarrow.parquet as pq, pyarrow as pa
csv, parq = sys.argv[1], sys.argv[2]
df = pd.read_csv(csv)
if not df.empty and 'objectnumber' in df.columns:
    flags = df[['objectnumber']].drop_duplicates().rename(columns={'objectnumber':'NUMBER'})
    flags['has_other_archive_match'] = True
else:
    flags = pd.DataFrame(columns=['NUMBER','has_other_archive_match'])
pq.write_table(pa.Table.from_pandas(flags, preserve_index=False), parq)
print("[OK] wrote", parq, "rows=", len(flags))
PY

# Summary report with numeric ID normalization (handles "1.0" vs "1")
python - <<'PY' "$POS" "$UNION" "$OUTDIR/flags_${PTF_TABLE}_SUMMARY.txt"
import sys, pandas as pd
pos, union, rpt = sys.argv[1], sys.argv[2], sys.argv[3]
src = pd.read_csv(pos)
hit = pd.read_csv(union)
src['NUMBER_norm'] = pd.to_numeric(src['NUMBER'], errors='coerce').astype('Int64')
hit['NUMBER_norm'] = pd.to_numeric(hit['objectnumber'], errors='coerce').astype('Int64')
merged = src.merge(hit[['NUMBER_norm']], on='NUMBER_norm', how='left', indicator=True)
tot = len(src); matched = (merged['_merge']=='both').sum(); unmatched = (merged['_merge']=='left_only').sum()
with open(rpt,'w') as f:
    f.write(f"Input={pos}\nUnionCSV={union}\nTotal={tot}  Matched={matched}  Unmatched={unmatched}\n")
print("[SUMMARY]", rpt)
PY

echo "[DONE] ALL CSV=$UNION  PARQUET=$OUTDIR/flags_${PTF_TABLE}.parquet"

