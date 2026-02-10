#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="${REPO_ROOT:-$HOME/code/vasco}"; cd "$REPO_ROOT"

WORK="${WORK:-./work}"
CHUNK_DIR="${CHUNK_DIR:-$WORK/scos_chunks}"
OUTROOT="${OUTROOT:-./data/local-cats/_master_optical_parquet_flags/flags_supercosmos}"
LOG_DIR="${LOG_DIR:-$REPO_ROOT/logs}"
DONE_LIST="${DONE_LIST:-$LOG_DIR/SCOS_DONE.list}"
RUN_LOG="${RUN_LOG:-$LOG_DIR/SCOS_RUN.log}"

mkdir -p "$OUTROOT" "$LOG_DIR"; touch "$DONE_LIST" "$RUN_LOG"
shopt -s nullglob
mapfile -t chunks < <(ls -1 "$CHUNK_DIR"/chunk_*.vot "$CHUNK_DIR"/chunk_*.csv 2>/dev/null | sort)

for f in "${chunks[@]}"; do
  base="$(basename "${f%.*}")"
  grep -qx "$base" "$DONE_LIST" && { echo "[skip] $base" | tee -a "$RUN_LOG"; continue; }
  src="$f"
  if [[ "$f" == *.csv ]]; then
    vot="${f%.csv}.vot"
    stilts tcopy in="$f" ifmt=csv out="$vot" ofmt=votable
    src="$vot"
  fi
  tmpout="$OUTROOT/_tmp/${base}"
  final_parq="$OUTROOT/flags_supercosmos__${base}.parquet"
  mkdir -p "$tmpout"
  if ./scripts/fetch_supercosmos_stilts.sh "$src" "$tmpout"; then
    [[ -f "$tmpout/flags_supercosmos.parquet" ]] && mv -f "$tmpout/flags_supercosmos.parquet" "$final_parq"
    rm -rf "$tmpout"
    echo "$base" >> "$DONE_LIST"
    echo "[ok] $base" | tee -a "$RUN_LOG"
  else
    echo "[fail] $base" | tee -a "$RUN_LOG"
    rm -rf "$tmpout"; exit 1
  fi
done

# --- Consolidate per-chunk outputs -----------------------------------------
if command -v duckdb >/dev/null 2>&1; then
  echo "[info] consolidating with DuckDB" | tee -a "$RUN_LOG"
  duckdb -c "
  PRAGMA memory_limit='9GB';
  PRAGMA temp_directory='$WORK/_duckdb_tmp';
  CREATE OR REPLACE TABLE scos_union AS
    SELECT * FROM read_parquet('$OUTROOT/flags_supercosmos__*.parquet');
  CREATE OR REPLACE TABLE scos_dedup AS
    SELECT * FROM (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY row_id ORDER BY row_id) AS rn
      FROM scos_union
    ) WHERE rn=1;
  COPY (SELECT row_id, is_supercosmos_artifact FROM scos_dedup)
  TO '$OUTROOT/flags_supercosmos.parquet' (FORMAT PARQUET);
  "
else
  echo "[warn] duckdb CLI not found; falling back to Python consolidator" | tee -a "$RUN_LOG"
  python - "$OUTROOT" <<'PY'
import glob, sys, pandas as pd, pyarrow as pa, pyarrow.parquet as pq, os
outroot = sys.argv[1]
paths = sorted(glob.glob(os.path.join(outroot, "flags_supercosmos__*.parquet")))
if not paths:
    print("[ERR] no per-chunk Parquets to consolidate", file=sys.stderr); sys.exit(2)
dfs = [pd.read_parquet(p) for p in paths]
df = pd.concat(dfs, ignore_index=True)
if "row_id" not in df.columns:
    print("[ERR] expected 'row_id' in schema; got:", list(df.columns), file=sys.stderr); sys.exit(3)
df = df[["row_id","is_supercosmos_artifact"]].drop_duplicates(subset="row_id")
pq.write_table(pa.Table.from_pandas(df, preserve_index=False),
               os.path.join(outroot, "flags_supercosmos.parquet"))
print(f"[DONE] -> {os.path.join(outroot, 'flags_supercosmos.parquet')} rows={len(df)}")
PY
fi

echo "[DONE] consolidation finished" | tee -a "$RUN_LOG"
