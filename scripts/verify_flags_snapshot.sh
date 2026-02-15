#!/usr/bin/env bash
# verify_flags_snapshot.sh — v2.1 (schema-aligned; row_id-safe)
# Verifies VOSA-like, SuperCOSMOS, PTF(ngood), VSX (and optional SkyBoT) flags.
# Uses chunk CSVs (row_id + number) as base to avoid NUMBER reuse pitfalls.
set -euo pipefail

# --------------------[ CONFIG: edit here or via env ]--------------------
SURV_GLOB="${SURV_GLOB:-./data/vasco-candidates/post16/candidates_final_core_dataset_20260205_170455/ra_bin=*/dec_bin=*/part-*.parquet}"
CHUNKS_GLOB="${CHUNKS_GLOB:-./work/scos_chunks/chunk_*.csv}"

FLAGS_ROOT="${FLAGS_ROOT:-./data/local-cats/_master_optical_parquet_flags}"
VOSA_CANON="${VOSA_CANON:-$FLAGS_ROOT/vosa_like/flags_vosa_like.parquet}"            # row_id-keyed
SCOS_CANON="${SCOS_CANON:-$FLAGS_ROOT/flags_supercosmos/flags_supercosmos.parquet}"  # row_id-keyed
PTF_CANON="${PTF_CANON:-$FLAGS_ROOT/flags_ptf_objects_ngood.parquet}"                # NUMBER-keyed
VSX_PARTS="${VSX_PARTS:-$FLAGS_ROOT/vsx/parts/flags_vsx__*.parquet}"                 # NUMBER-keyed

SKYBOT_PARTS="${SKYBOT_PARTS:-$FLAGS_ROOT/skybot/parts/flags_skybot__*.parquet}"     # optional (row_id & NUMBER)
INCLUDE_SKYBOT="${INCLUDE_SKYBOT:-0}"

STRICT="${STRICT:-0}"
# ------------------------------------------------------------------------

c_bold='\033[1m'; c_red='\033[31m'; c_yel='\033[33m'; c_grn='\033[32m'; c_off='\033[0m'
say()  { printf "%b[verify]%b %s\n" "$c_bold" "$c_off" "$*"; }
ok()   { printf "%b[ok]%b     %s\n" "$c_grn" "$c_off" "$*"; }
warn() { printf "%b[warn]%b   %s\n" "$c_yel" "$c_off" "$*"; }
err()  { printf "%b[err]%b    %s\n" "$c_red" "$c_off" "$*"; }
die()  { err "$*"; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || die "Missing executable: $1"; }
need duckdb

check_glob() { # $1 label, $2 glob
  shopt -s nullglob
  local matches=( $2 )
  shopt -u nullglob
  if (( ${#matches[@]} == 0 )); then
    if (( STRICT == 1 )); then die "Missing $1 at: $2"; else warn "Missing $1 at: $2"; fi
    return 1
  else
    ok "$1: ${#matches[@]} file(s)"
    return 0
  fi
}

say "Input discovery"
check_glob "Survivors parquet (Hive)" "$SURV_GLOB"
check_glob "Chunk CSVs (row_id + number)" "$CHUNKS_GLOB"
check_glob "VOSA canonical" "$VOSA_CANON"
check_glob "SuperCOSMOS canonical" "$SCOS_CANON"
check_glob "PTF ngood canonical" "$PTF_CANON"
check_glob "VSX parts" "$VSX_PARTS"
if (( INCLUDE_SKYBOT == 1 )); then check_glob "SkyBoT parts (optional)" "$SKYBOT_PARTS" || true; fi

# --------------------[ Section 0 — Survivors totals ]--------------------
say "Survivors (strict) totals via parquet_scan()"
duckdb -batch <<'SQL'
INSTALL parquet; LOAD parquet;
-- Use environment expansion in shell, not here; heredoc is quoted at call site.
SQL
duckdb -batch <<SQL
INSTALL parquet; LOAD parquet;
SELECT
  COUNT(*) AS survivors_rows,
  COUNT(DISTINCT NUMBER) AS survivors_distinct_number
FROM parquet_scan('$SURV_GLOB');
SQL

# --------------------[ Section 1 — VOSA-like tallies ]-------------------
say "VOSA-like canonical tallies (row_id-keyed)"
duckdb -batch <<SQL
INSTALL parquet; LOAD parquet;
WITH v AS (SELECT * FROM parquet_scan('$VOSA_CANON'))
SELECT
  COUNT(*)                                            AS total_rows,
  SUM(has_vosa_like_match)::BIGINT                    AS any_vosa_like,
  SUM(has_catwise2020_match)::BIGINT                  AS catwise,
  SUM(has_unwise_match)::BIGINT                       AS unwise,
  SUM(has_allwise_match)::BIGINT                      AS allwise,
  SUM(has_2mass_match)::BIGINT                        AS two_mass,
  SUM(has_galex_match)::BIGINT                        AS galex
FROM v;
SQL

# ---------[ Section 2 — Build identity bridge (in this session) ]--------
say "Building row_id <-> NUMBER bridge from chunk CSVs (lowercase 'number')"
duckdb -batch <<SQL
INSTALL parquet; LOAD parquet;
CREATE OR REPLACE VIEW chunk_map AS
SELECT
  CAST(row_id AS VARCHAR)  AS row_id,
  CAST(number AS BIGINT)   AS NUMBER
FROM read_csv_auto('$CHUNKS_GLOB');

SELECT COUNT(*) AS chunk_rows,
       COUNT(DISTINCT row_id)   AS chunk_distinct_row_id,
       COUNT(DISTINCT NUMBER)   AS chunk_distinct_number
FROM chunk_map;
SQL

# ----------------[ Section 3 — SuperCOSMOS tallies + join ]--------------
say "SuperCOSMOS tallies and joinability via row_id bridge (recreate bridge here)"
duckdb -batch <<SQL
INSTALL parquet; LOAD parquet;

-- Bridge recreated in this session (views are per-session)
CREATE OR REPLACE VIEW chunk_map AS
SELECT CAST(row_id AS VARCHAR) AS row_id,
       CAST(number AS BIGINT)  AS NUMBER
FROM read_csv_auto('$CHUNKS_GLOB');

CREATE OR REPLACE VIEW scos AS
  SELECT row_id, is_supercosmos_artifact
  FROM parquet_scan('$SCOS_CANON');

-- Raw tallies
SELECT COUNT(*) AS scos_rows,
       SUM(is_supercosmos_artifact)::BIGINT AS scos_true
FROM scos;

-- Joinability to chunk_map (should be 0)
SELECT
  (SELECT COUNT(*) FROM scos) AS scos_rows,
  (SELECT COUNT(*) FROM scos s LEFT JOIN chunk_map m USING(row_id)
    WHERE m.row_id IS NULL)   AS scos_not_in_chunks;
SQL

# --------------------[ Section 4 — PTF + VSX tallies ]-------------------
say "PTF (ngood) and VSX tallies (NUMBER-keyed, safe)"
duckdb -batch <<SQL
INSTALL parquet; LOAD parquet;

SELECT COUNT(*) AS ptf_distinct_numbers,
       SUM(ptf_match_ngood)::BIGINT AS ptf_true
FROM (SELECT DISTINCT * FROM parquet_scan('$PTF_CANON'));

WITH vsx AS (SELECT * FROM parquet_scan('$VSX_PARTS'))
SELECT COUNT(DISTINCT NUMBER) AS vsx_distinct_numbers FROM vsx;
SQL

# ---------[ Section 5 — Union readiness (row_id base only) ]------------
say "Union readiness (row_id base). Counts are DISTINCT row_id per gate — no tile_id needed."
duckdb -batch <<SQL
INSTALL parquet; LOAD parquet;

-- Recreate the bridge for this session
CREATE OR REPLACE VIEW chunk_map AS
SELECT CAST(row_id AS VARCHAR) AS row_id,
       CAST(number AS BIGINT)  AS NUMBER
FROM read_csv_auto('$CHUNKS_GLOB');

-- Flags
CREATE OR REPLACE VIEW vosa AS
  SELECT row_id, has_vosa_like_match FROM parquet_scan('$VOSA_CANON');
CREATE OR REPLACE VIEW scos AS
  SELECT row_id, is_supercosmos_artifact FROM parquet_scan('$SCOS_CANON');
CREATE OR REPLACE VIEW ptf AS
  SELECT NUMBER, ptf_match_ngood FROM parquet_scan('$PTF_CANON');
CREATE OR REPLACE VIEW vsx AS
  SELECT NUMBER, is_known_variable_or_transient FROM parquet_scan('$VSX_PARTS');

WITH joined AS (
  SELECT
    m.row_id,
    -- Attach NUMBER-based flags to each row_id via NUMBER from chunk_map
    COALESCE(v.has_vosa_like_match, FALSE)            AS vosa_any,
    COALESCE(c.is_supercosmos_artifact, FALSE)        AS scos_art,
    COALESCE(p.ptf_match_ngood, FALSE)                AS ptf_ngood,
    COALESCE(x.is_known_variable_or_transient, FALSE) AS vsx_hit
  FROM chunk_map m
  LEFT JOIN vosa v USING(row_id)
  LEFT JOIN scos c USING(row_id)
  LEFT JOIN ptf  p USING(NUMBER)
  LEFT JOIN vsx  x USING(NUMBER)
)
SELECT
  COUNT(DISTINCT CASE WHEN vosa_any THEN row_id END)::BIGINT  AS vosa_true_row_id,
  COUNT(DISTINCT CASE WHEN scos_art THEN row_id END)::BIGINT  AS scos_true_row_id,
  COUNT(DISTINCT CASE WHEN ptf_ngood THEN row_id END)::BIGINT AS ptf_true_row_id,
  COUNT(DISTINCT CASE WHEN vsx_hit THEN row_id END)::BIGINT   AS vsx_true_row_id
FROM joined;
SQL

# ------------------[ Section 6 — Optional SkyBoT now ]-------------------
if (( INCLUDE_SKYBOT == 1 )); then
  say "SkyBoT parts (current) — counts per row_id (optional)"
  duckdb -batch <<SQL
  INSTALL parquet; LOAD parquet;

  CREATE OR REPLACE VIEW chunk_map AS
  SELECT CAST(row_id AS VARCHAR) AS row_id,
         CAST(number AS BIGINT)  AS NUMBER
  FROM read_csv_auto('$CHUNKS_GLOB');

  CREATE OR REPLACE VIEW skybot AS
    SELECT row_id,
           COALESCE(has_skybot_match, FALSE)  AS skybot_strict,
           COALESCE(wide_skybot_match, FALSE) AS skybot_wide
    FROM parquet_scan('$SKYBOT_PARTS');

  SELECT
    COUNT(DISTINCT CASE WHEN skybot_strict THEN row_id END)::BIGINT AS skybot_strict_row_id,
    COUNT(DISTINCT CASE WHEN skybot_wide   THEN row_id END)::BIGINT AS skybot_wide_row_id
  FROM skybot s
  -- Limit to our survivor rows (chunk_map) to avoid counting any stray rows
  JOIN chunk_map m USING(row_id);
SQL
else
  warn "Skipping SkyBoT sections (set INCLUDE_SKYBOT=1 to include)"
fi

ok "Verification completed."
