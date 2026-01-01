#!/usr/bin/env bash
set -euo pipefail

# --- Fast CDS defaults (paper-aligned) ---
export VASCO_CDS_MODE="${VASCO_CDS_MODE:-single}"
export VASCO_CDS_MAX_RETRIES="${VASCO_CDS_MAX_RETRIES:-2}"
export VASCO_CDS_BASE_BACKOFF="${VASCO_CDS_BASE_BACKOFF:-1.5}"
export VASCO_CDS_BLOCKSIZE="${VASCO_CDS_BLOCKSIZE:-omit}"
export VASCO_CDS_INTER_CHUNK_DELAY="${VASCO_CDS_INTER_CHUNK_DELAY:-0}"
export VASCO_CDS_JITTER="${VASCO_CDS_JITTER:-0}"

# Tables (override if needed)
GAIA_TABLE="${1:-I/355/gaiadr3}"
PS1_TABLE="${2:-II/349/ps1}"

# --- discover tiles in flat and sharded layouts ---
find_tiles() {
  local root="./data"
  find "$root/tiles" -maxdepth 1 -type d -name 'tile-*' 2>/dev/null || true
  find "$root/tiles_by_sky" -type d -path '*/ra_bin=*/dec_bin=*/tile-*' 2>/dev/null || true
}

has_data_rows() {
  local f="$1"
  [[ -s "$f" ]] || return 1
  local rows; rows=$(awk 'NR>1{c++} END{print c+0}' "$f" 2>/dev/null)
  [[ ${rows:-0} -gt 0 ]]
}

parse_ra_dec_from_dir() {
  local dname="$1"; local base; base="$(basename "$dname")"
  [[ "$base" =~ ^tile-RA([^/]+)-DEC(.+)$ ]] || return 1
  echo "${BASH_REMATCH[1]} ${BASH_REMATCH[2]}"
}

for tile in $(find_tiles); do
  [[ -d "$tile" ]] || continue
  echo "=== [TILE] $tile ==="
  if [[ ! -f "$tile/pass2.ldac" ]]; then
    if ! ls "$tile/raw/"*.fits >/dev/null 2>&1; then
      if ra_dec=$(parse_ra_dec_from_dir "$tile"); then
        ra=$(echo "$ra_dec" | awk '{print $1}')
        dec=$(echo "$ra_dec" | awk '{print $2}')
        echo "[STEP1] Downloading FITS for RA=$ra Dec=$dec"
        python -u -m vasco.cli_pipeline step1-download \
          --ra "$ra" --dec "$dec" \
          --size-arcmin 30 --survey dss1-red --pixel-scale-arcsec 1.7 \
          --workdir "$tile" || true
      else
        echo "[WARN] Cannot parse RA/Dec from $tile; skipping"
        continue
      fi
    fi
    if ! ls "$tile/raw/"*.fits >/dev/null 2>&1; then
      echo "[SKIP] No FITS in raw/ (POSS-I filter or download error); skipping tile"
      continue
    fi
    if [[ ! -f "$tile/pass1.ldac" ]]; then
      echo "[STEP2] pass1"
      python -u -m vasco.cli_pipeline step2-pass1 --workdir "$tile" || true
    fi
    if [[ ! -f "$tile/pass2.ldac" ]]; then
      echo "[STEP3] PSFEx + pass2"
      python -u -m vasco.cli_pipeline step3-psf-and-pass2 --workdir "$tile" || true
    fi
    if [[ ! -f "$tile/pass2.ldac" ]]; then
      echo "[FAIL] pass2.ldac still missing after step2/3; skipping tile"
      continue
    fi
  fi

  need_xmatch=0
  gaia_csv="$tile/xmatch/sex_gaia_xmatch_cdss.csv"
  ps1_csv="$tile/xmatch/sex_ps1_xmatch_cdss.csv"
  if ! has_data_rows "$gaia_csv"; then need_xmatch=1; fi
  if ! has_data_rows "$ps1_csv"; then need_xmatch=1; fi
  if [[ $need_xmatch -eq 1 ]]; then
    echo "[STEP4] CDS xmatch → $tile"
    python -u -m vasco.cli_pipeline step4-xmatch \
      --workdir "$tile" \
      --xmatch-backend cds \
      --xmatch-radius-arcsec 5.0 \
      --size-arcmin 30 \
      --cds-gaia-table "$GAIA_TABLE" \
      --cds-ps1-table "$PS1_TABLE" || true
  else
    echo "[STEP4] Skipped (xmatch CSVs already populated)"
  fi

  need_within5=0
  shopt -s nullglob
  for f in "$tile"/xmatch/sex_*_xmatch*_cdss.csv; do
    wf="${f%.csv}_within5arcsec.csv"
    [[ -s "$wf" ]] || need_within5=1
  done
  shopt -u nullglob
  if [[ $need_within5 -eq 1 ]]; then
    echo "[STEP5] within5"
    python -u -m vasco.cli_pipeline step5-filter-within5 --workdir "$tile" || true
  else
    echo "[STEP5] Skipped (within5 CSVs present)"
  fi

  if [[ ! -f "$tile/final_catalog.csv" ]]; then
    echo "[STEP6] summarize"
    python -u -m vasco.cli_pipeline step6-summarize --workdir "$tile" --export csv --hist-col FWHM_IMAGE || true
  else
    echo "[STEP6] Skipped (final_catalog.csv present)"
  fi

done

echo "=== Backfill sweep complete ==="
