#!/usr/bin/env bash
set -euo pipefail

out="${1:?usage: write_adql_simple.sh <output.sql>}"

cat > "$out" <<'SQL'
SELECT
  p.row_id,
  p.ra AS in_ra, p.dec AS in_dec,
  neow.cntr, neow.ra, neow.dec, neow.mjd,
  neow.w1snr, neow.w2snr,
  neow.qual_frame, neow.qi_fact, neow.saa_sep, neow.moon_masked
FROM neowiser_p1bs_psd AS neow, TAP_UPLOAD.my_positions AS p
WHERE
  CONTAINS(POINT(neow.ra, neow.dec),
           CIRCLE(p.ra, p.dec, 5.0/3600.0)) = 1
  AND neow.qual_frame > 0
  AND neow.qi_fact > 0
  AND neow.saa_sep > 0
  AND neow.moon_masked = '00'
  AND neow.w1snr >= 5
  AND neow.mjd <= 59198
ORDER BY p.row_id, neow.ra
SQL

echo "[OK] ADQL at $out"
