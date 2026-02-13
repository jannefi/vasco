#!/usr/bin/env bash
# Bootstrap a local VSX mirror from CDS/VizieR and build a slim FITS for fast local matching.
# Output files (by default):
#   ./data/local-cats/_external_catalogs/vsx/ReadMe
#   ./data/local-cats/_external_catalogs/vsx/vsx.dat.gz
#   ./data/local-cats/_external_catalogs/vsx/vsx_master_slim.fits  (RAdeg, DEdeg, OID, Name, Type)

set -euo pipefail

ROOT="${1:-./data/local-cats/_external_catalogs/vsx}"
mkdir -p "${ROOT}"

cd "${ROOT}"

# 1) Fetch metadata and main table from VizieR/CDS (two mirrors for robustness)
READMES=(
  "https://cdsarc.cds.unistra.fr/ftp/B/vsx/ReadMe"
  "https://vizier.cds.unistra.fr/ftp/B/vsx/ReadMe"
)
DATAS=(
  "https://cdsarc.cds.unistra.fr/viz-bin/nph-Cat/txt.gz?B/vsx/vsx.dat"
  "https://vizier.cds.unistra.fr/ftp/B/vsx/vsx.dat.gz"
)

# Fetch ReadMe
if [ ! -s ReadMe ]; then
  for u in "${READMES[@]}"; do
    echo "[fetch] $u"
    if curl -fSL --retry 3 -o ReadMe.new "$u"; then
      mv -f ReadMe.new ReadMe
      break
    fi
  done
fi
[ -s ReadMe ] || { echo "[error] ReadMe not fetched"; exit 2; }

# Fetch data (~2.5 GB gzip)
if [ ! -s vsx.dat.gz ]; then
  for u in "${DATAS[@]}"; do
    echo "[fetch] $u"
    if curl -fSL --retry 3 -o vsx.dat.gz.new "$u"; then
      mv -f vsx.dat.gz.new vsx.dat.gz
      break
    fi
  done
fi
[ -s vsx.dat.gz ] || { echo "[error] vsx.dat.gz not fetched"; exit 2; }

# 2) Convert VizieR CDS fixed-width to a slim FITS using STILTS.
#    We keep only columns needed for cone matching + a couple of IDs:
#    RAdeg, DEdeg, OID, Name, Type
SLIM_FITS="vsx_master_slim.fits"
if [ ! -s "${SLIM_FITS}" ]; then
  echo "[stilts] building ${SLIM_FITS}"
  stilts tpipe \
    ifmt=cds in=vsx.dat.gz inmeta=ReadMe \
    cmd='keepcols "RAdeg DEdeg OID Name Type"' \
    out="${SLIM_FITS}" ofmt=fits
fi

# 3) Quick sanity (row count)
echo "[stilts] count rows in ${SLIM_FITS}"
stilts tpipe in="${SLIM_FITS}" ifmt=fits cmd='addcol one 1; stats col=one' out=/dev/null | sed -n 's/^.*Nrow=//p'
echo "[done] Local VSX is ready at: ${ROOT}/${SLIM_FITS}"

