
#!/usr/bin/env bash
set -euo pipefail

IMG=${1:-astro-tools:latest}
TMP=$(mktemp -d)
CID=$(docker create "$IMG")
docker cp "$CID":/app/vasco/cli_pipeline.py "$TMP/cli_pipeline.from_image.py"
docker rm "$CID"

python3 - <<PY
import py_compile, sys
py_compile.compile("$TMP/cli_pipeline.from_image.py", doraise=True)
print("OK: syntax validated")
PY

cp "$TMP/cli_pipeline.from_image.py" vasco/cli_pipeline.py
git add vasco/cli_pipeline.py
git commit -m "Restore cli_pipeline.py from working Docker image"
echo "Restored cli_pipeline.py from image $IMG and committed."
