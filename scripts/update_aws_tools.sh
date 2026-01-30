#!/usr/bin/env bash
set -euo pipefail

TOOLS_DIR="../aws-tools-personal/scripts"
DEST_DIR="./scripts/automation"

echo "[INFO] Ensuring destination folder exists..."
mkdir -p "$DEST_DIR"

echo "[INFO] Copying AWS automation scripts..."
cp -v "$TOOLS_DIR"/*.sh "$DEST_DIR"/

echo "[OK] AWS automation scripts updated."

