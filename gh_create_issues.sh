#!/usr/bin/env bash
set -euo pipefail
# Usage: ./gh_create_issues.sh [owner/repo]
REPO="${1:-}"
if [[ -n "$REPO" ]]; then
  gh repo set-default "$REPO"
fi

# Ensure labels (no-op if they already exist)
# gh label create has --force in newer versions; fall back to ignoring errors
(gh label create pipeline --color BFD4F2 --description "VASCO pipeline" 2>/dev/null) || true
(gh label create step --color D4C5F9 --description "Step-tracked work" 2>/dev/null) || true
(gh label create vasco --color FAE8C8 --description "VASCO project" 2>/dev/null) || true

# Ensure milestone exists
(gh milestone create "Multi-step split" --description "Breakdown of VASCO pipeline into Steps 0–10" 2>/dev/null) || true

for f in github_issues/*.md; do
  title="$(head -n1 "$f" | sed 's/^# //')"
  echo "Creating: $title"
  gh issue create \
    --title "$title" \
    --body-file "$f" \
    --label pipeline --label step --label vasco \
    --milestone "Multi-step split"
  sleep 0.2
done
