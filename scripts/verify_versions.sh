
#!/usr/bin/env bash
set -euo pipefail

REMOTE=${1:-$(git remote | head -1)}
BRANCH=$(git rev-parse --abbrev-ref HEAD)
LOCAL=$(git rev-parse HEAD)
REMOTE_HASH=$(git ls-remote "$REMOTE" -h "refs/heads/$BRANCH" | awk '{print $1}')
IMAGE_HASH=$(docker inspect astro-tools:latest --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' 2>/dev/null || echo "none")

echo "Branch: $BRANCH"
echo "Local:  $LOCAL"
echo "Remote: ${REMOTE_HASH:-<not found>}"
echo "Image:  ${IMAGE_HASH:-<none>}"

if [ -z "${REMOTE_HASH}" ]; then
  echo "⚠️  Remote branch not found. Push it:"
  echo "    git push -u $REMOTE $BRANCH"
fi

if [ "$LOCAL" != "${REMOTE_HASH:-$LOCAL}" ]; then
  echo "⚠️  Local and Remote differ."
else
  echo "✅ Local and Remote match."
fi

if [ "$IMAGE_HASH" = "none" ]; then
  echo "ℹ️  Image has no commit label. Rebuild with:"
  echo "    docker build --build-arg GIT_COMMIT=$LOCAL -t astro-tools:latest ."
elif [ "$LOCAL" != "$IMAGE_HASH" ]; then
  echo "⚠️  Image commit label differs from Local."
else
  echo "✅ Image matches Local."
fi

