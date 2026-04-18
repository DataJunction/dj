#!/bin/bash
# Multi-worktree Docker Compose wrapper.
# Each worktree can have a .dev-port file containing a slot number (1, 2, 3…).
# The slot number determines port offsets so multiple DJ instances can run simultaneously.
#
# Slot 0 (default): DJ 8000, UI 3000, PG 5432
# Slot 1:           DJ 8100, UI 3100, PG 5433
# Slot 2:           DJ 8200, UI 3200, PG 5434
# …
#
# Setup a new slot:
#   git worktree add ../dj-slot-1
#   echo 1 > ../dj-slot-1/.dev-port
#   cd ../dj-slot-1
#   ./dev.sh up          # DJ on :8100, UI on :3100, PG on :5433
#
# Check ports:
#   ./dev.sh urls
#
# Reuse a slot for a new feature:
#   ./dev.sh down
#   git checkout -b next-feature
#   ./dev.sh up          # same ports as before

set -euo pipefail

OFFSET=$(cat .dev-port 2>/dev/null || echo 0)
BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")

export COMPOSE_PROJECT_NAME="dj-slot-${OFFSET}"
export DJ_CONTAINER_PREFIX="dj-s${OFFSET}"
export DJ_PORT=$((8000 + OFFSET * 100))
export DJ_UI_PORT=$((3000 + OFFSET * 100))
export DJ_PG_PORT=$((5432 + OFFSET))
export DJ_QS_PORT=$((8001 + OFFSET * 100))
export DJ_REDIS_PORT=$((6379 + OFFSET))
export DJ_JUPYTER_PORT=$((8181 + OFFSET * 100))
export DJ_TRINO_PORT=$((8080 + OFFSET * 100))

case "${1:-}" in
  urls)
    echo "=== Slot $OFFSET | Branch: $BRANCH ==="
    echo "DJ  : http://localhost:$DJ_PORT"
    echo "UI  : http://localhost:$DJ_UI_PORT"
    echo "PG  : localhost:$DJ_PG_PORT"
    exit 0
    ;;
esac

echo "=== Slot $OFFSET | Branch: $BRANCH ==="
echo "DJ  : http://localhost:$DJ_PORT"
echo "UI  : http://localhost:$DJ_UI_PORT"
echo ""

docker compose "$@"
