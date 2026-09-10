#!/bin/sh
set -eu

java -jar /app/bridge.jar &
BRIDGE_PID=$!

cleanup() {
  kill "$BRIDGE_PID" 2>/dev/null || true
}
trap cleanup INT TERM EXIT

python -m gunicorn bot:app --bind "0.0.0.0:${PORT:-8080}"
