#!/bin/sh

set -eu

echo "Starting Dukascopy Bridge..."

java -jar /app/bridge.jar &
BRIDGE_PID=$!

cleanup() {
    echo "Stopping Dukascopy Bridge..."
    kill "$BRIDGE_PID" 2>/dev/null || true
}

trap cleanup INT TERM EXIT

echo "Starting RAJA AI Bot..."

python -m gunicorn bot:app --bind "0.0.0.0:${PORT:-8080}"
