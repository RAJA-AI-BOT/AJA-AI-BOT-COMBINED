#!/bin/sh
set -eu

DUKASCOPY_ENABLED_VALUE="${DUKASCOPY_ENABLED:-false}"
case "$(printf '%s' "$DUKASCOPY_ENABLED_VALUE" | tr '[:upper:]' '[:lower:]')" in
  1|true|yes|on)
    echo "Starting Dukascopy Bridge..."
    java -jar /app/bridge.jar &
    BRIDGE_PID=$!
    ;;
  *)
    echo "Dukascopy bridge temporarily DISABLED."
    BRIDGE_PID=""
    ;;
esac

cleanup() {
  if [ -n "${BRIDGE_PID:-}" ]; then
    echo "Stopping Dukascopy Bridge..."
    kill "$BRIDGE_PID" 2>/dev/null || true
  fi
}
trap cleanup INT TERM EXIT

echo "Starting RAJA AI Bot..."
python -m gunicorn bot:app --bind "0.0.0.0:${PORT:-8080}"
