#!/bin/sh
set -eu

# RAJA AI Bot: Dukascopy is permanently OFF at runtime.
# BiQuote remains the primary live Forex/metals feed.
export DUKASCOPY_ENABLED=false
export BIQUOTE_ENABLED=true
export BIQUOTE_API_URL="${BIQUOTE_API_URL:-https://biquote.io/api}"

# Do NOT start the Java Dukascopy bridge.
echo "Starting RAJA AI Bot..."
echo "BiQuote: ENABLED"
echo "Dukascopy: DISABLED"

exec python -m gunicorn bot:app --bind "0.0.0.0:${PORT:-8080}" --threads 4 --timeout 30
