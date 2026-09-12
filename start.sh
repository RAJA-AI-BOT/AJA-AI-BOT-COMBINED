#!/bin/sh
set -eu

# RAJA Railway production: Python-only runtime.
# BiQuote remains enabled for normal LIVE Forex/metals.
# Legacy Finnhub/Yahoo startup preload remains disabled.
export BIQUOTE_ENABLED=true
export BIQUOTE_API_URL="${BIQUOTE_API_URL:-https://biquote.io/api}"
export RAJA_ENABLE_FINNHUB_PRELOAD=false

echo "Starting RAJA AI Bot (Python-only)..."
echo "BiQuote: ENABLED (primary LIVE Forex/metals)"
echo "Finnhub/Yahoo startup preload: DISABLED"

exec python -m gunicorn bot:app \
  --bind "0.0.0.0:${PORT:-8080}" \
  --worker-class gthread \
  --workers 1 \
  --threads 8 \
  --timeout 20 \
  --keep-alive 5 \
  --access-logfile -
