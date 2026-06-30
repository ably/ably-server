#!/usr/bin/env bash
# Run the native ably-python smoke test through the FastAPI proxy.
#   ./scripts/run-sdk-smoke.sh [port]   -> results/python-smoke.json
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

PORT="${1:-8583}"
APP_LOG="$(mktemp)"; export APP_LOG
cd "$PY_DIR"

APP_PID="$(start_app "$PORT" "")"
trap 'kill -TERM "$APP_PID" 2>/dev/null || true; wait "$APP_PID" 2>/dev/null || true' EXIT
wait_ready "http://127.0.0.1:$PORT/readyz"

set +e
ABLY_PUBLIC_PORT="$PORT" "$VENV_PY" examples/ably_python_smoke.py
RC=$?
set -e
exit $RC
