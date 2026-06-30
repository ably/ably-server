#!/usr/bin/env bash
# Shared helpers for the Python track's run scripts.
set -euo pipefail

PY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EMBED_DIR="$(cd "$PY_DIR/.." && pwd)"
VENV_PY="$PY_DIR/.venv/bin/python"
UVICORN="$PY_DIR/.venv/bin/uvicorn"
export ABLY_SERVER_BINARY="${ABLY_SERVER_BINARY:-$PY_DIR/bin/ably-server}"
export ABLY_SERVER_API_KEY="${ABLY_SERVER_API_KEY:-app.key:secret}"

# wait_ready <url> [tries] — poll an HTTP endpoint until 200.
wait_ready() {
  local url="$1" tries="${2:-60}"
  for _ in $(seq 1 "$tries"); do
    if curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    sleep 0.25
  done
  echo "wait_ready: $url never came up" >&2
  return 1
}

# start_app <public_port> [base_path] -> echoes the app PID.
# Launches the FastAPI example via the run_fastapi.py runner (which exits 0 on
# SIGTERM after a graceful shutdown), redirecting logs to $APP_LOG.
start_app() {
  local port="$1" base="${2:-}"
  ABLY_PUBLIC_PORT="$port" ABLY_BASE_PATH="$base" \
    "$VENV_PY" examples/run_fastapi.py \
    > "${APP_LOG:-/dev/null}" 2>&1 &
  echo $!
}
