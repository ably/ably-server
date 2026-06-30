#!/usr/bin/env bash
# Shared helpers for the .NET embedding PoC verification scripts.
set -euo pipefail

# Resolve repo root from this script's location (dotnet/scripts -> repo root).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOTNET_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ROOT="$(cd "$DOTNET_DIR/../../.." && pwd)"

export PATH="/opt/homebrew/bin:$PATH"

DOTNET="${DOTNET:-dotnet}"
API_KEY="${ABLY_SERVER_API_KEY:-app.key:secret}"
SERVER_BIN="$DOTNET_DIR/bin/ably-server"
HARNESS="$ROOT/experiments/embedding/harness/harness"
APP_DLL="$DOTNET_DIR/examples/ExampleApp/bin/Release/net10.0/ExampleApp.dll"
SMOKE_DLL="$DOTNET_DIR/examples/SdkSmoke/bin/Release/net10.0/SdkSmoke.dll"

# wait_ready <port> <tries> : poll a proxied /readyz until 200 or give up.
wait_ready() {
  local port="$1" tries="${2:-100}" i
  for ((i = 1; i <= tries; i++)); do
    if curl -fsS "http://127.0.0.1:$port/readyz" >/dev/null 2>&1; then
      echo "  /readyz green on :$port (after $i tries)"
      return 0
    fi
    sleep 0.2
  done
  echo "  /readyz NEVER green on :$port" >&2
  return 1
}

# start_app <publicPort> <logfile> : launch the example host in the CALLER's
# shell (so `wait` can reap it) and set the global APP_PID. We must background
# directly here — using command substitution ($(start_app ...)) would spawn the
# child in a subshell where the parent script can no longer `wait` on it.
start_app() {
  local port="$1" log="$2"
  ABLY_SERVER_API_KEY="$API_KEY" \
  ABLY_SERVER_BINARY="$SERVER_BIN" \
    "$DOTNET" "$APP_DLL" --port "$port" >"$log" 2>&1 &
  APP_PID=$!
}

# embedded_pid <logfile> : extract the child ably-server PID from the app log.
embedded_pid() {
  grep -oE 'spawned ably-server pid [0-9]+' "$1" | tail -1 | grep -oE '[0-9]+'
}
