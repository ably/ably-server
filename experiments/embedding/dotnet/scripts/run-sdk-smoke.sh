#!/usr/bin/env bash
# Verification B: native (unmodified) Ably .NET SDK smoke test THROUGH YARP.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

PORT="${1:-8562}"
LOG="$(mktemp -t ably-dotnet-app)"

echo "== Verification B: native Ably .NET SDK (ably.io) through YARP (:$PORT) =="
start_app "$PORT" "$LOG"
echo "  example app pid=$APP_PID  log=$LOG"
trap 'kill -TERM "$APP_PID" 2>/dev/null || true; wait "$APP_PID" 2>/dev/null || true' EXIT

wait_ready "$PORT"

echo "  running IO.Ably smoke client..."
ABLY_SERVER_API_KEY="$API_KEY" "$DOTNET" "$SMOKE_DLL" "$PORT" "$API_KEY"
RC=$?
echo "  smoke exit=$RC"
exit $RC
