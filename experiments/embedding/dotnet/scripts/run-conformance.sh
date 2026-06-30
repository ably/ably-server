#!/usr/bin/env bash
# Verification A: run the Go conformance harness THROUGH the .NET+YARP host.
# Writes the JSON report to experiments/embedding/results/dotnet-yarp.json.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

PORT="${1:-8561}"
LOG="$(mktemp -t ably-dotnet-app)"
RESULTS="$ROOT/experiments/embedding/results/dotnet-yarp.json"

echo "== Verification A: conformance harness through .NET + YARP (:$PORT) =="
start_app "$PORT" "$LOG"
echo "  example app pid=$APP_PID  log=$LOG"
trap 'kill -TERM "$APP_PID" 2>/dev/null || true; wait "$APP_PID" 2>/dev/null || true' EXIT

wait_ready "$PORT"
EMB_PID=$(embedded_pid "$LOG")
echo "  embedded ably-server pid=$EMB_PID"

echo "  running harness (all six scenarios)..."
"$HARNESS" --port "$PORT" --label dotnet-yarp --json >"$RESULTS"
RC=$?
echo "  harness exit=$RC -> $RESULTS"
exit $RC
