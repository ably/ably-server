#!/usr/bin/env bash
# Run the Go conformance harness through the FastAPI proxy.
#   ./scripts/run-conformance.sh [port]            # root catch-all  -> python-fastapi.json
#   ./scripts/run-conformance.sh [port] /ably      # subpath mount   -> python-subpath.json
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

PORT="${1:-8581}"
BASE="${2:-}"
HARNESS="$EMBED_DIR/harness/harness"
if [[ -n "$BASE" ]]; then
  LABEL="python-subpath"; OUT="$EMBED_DIR/results/python-subpath.json"
  READY_URL="http://127.0.0.1:$PORT$BASE/readyz"
else
  LABEL="python-fastapi"; OUT="$EMBED_DIR/results/python-fastapi.json"
  READY_URL="http://127.0.0.1:$PORT/readyz"
fi
APP_LOG="$(mktemp)"; export APP_LOG
cd "$PY_DIR"

APP_PID="$(start_app "$PORT" "$BASE")"
trap 'kill -TERM "$APP_PID" 2>/dev/null || true; wait "$APP_PID" 2>/dev/null || true' EXIT
wait_ready "$READY_URL"

set +e
if [[ -n "$BASE" ]]; then
  "$HARNESS" --port "$PORT" --base-path "$BASE" --label "$LABEL" --json > "$OUT"
else
  "$HARNESS" --port "$PORT" --label "$LABEL" --json > "$OUT"
fi
RC=$?
set -e
echo "harness exit: $RC -> $OUT"
exit $RC
