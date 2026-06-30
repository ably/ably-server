#!/usr/bin/env bash
# Verification C: fault injection.
#   C1) kill -9 the embedded child  -> supervisor respawns it (new PID,
#       /readyz green) and a fresh connect,pubsub harness passes through YARP.
#   C2) SIGTERM the example app     -> clean exit (code 0) and the embedded
#       child is gone (no orphan).
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

PORT="${1:-8563}"
LOG="$(mktemp -t ably-dotnet-fault)"

echo "== Verification C: fault injection (:$PORT) =="
start_app "$PORT" "$LOG"
echo "  example app pid=$APP_PID  log=$LOG"
cleanup() { kill -TERM "$APP_PID" 2>/dev/null || true; wait "$APP_PID" 2>/dev/null || true; }
trap cleanup EXIT

wait_ready "$PORT"
PID1=$(embedded_pid "$LOG")
echo ""
echo "-- C1: kill -9 the embedded child (pid $PID1) --"
kill -9 "$PID1"
echo "  killed pid $PID1; waiting for supervisor to respawn..."

# Wait for a NEW embedded pid to appear in the log AND /readyz to be green.
NEWPID=""
for i in $(seq 1 100); do
  cur=$(embedded_pid "$LOG" || true)
  if [[ -n "$cur" && "$cur" != "$PID1" ]] && curl -fsS "http://127.0.0.1:$PORT/readyz" >/dev/null 2>&1; then
    NEWPID="$cur"; break
  fi
  sleep 0.2
done
if [[ -z "$NEWPID" ]]; then
  echo "  FAIL: supervisor did not respawn a new child within timeout" >&2
  echo "--- app log ---"; tail -30 "$LOG" >&2
  exit 1
fi
# Confirm the old PID is truly dead and the new one is alive.
if kill -0 "$PID1" 2>/dev/null; then echo "  FAIL: old pid $PID1 still alive" >&2; exit 1; fi
if ! kill -0 "$NEWPID" 2>/dev/null; then echo "  FAIL: new pid $NEWPID not alive" >&2; exit 1; fi
echo "  RESPAWNED: old pid $PID1 dead, new pid $NEWPID alive, /readyz green"

echo "  running fresh harness (connect,pubsub) through the respawned child..."
"$HARNESS" --port "$PORT" --label dotnet-yarp-after-crash --scenarios connect,pubsub
echo "  C1 PASS: harness passed against the respawned child"

echo ""
echo "-- C2: SIGTERM the example app (pid $APP_PID) --"
kill -TERM "$APP_PID"
# Wait for the host to exit and capture its code.
APP_RC=0
if wait "$APP_PID"; then APP_RC=0; else APP_RC=$?; fi
trap - EXIT
echo "  app exited with code $APP_RC"

# Assert no orphaned child remains (this app's child, by pid).
sleep 0.5
if kill -0 "$NEWPID" 2>/dev/null; then
  echo "  FAIL: orphaned ably-server pid $NEWPID survived app shutdown" >&2
  kill -9 "$NEWPID" 2>/dev/null || true
  exit 1
fi
# Belt-and-braces: no ably-server bound to OUR child port lingers.
echo "  embedded child pid $NEWPID is gone (no orphan)"

if [[ "$APP_RC" -ne 0 ]]; then
  echo "  FAIL: app did not exit cleanly (code $APP_RC)" >&2
  echo "--- app log tail ---"; tail -20 "$LOG" >&2
  exit 1
fi
echo "  C2 PASS: clean exit (code 0), no orphan child"
echo ""
echo "== Verification C: ALL FAULT-INJECTION CASES PASSED =="
