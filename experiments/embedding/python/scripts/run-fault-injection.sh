#!/usr/bin/env bash
# Fault injection (EMBEDDING-POC.md §7 reliability), RUN not described:
#   C1: kill -9 the embedded child -> supervisor respawns (new PID, /readyz
#       green) -> a fresh harness (connect,pubsub) PASSES through the proxy.
#   C2: SIGTERM the app -> clean exit (code 0), child gone (no orphan).
# All output is tee'd to results/python-fault-injection.log.
#
# The host app is started with a plain `&` in THIS shell (a direct child) so
# `wait` can read its exit code for C2. No nested subshells / pipes that would
# break job control.
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

PORT="${1:-8585}"
HARNESS="$EMBED_DIR/harness/harness"
LOG="$EMBED_DIR/results/python-fault-injection.log"
APP_LOG="$(mktemp)"
cd "$PY_DIR"

# Write everything to the log file with a plain redirect (no process
# substitution / tee — that interferes with job control on shutdown).
exec >"$LOG" 2>&1

child_pid() { pgrep -f "ably-server --mode memory --listen 127.0.0.1:" | head -1; }

echo "== Python embedding fault injection =="
echo "date: $(date -u +%FT%TZ)  publicPort: $PORT"
echo

# Start the host app as a DIRECT child of this shell (so `wait` reads its exit
# code in C2). run_fastapi.py exits 0 on SIGTERM after a graceful shutdown.
ABLY_PUBLIC_PORT="$PORT" ABLY_BASE_PATH="" \
  "$VENV_PY" examples/run_fastapi.py > "$APP_LOG" 2>&1 &
APP_PID=$!
echo "app (uvicorn) pid: $APP_PID"
wait_ready "http://127.0.0.1:$PORT/readyz" || { echo "app never ready"; cat "$APP_LOG"; exit 1; }
echo "app /readyz green via proxy"

# ---- C1: kill -9 the embedded child ------------------------------------
echo
echo "-- C1: kill -9 the embedded ably-server child --"
OLD_PID="$(child_pid)"
echo "embedded child pid (before): ${OLD_PID:-<none>}"
kill -9 "$OLD_PID"
echo "sent SIGKILL to $OLD_PID"

NEW_PID=""
for _ in $(seq 1 80); do
  sleep 0.25
  CUR="$(child_pid)"
  if [[ -n "$CUR" && "$CUR" != "$OLD_PID" ]] \
     && curl -fsS "http://127.0.0.1:$PORT/readyz" >/dev/null 2>&1; then
    NEW_PID="$CUR"; break
  fi
done
if [[ -z "$NEW_PID" ]]; then
  echo "C1 FAIL: supervisor did not respawn a healthy child"
  kill -TERM "$APP_PID" 2>/dev/null; exit 1
fi
echo "respawned child pid (after): $NEW_PID  (old $OLD_PID dead, /readyz green)"

echo "running harness --scenarios connect,pubsub through the proxy after respawn:"
"$HARNESS" --port "$PORT" --label python-postkill --scenarios connect,pubsub \
  --json > "$EMBED_DIR/results/python-postkill.json"
C1RC=$?
echo "C1 harness exit: $C1RC"
if [[ "$C1RC" -ne 0 ]]; then echo "C1 FAIL"; kill -TERM "$APP_PID" 2>/dev/null; exit 1; fi
echo "C1 PASS: respawn + connect,pubsub OK"

# ---- C2: SIGTERM the app, assert clean exit and no orphan --------------
echo
echo "-- C2: SIGTERM the host app --"
CHILD_BEFORE_TERM="$(child_pid)"
echo "embedded child pid (before SIGTERM): $CHILD_BEFORE_TERM"
kill -TERM "$APP_PID"
wait "$APP_PID"; APPRC=$?
echo "app exit code: $APPRC"
sleep 0.5
if kill -0 "$CHILD_BEFORE_TERM" 2>/dev/null; then
  echo "C2 FAIL: orphaned ably-server child $CHILD_BEFORE_TERM still alive"
  kill -9 "$CHILD_BEFORE_TERM" 2>/dev/null; exit 1
fi
echo "embedded child $CHILD_BEFORE_TERM gone — no orphan"
if [[ "$APPRC" -ne 0 ]]; then echo "C2 FAIL: app exit $APPRC (expected 0)"; exit 1; fi
echo "C2 PASS: clean shutdown (exit 0), child reaped"

echo
echo "== FAULT INJECTION PASS (C1 + C2) =="
