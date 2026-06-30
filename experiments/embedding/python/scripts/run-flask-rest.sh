#!/usr/bin/env bash
# Demonstrate the Flask (WSGI) track: REST proxying WORKS, the realtime
# WebSocket upgrade does NOT (WSGI has no upgrade hook). Captures real output
# to results/python-flask-rest.log. Also checks clean shutdown (no orphan).
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

PORT="${1:-8588}"
LOG="$EMBED_DIR/results/python-flask-rest.log"
APP_LOG="$(mktemp)"
cd "$PY_DIR"
exec >"$LOG" 2>&1

child_pid() { pgrep -f "ably-server --mode memory --listen 127.0.0.1:" | head -1; }

echo "== Flask (WSGI) embedding: REST works, WS does not =="
echo "date: $(date -u +%FT%TZ)  publicPort: $PORT"
echo

ABLY_PUBLIC_PORT="$PORT" "$VENV_PY" examples/flask_app.py >"$APP_LOG" 2>&1 &
APP_PID=$!
echo "flask app pid: $APP_PID"
wait_ready "http://127.0.0.1:$PORT/readyz" || { echo "flask never ready"; cat "$APP_LOG"; exit 1; }
echo "app /readyz green via Flask proxy"
echo

echo "-- REST through Flask (proxied to the embedded child) --"
echo -n "GET /time            -> HTTP "
curl -s -u app.key:secret -o /dev/null -w "%{http_code}\n" "http://127.0.0.1:$PORT/time"
echo -n "POST publish         -> HTTP "
curl -s -u app.key:secret -H 'content-type: application/json' \
  -d '{"name":"ev","data":"flask-rest-hi"}' \
  -o /tmp/pub.out -w "%{http_code}" "http://127.0.0.1:$PORT/channels/flaskdemo/messages"
echo "  body: $(cat /tmp/pub.out)"
echo -n "GET history          -> HTTP "
curl -s -u app.key:secret -o /tmp/hist.out -w "%{http_code}" \
  "http://127.0.0.1:$PORT/channels/flaskdemo/messages?limit=5"
echo "  body: $(cat /tmp/hist.out)"
echo

echo "-- WebSocket upgrade through Flask (the WSGI limitation) --"
echo "Attempting an Upgrade: websocket handshake to ws://127.0.0.1:$PORT/ ..."
curl -s -i -N \
  -H "Connection: Upgrade" -H "Upgrade: websocket" \
  -H "Sec-WebSocket-Version: 13" \
  -H "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==" \
  "http://127.0.0.1:$PORT/?key=app.key:secret&format=json" 2>&1 | head -1
echo "(WSGI cannot complete a WS upgrade in-process: Werkzeug's dev server"
echo " rejects the Upgrade request outright; even a production WSGI server"
echo " has no PEP-3333 hook to hijack the socket. The realtime transport"
echo " therefore does NOT work through Flask — use the FastAPI/ASGI example"
echo " or an ASGI server / ingress in front for the WS path.)"
echo

echo "-- Clean shutdown (SIGTERM) --"
CHILD="$(child_pid)"
echo "embedded child before SIGTERM: $CHILD"
kill -TERM "$APP_PID"
wait "$APP_PID" 2>/dev/null; echo "flask app exited"
sleep 0.5
if kill -0 "$CHILD" 2>/dev/null; then
  echo "FAIL: orphaned child $CHILD still alive"
  kill -9 "$CHILD" 2>/dev/null
  exit 1
fi
echo "embedded child $CHILD gone — no orphan (signal handler stopped it)"
echo
echo "== FLASK REST-ONLY DEMO COMPLETE (REST works; WS unsupported by WSGI) =="
