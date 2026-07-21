#!/usr/bin/env bash
#
# run.sh — run the ably-go integration suite against a local ably-server to
# gauge protocol compatibility, and emit a shared JSON report for compat-gate
# to diff against compat/known-failures/ably-go.toml.
#
# This runner lives in ably-server (not ably-go): ably-server's CI is the only
# caller. It drives a caller-supplied ably-go checkout (--sdk-dir), which needs
# only the inert ABLY_LOCAL_SANDBOX_URL support in its own test suite. See
# compat/README.md for the shared contract, verdict vocabulary, and policy.
#
# A running local sandbox is a prerequisite — the caller manages it, like a
# database. Start one in another terminal (or as a CI service), e.g.:
#
#     go run ./cmd/ably-local-sandbox --listen :9010
#
# and point this script at it with --url / ABLY_LOCAL_SANDBOX_URL (default
# http://localhost:9010). Each test process then provisions its OWN app through
# the local sandbox's /apps endpoint (which boots an isolated in-memory ably-server
# child seeded with the appspec's keys and presence fixtures, and returns that
# child's endpoint/port); see internal/ablytest's LocalSandboxURL hook. Because every
# test gets a fresh, isolated app, tests never contend over shared server state
# and can run concurrently.
#
# Every matched test runs in its own `go test` process, so a panic in one test
# (e.g. a nil deref against an unimplemented endpoint) doesn't abort the whole
# batch. Results are written as a shared JSON report (--out) for compat-gate,
# plus a text tally. Tests run across a worker pool (-j, default 8).
#
# Usage:
#   compat/runners/ably-go/run.sh [options] [-- extra go test args]
#
# Options:
#   -s, --sdk-dir DIR    path to the ably-go checkout to test
#                        (default $ABLY_GO_DIR or ../ably-go beside ably-server)
#   -u, --url URL        local sandbox base URL
#                        (default $ABLY_LOCAL_SANDBOX_URL or http://localhost:9010)
#   -r, --run REGEX      only run tests matching REGEX (go test -run)
#   -j, --jobs N         number of tests to run concurrently (default 8)
#       --retries N      re-run a timed-out test serially up to N times to rule
#                        out load starvation before trusting the timeout
#                        (default 1; 0 disables)
#   -t, --timeout DUR    per-test timeout (default 25s). Keep this above the
#                        tests' own internal waits (message delivery ~15s,
#                        ablytest.Timeout 30s for graceful closes) so a test that
#                        fails on its own does so cleanly; a test that hits this
#                        timeout is genuinely wedged.
#   -o, --out PATH       write the JSON report to PATH
#                        (default compat-results-ably-go.json in ably-server)
#   -h, --help           show this help
set -euo pipefail

# --- paths ------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVER_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# --- defaults ---------------------------------------------------------------
ABLY_GO_DIR="${ABLY_GO_DIR:-$SERVER_ROOT/../ably-go}"
LOCAL_SANDBOX_URL="${ABLY_LOCAL_SANDBOX_URL:-http://localhost:9010}"
RUN_REGEX=""
PER_TEST_TIMEOUT="25s"
JOBS=8
RETRIES=1
JSON_OUT="$SERVER_ROOT/compat-results-ably-go.json"
EXTRA_ARGS=()

usage() { awk 'NR==1{next} /^#/{sub(/^# ?/,""); print; next} {exit}' "${BASH_SOURCE[0]}"; }

# --- arg parsing ------------------------------------------------------------
while [[ $# -gt 0 ]]; do
	case "$1" in
	-s | --sdk-dir) ABLY_GO_DIR="$2"; shift 2 ;;
	-u | --url) LOCAL_SANDBOX_URL="$2"; shift 2 ;;
	-r | --run) RUN_REGEX="$2"; shift 2 ;;
	-j | --jobs) JOBS="$2"; shift 2 ;;
	--retries) RETRIES="$2"; shift 2 ;;
	-t | --timeout) PER_TEST_TIMEOUT="$2"; shift 2 ;;
	-o | --out) JSON_OUT="$2"; shift 2 ;;
	-h | --help) usage; exit 0 ;;
	--) shift; EXTRA_ARGS=("$@"); break ;;
	*) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
	esac
done

if [[ ! -d "$ABLY_GO_DIR" ]]; then
	echo "error: ably-go checkout not found at $ABLY_GO_DIR" >&2
	echo "       pass --sdk-dir DIR or set ABLY_GO_DIR." >&2
	exit 1
fi
ABLY_GO_DIR="$(cd "$ABLY_GO_DIR" && pwd)"

# --- check the local sandbox is reachable -----------------------------------------
# POST /stats is the local sandbox's cheapest always-on endpoint.
if ! curl -fsS -X POST "$LOCAL_SANDBOX_URL/stats" -d '[]' >/dev/null 2>&1; then
	echo "error: no local sandbox reachable at $LOCAL_SANDBOX_URL" >&2
	echo "       start one first, e.g.:" >&2
	echo "         (cd $SERVER_ROOT && go run ./cmd/ably-local-sandbox --listen :9010)" >&2
	echo "       then re-run, or point --url / ABLY_LOCAL_SANDBOX_URL at it." >&2
	exit 1
fi

RESULT_DIR="$(mktemp -d -t ably-compat.XXXXXX)"
cleanup() { rm -rf "$RESULT_DIR"; }
trap cleanup EXIT INT TERM

# --- run the tests -----------------------------------------------------------
# Each test process provisions a fresh app against the local sandbox.
export ABLY_LOCAL_SANDBOX_URL="$LOCAL_SANDBOX_URL"
cd "$ABLY_GO_DIR"

GO_TEST_BASE=(go test -tags=integration -count=1 ./ably/...)

LIST_REGEX="${RUN_REGEX:-.}"
echo ">> testing ably-go checkout at $ABLY_GO_DIR"
echo ">> using local sandbox at $LOCAL_SANDBOX_URL"
echo ">> enumerating integration tests"
mapfile -t TESTS < <("${GO_TEST_BASE[@]}" -list "$LIST_REGEX" 2>/dev/null | grep -E '^Test')
if [[ "${#TESTS[@]}" -eq 0 ]]; then
	echo "error: no matching tests found" >&2
	exit 1
fi
echo ">> ${#TESTS[@]} tests to run (per-test isolation, ${JOBS} workers, ${PER_TEST_TIMEOUT} each)"

# run_one_test runs a single test in its own process, capturing go test's JSON
# stream (for authoritative verdict/duration parsing later) plus rc, and prints
# a live status line. Always returns 0 so a failing test never aborts the pool.
run_one_test() {
	local idx="$1" t="$2"
	local jf="$RESULT_DIR/$idx.json"
	printf '%s' "$t" > "$RESULT_DIR/$idx.name"
	local rc=0
	"${GO_TEST_BASE[@]}" -json -run "^${t}\$" -timeout "$PER_TEST_TIMEOUT" \
		"${EXTRA_ARGS[@]}" > "$jf" 2>&1 || rc=$?
	echo "$rc" > "$RESULT_DIR/$idx.rc"

	local kind
	if grep -q 'panic: test timed out' "$jf"; then kind=TIMEOUT
	elif grep -q 'panic:\|fatal error:' "$jf"; then kind=PANIC
	elif [[ "$rc" -eq 0 ]]; then kind=PASS
	else kind=FAIL; fi
	printf '%-7s %s\n' "$kind" "$t"
}

i=0
for t in "${TESTS[@]}"; do
	run_one_test "$i" "$t" &
	i=$((i + 1))
	# Throttle to at most JOBS concurrent workers.
	while [[ "$(jobs -r -p | wc -l)" -ge "$JOBS" ]]; do wait -n || true; done
done
wait

# Retry timeouts serially. A test can hit its per-test timeout not because it is
# wedged but because the machine was saturated — under -j workers each running a
# test that provisions its own ably-server child, a timing-sensitive test may be
# starved past the deadline. Re-running each timed-out test on its own (nothing
# else in flight) tells the two apart: a load flake now completes, while a test
# that is genuinely stuck on server behaviour that never arrives times out again.
for _ in $(seq 1 "$RETRIES"); do
	retry_idxs=()
	for nf in "$RESULT_DIR"/*.name; do
		[[ -e "$nf" ]] || continue
		idx="$(basename "$nf" .name)"
		grep -q 'panic: test timed out' "$RESULT_DIR/$idx.json" 2>/dev/null && retry_idxs+=("$idx")
	done
	[[ "${#retry_idxs[@]}" -eq 0 ]] && break
	echo ">> retrying ${#retry_idxs[@]} timed-out test(s) serially to rule out load starvation"
	for idx in "${retry_idxs[@]}"; do
		run_one_test "$idx" "$(cat "$RESULT_DIR/$idx.name")"
	done
done

# --- aggregate results -------------------------------------------------------
# A single pass over every test's captured go test -json stream produces both
# the shared JSON report (metadata + a per-test results array) and the text
# tally. The verdict vocabulary is pass/fail/panic/timeout/skip; this runner
# deliberately knows nothing about which failures are "expected" — that policy
# lives in compat-gate, which diffs this report against its known-failures list.
SDK_REV="$(git -C "$ABLY_GO_DIR" rev-parse HEAD 2>/dev/null || echo '')"
python3 - "$RESULT_DIR" "$JSON_OUT" "$SDK_REV" "$LOCAL_SANDBOX_URL" "$JOBS" <<'PY'
import datetime, json, os, sys

result_dir, json_out, sdk_rev, sandbox_url, jobs = sys.argv[1:6]

def classify(test, raw, rc):
    action = elapsed = pkg_elapsed = None
    out = []
    for line in raw.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            out.append(line)  # non-JSON (e.g. a build error)
            continue
        try:
            ev = json.loads(line)
        except ValueError:
            out.append(line)
            continue
        a = ev.get("Action")
        if a == "output":
            out.append(ev.get("Output", ""))
        if ev.get("Test") == test and a in ("pass", "fail", "skip"):
            action = a
            if ev.get("Elapsed") is not None:
                elapsed = ev["Elapsed"]
        elif ev.get("Test") is None and a in ("pass", "fail"):
            if ev.get("Elapsed") is not None:
                pkg_elapsed = ev["Elapsed"]
    text = "".join(out)
    if "panic: test timed out" in text:
        verdict = "timeout"
    elif "panic:" in text or "fatal error:" in text:
        verdict = "panic"
    elif action in ("pass", "fail", "skip"):
        verdict = action
    else:
        verdict = "pass" if rc == 0 else "fail"
    # On panic/timeout the test's own Elapsed is absent; fall back to the
    # package elapsed so a duration is always reported.
    duration = elapsed if elapsed is not None else (pkg_elapsed or 0.0)
    return verdict, duration

results = []
idx = 0
while os.path.exists(os.path.join(result_dir, f"{idx}.name")):
    with open(os.path.join(result_dir, f"{idx}.name")) as f:
        test = f.read()
    with open(os.path.join(result_dir, f"{idx}.json")) as f:
        raw = f.read()
    rc_path = os.path.join(result_dir, f"{idx}.rc")
    rc = int(open(rc_path).read().strip() or "0") if os.path.exists(rc_path) else 1
    verdict, duration = classify(test, raw, rc)
    results.append({"name": test, "verdict": verdict, "duration": round(duration, 3)})
    idx += 1

results.sort(key=lambda r: r["name"])

tally = {}
for r in results:
    tally[r["verdict"]] = tally.get(r["verdict"], 0) + 1

report = {
    "generatedAt": datetime.datetime.now(datetime.timezone.utc)
        .isoformat(timespec="seconds").replace("+00:00", "Z"),
    "sdk": "ably-go",
    "sdkRev": sdk_rev or None,
    "localSandboxURL": sandbox_url,
    "jobs": int(jobs),
    "transports": None,
    "complete": True,
    "tally": tally,
    "results": results,
}
with open(json_out, "w") as f:
    json.dump(report, f, indent=2)
    f.write("\n")

print()
print("================ compat summary ================")
for verdict in ("pass", "fail", "panic", "timeout", "skip"):
    if verdict in tally:
        print(f"{verdict.upper():7s} {tally[verdict]}")
print("===============================================")
for verdict in ("fail", "panic", "timeout"):
    for r in results:
        if r["verdict"] == verdict:
            print(f"{verdict.upper():7s} {r['name']}")
print(f">> wrote JSON report to {json_out}")

# A completed run exits 0 regardless of individual test verdicts: whether the
# results are acceptable is decided by compat-gate diffing this report against
# its known-failures list, not by this policy-free runner.
sys.exit(0)
PY
