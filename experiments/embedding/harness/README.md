# Embedding conformance harness

Language-agnostic conformance runner for the embedding PoC
([EMBEDDING-POC.md](../../../EMBEDDING-POC.md) §8). It drives an
Ably-compatible endpoint by host + port and asserts the behaviours the PoC
must prove through whatever sits in front of the embedded server: nothing
(the floor), a Go in-process mount (the ceiling), or a Node / .NET reverse
proxy.

It lives inside the `ably-server` Go module so the resume scenario can
speak the wire protocol directly via `internal/protocol`. It is a runnable
binary with no tests, so `go test ./...` only compiles it.

## Build

```sh
go build -o harness ./experiments/embedding/harness
```

## Run

```sh
# against a standalone server on :8431
./harness --port 8431 --label baseline --json
```

Exit code is `0` iff every selected scenario passes. Human-readable
progress goes to stderr; `--json` prints one JSON report line to stdout.

### Flags

| Flag | Default | Meaning |
|---|---|---|
| `--host` | `127.0.0.1` | endpoint host |
| `--port` | (required) | endpoint port |
| `--key` | `app.key:secret` (or `$ABLY_SERVER_API_KEY`) | API key `appId.keyId:keySecret` |
| `--label` | `unlabelled` | label recorded in the report |
| `--scenarios` | `all` | comma list: `connect,pubsub,restpubsub,history,resume,soak` |
| `--binary` | `false` | use the msgpack SDK protocol instead of JSON |
| `--latency-samples` | `20` | round-trips sampled for pub/sub latency |
| `--history-count` | `10` | messages published + read back in `history` |
| `--soak-messages` | `200` | messages for `soak` (0 disables) |
| `--op-timeout` | `10s` | per-operation timeout |

## Scenarios

| Scenario | What it proves | How |
|---|---|---|
| `connect` | unmodified ably-go SDK reaches CONNECTED | `ably.NewRealtime` + `Connect()` |
| `pubsub` | realtime publish→subscribe round-trip | SDK publish, await own echo; latency distribution |
| `restpubsub` | REST publish reaches a WS subscriber (cross-protocol) | REST POST, await on WS; REST→WS latency |
| `history` | SDK REST publish + ordered history read | publish N, `History()` forwards, assert count+order |
| `resume` | resume-after-drop, RESUMED flag, zero gap loss | raw WS: attach, capture cursor, drop, publish gap, re-attach with cursor, assert exactly the gap replays |
| `soak` | steady-state zero loss | publish M via REST to a WS subscriber, assert all once |

The `resume` scenario uses a raw WebSocket (gorilla/websocket +
`internal/protocol`) rather than the SDK so the drop is deterministic and
independent of any SDK's reconnect timing — and so it exercises the
server's `computeResumeReplay` path through a proxy identically to direct.

## Transport coverage

`ably-server` exposes **WebSocket** (`GET /`) and **REST** (`/channels/*`,
`/time`, `/healthz`, `/readyz`) only — there is **no comet/SSE fallback**
(`/comet`, `/sse` return 404). The cross-transport reliability leg is
therefore scoped to WebSocket + REST.
