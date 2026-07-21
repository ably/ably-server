# CLAUDE.md

Guidance for agents working in this repo. `ably-server` is a single-binary,
Ably-compatible realtime/REST server (see [README.md](README.md) for what it
is, [DESIGN.md](DESIGN.md) for the full spec, and
[CONTRIBUTING.md](CONTRIBUTING.md) for the contribution flow). DESIGN.md is
the durable spec anchor — cite it as `DESIGN.md §N`, and update it in the same
change when you alter documented behaviour.

## Build & test

    go build ./...
    go vet ./... && go vet -tags=integration ./...
    go test -race ./...                        # unit
    go test -tags=integration -race ./...      # integration
