## Contributing to ably-server

ably-server is an experimental implementation with no stability or support
guarantees — interfaces, wire coverage, and behaviour may change without
notice (see the [Status](./README.md#status) section of the README). Bug
reports, questions, and small fixes are welcome via
[GitHub Issues](https://github.com/ably/ably-server/issues) and pull
requests. For anything larger, please open an issue to discuss the approach
before investing in a change.

### Development flow

1. Fork `github.com/ably/ably-server`.
2. Add your fork as a remote: `git remote add fork git@github.com:your-username/ably-server`.
3. Create your feature branch: `git checkout -b my-new-feature`.
4. Make your change, with suitable tests (see below).
5. Commit your changes (`git commit -am 'Add some feature'`).
6. Push to the branch: `git push fork my-new-feature`.
7. Open a pull request against `main`.

[`DESIGN.md`](./DESIGN.md) is the durable spec — it describes the externally
observable protocol surface as well as the internals. If a change alters
behaviour that `DESIGN.md` documents, update the doc in the same PR.

### Building

Requires Go 1.26+ (the floor is pinned in `go.mod`).

```sh
go build ./...
```

### Tests

CI runs `build`, `vet`, and the test suites on every push and pull request.
Please run them locally before opening a PR; the checks are not required, so
verifying locally is what keeps `main` green.

```sh
# vet (both build configurations CI vets)
go vet ./...
go vet -tags=integration ./...

# unit tests
go test -race ./...

# integration tests (spin the binary up against real client flows)
go test -tags=integration -race ./...
```

The storage interface has an in-memory implementation exercised by the same
table-driven contract tests as the bbolt and Postgres backends, so most
storage behaviour is covered without external dependencies. See
[`DESIGN.md` §18](./DESIGN.md#18-testing-strategy) for the overall testing
strategy.

### SDK compatibility harnesses

The server's headline goal is that existing Ably SDKs connect unchanged. The
[`compat/`](./compat/README.md) harnesses drive real Ably SDK test suites
against a local server and gate the result against a checked-in list of known
failures. See [`compat/README.md`](./compat/README.md) for how to run them and
how the known-failures lists work.

### Running a local cluster

To exercise `cluster` mode (Postgres + several stateless nodes) locally:

```sh
docker compose up --build
```

See the [README](./README.md#local-cluster-docker-compose) for details.
