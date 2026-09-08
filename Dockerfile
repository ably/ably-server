# syntax=docker/dockerfile:1

# Two images are built from this file, from one shared build stage:
#
#   --target release  the artifact we publish and support, on scratch.
#                     Nothing but the binary: no shell, no package
#                     manager, no distribution packages, no CA bundle.
#                     Its SBOM is the binary's SBOM, and there is no
#                     operating system layer to carry CVEs of its own.
#   --target dev      a development image, on alpine, built locally by
#                     Docker Compose. Identical binary, on a base that
#                     has a shell and busybox wget — which is what the
#                     Compose healthcheck polls /readyz with, and what
#                     makes a container worth exec-ing into.
#
# release is last, so it is what a bare `docker build .` produces.

# Pinned to the patch release, not the 1.26 floating tag: the Go
# toolchain determines the stdlib version linked into the binary, which
# is a component in its own right in the SBOM and a source of CVEs of
# its own. It should change because we changed it.
ARG GO_VERSION=1.26.8
ARG ALPINE_VERSION=3.20

# Cross-compiles rather than emulating: the binary is pure Go with
# CGO_ENABLED=0, so building linux/arm64 on an amd64 runner is the same
# work as building it natively, where QEMU would be many times slower.
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS build
WORKDIR /src

# The shared protocol module is in a private repository, so the build
# fetches it over SSH, authenticated by the host's agent: this image
# builds with `docker build --ssh default` — or `mise run image` — and
# not without. Only that repository is named, so every other module
# still comes from the proxy. All of this goes away when the module is
# published.
RUN apk add --no-cache git openssh-client \
 && mkdir -p -m 0700 ~/.ssh \
 && ssh-keyscan github.com >> ~/.ssh/known_hosts
ENV GOPRIVATE=github.com/ably/server-protocol
RUN git config --global \
    url."ssh://git@github.com/ably/server-protocol".insteadOf "https://github.com/ably/server-protocol"

# Download modules first so the layer caches across source changes.
COPY go.mod go.sum ./
RUN --mount=type=ssh go mod download

COPY . .

# The build context excludes .git, so the toolchain can stamp no VCS
# information of its own and the release identity has to be passed in.
# Left unset it reports "dev" with no commit, which is what an
# unstamped image should look like — recognisably not a release.
ARG VERSION=dev
ARG COMMIT=""
ARG TARGETOS
ARG TARGETARCH
RUN --mount=type=cache,target=/root/.cache/go-build,id=go-build-$TARGETOS-$TARGETARCH \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build \
      -trimpath \
      -ldflags="-s -w \
        -X github.com/ably/ably-server/internal/version.version=${VERSION} \
        -X github.com/ably/ably-server/internal/version.commit=${COMMIT}" \
      -o /out/ably-server ./cmd/ably-server

# -s -w takes about 30% off the binary and costs nothing that matters
# here. It drops the symbol table and DWARF, but not the pclntab, which
# is where both `govulncheck -mode=binary` and syft's symbol capture
# read the function names they need — so function-level reachability
# analysis on the shipped artifact is unaffected, as are the runtime's
# own stack traces. Measured: identical findings and an identical 471
# captured symbols, stripped or not.

# Labels are repeated per runtime stage because a stage inherits none
# from the one it copies from. They are what ties a pulled image back
# to the source revision it was built from, without running it.

FROM alpine:${ALPINE_VERSION} AS dev
ARG VERSION
ARG COMMIT
LABEL org.opencontainers.image.title="ably-server" \
      org.opencontainers.image.description="Ably-compatible realtime and REST server (development image)" \
      org.opencontainers.image.source="https://github.com/ably/ably-server" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}"
COPY --from=build /out/ably-server /usr/local/bin/ably-server
# Numeric, because there is no user to name: it matches the scratch
# image, where there is no /etc/passwd to name one in.
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/ably-server"]

FROM scratch AS release
ARG VERSION
ARG COMMIT
LABEL org.opencontainers.image.title="ably-server" \
      org.opencontainers.image.description="Ably-compatible realtime and REST server" \
      org.opencontainers.image.source="https://github.com/ably/ably-server" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}"
COPY --from=build /out/ably-server /usr/local/bin/ably-server
USER 65532:65532
EXPOSE 8080
# Absolute, because a scratch image has no PATH to resolve a bare name
# against.
ENTRYPOINT ["/usr/local/bin/ably-server"]
