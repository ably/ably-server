# syntax=docker/dockerfile:1

# Build a static ably-server binary.
FROM golang:1.26-alpine AS build
WORKDIR /src

# The shared protocol module is in a private repository, so the build fetches
# it over SSH, authenticated by the host's agent: this image builds with
# `docker build --ssh default` — or `mise run image` — and not without. Only
# that repository is named, so every other module still comes from the proxy.
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
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/ably-server ./cmd/ably-server

# Minimal runtime. alpine ships busybox wget, which the compose
# healthcheck uses to poll /healthz.
FROM alpine:3.20
COPY --from=build /out/ably-server /usr/local/bin/ably-server
EXPOSE 8080
ENTRYPOINT ["ably-server"]
