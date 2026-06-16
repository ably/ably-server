# syntax=docker/dockerfile:1

# Build a static ably-server binary.
FROM golang:1.25-alpine AS build
WORKDIR /src

# Download modules first so the layer caches across source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/ably-server ./cmd/ably-server

# Minimal runtime. alpine ships busybox wget, which the compose
# healthcheck uses to poll /healthz.
FROM alpine:3.20
COPY --from=build /out/ably-server /usr/local/bin/ably-server
EXPOSE 8080
ENTRYPOINT ["ably-server"]
