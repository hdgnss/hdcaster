# syntax=docker/dockerfile:1.7
FROM --platform=$BUILDPLATFORM golang:1.22-bookworm AS build
WORKDIR /src

COPY go.mod go.sum* ./
COPY cmd ./cmd
COPY internal ./internal

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_TIME=unknown

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} go build \
      -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.buildTime=${BUILD_TIME}" \
      -o /out/hdcaster ./cmd/hdcaster

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates sqlite3 tzdata \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --create-home --home-dir /var/lib/hdcaster --shell /usr/sbin/nologin hdcaster \
    && mkdir -p /var/lib/hdcaster /etc/hdcaster \
    && chown -R hdcaster:hdcaster /var/lib/hdcaster /etc/hdcaster

COPY --from=build /out/hdcaster /usr/local/bin/hdcaster

USER hdcaster
WORKDIR /var/lib/hdcaster

EXPOSE 2101 8080

ENTRYPOINT ["/usr/local/bin/hdcaster"]
CMD ["-state", "/var/lib/hdcaster/state.db", "-ntrip-addr", "0.0.0.0:2101", "-admin-addr", "0.0.0.0:8080"]
