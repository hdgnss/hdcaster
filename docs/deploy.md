# HDCaster Deployment

## Overview

`hdcaster` is designed for single-binary deployment on a small Linux VPS.
The only runtime dependency outside the binary is the system `sqlite3` CLI, which is used by the SQLite persistence layer.

This document covers:

- direct binary deployment
- docker compose deployment
- container deployment
- reverse proxy examples for the admin UI

## Ports

- NTRIP caster: `2101/tcp`
- Admin UI and JSON API: `8080/tcp`

Typical internet-facing deployment keeps:

- `2101/tcp` open for NTRIP clients and sources
- `8080/tcp` private behind `nginx` or `caddy`

## Build

```bash
cd hdcaster
VERSION=$(git describe --always --dirty 2>/dev/null || echo dev)
COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

GOCACHE=/tmp/hdcaster-gocache go build \
  -ldflags="-X main.version=${VERSION} -X main.commit=${COMMIT} -X main.buildTime=${BUILD_TIME}" \
  -o hdcaster ./cmd/hdcaster
```

## Direct Binary Deployment

1. Install runtime packages.

```bash
sudo apt-get update
sudo apt-get install -y sqlite3 ca-certificates
```

2. Create service user and directories.

```bash
sudo useradd --system --home-dir /var/lib/hdcaster --create-home --shell /usr/sbin/nologin hdcaster
sudo mkdir -p /etc/hdcaster
sudo chown -R hdcaster:hdcaster /var/lib/hdcaster /etc/hdcaster
```

3. Copy the binary.

```bash
sudo install -m 0755 ./hdcaster /usr/local/bin/hdcaster
```

4. Start it manually for a smoke test.

```bash
sudo -u hdcaster /usr/local/bin/hdcaster \
  -state /var/lib/hdcaster/state.db \
  -ntrip-addr 0.0.0.0:2101 \
  -admin-addr 127.0.0.1:8080
```

5. Verify readiness.

```bash
curl -s http://127.0.0.1:8080/healthz
curl -s http://127.0.0.1:8080/readyz
curl -s http://127.0.0.1:8080/version
```

## Docker Compose Deployment

A `docker-compose.yml` file is provided in the repository root.

Start the service in the background:

```bash
docker-compose up -d
```

The container reads runtime options from the environment variables or command-line flags. 
Common keys are:

```yaml
environment:
  - HDCASTER_STATE_PATH=/var/lib/hdcaster/state.db
  - HDCASTER_NTRIP_ADDR=0.0.0.0:2101
  - HDCASTER_ADMIN_ADDR=0.0.0.0:8080
  - HDCASTER_LOCAL_AUTH=true
```

Check status:

```bash
docker-compose ps
docker-compose logs -f hdcaster
```

## OIDC Environment

If you use Pocket ID, set these keys in your `.env` or `docker-compose.yml`:

```bash
HDCASTER_OIDC_POCKETID=true
HDCASTER_OIDC_ISSUER_URL=https://auth.example.com
HDCASTER_OIDC_CLIENT_ID=your-client-id
HDCASTER_OIDC_CLIENT_SECRET=your-client-secret
HDCASTER_OIDC_REDIRECT_URL=https://caster.example.com/api/v1/auth/oidc/callback
HDCASTER_OIDC_ALLOWED_EMAILS=admin@example.com,ops@example.com
HDCASTER_OIDC_ALLOWED_DOMAINS=example.com
```

If OIDC is disabled, local admin login remains available and bootstrap credentials are:

- username: `admin`
- password: `admin123456`

Change the bootstrap password immediately after first login.

## Reverse Proxy

These examples only proxy the admin UI and JSON API.
The NTRIP port should usually remain a direct TCP listener on `2101`.

## Docker

The repository contains a multi-stage `Dockerfile`.

Build:

```bash
cd hdcaster
docker build \
  --build-arg VERSION="$(git describe --always --dirty 2>/dev/null || echo dev)" \
  --build-arg COMMIT="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)" \
  --build-arg BUILD_TIME="$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
  -t hdcaster:local .
```

Run:

```bash
docker run --rm \
  -p 2101:2101 \
  -p 8080:8080 \
  -v hdcaster-data:/var/lib/hdcaster \
  hdcaster:local
```

## Upgrade Notes

- Stop the container before replacing the image.
- Keep a copy of `/var/lib/hdcaster/state.db`.
- Use the admin UI backup action or copy the database file directly while the container is stopped.
- Restart the container and verify `/readyz` returns `200`.

## Acceptance Checklist

- `hdcaster` starts under docker
- `/healthz`, `/readyz`, `/version` return valid responses
- admin UI loads through the reverse proxy
- NTRIP source and client traffic still works on `2101`
- SQLite backup can be downloaded from the admin UI
