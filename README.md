# HDCaster

Lightweight modern NTRIP caster implemented entirely inside `hdcaster`.

## What is implemented

- NTRIP Rev1 client/source flows
- NTRIP Rev2 client/source flows
- Built-in admin web UI
- Client/source user management
- Block IP / CIDR rules
- Mountpoint metadata management
- Online source and client visibility
- Runtime limits for max sources and max clients
- RTCM3 message scanning for message-type and constellation candidates
- RTCM metadata decode for `1005/1006/1007/1008/1033` plus MSM family summaries
- SQLite-only persistence backend


## Quick start

```bash
cd hdcaster
go build ./cmd/hdcaster
./hdcaster -state ./state.db -ntrip-addr :2101 -admin-addr :8080
```

For the current SQLite-only release, use a freshly initialized `state.db`.
Older experimental SQLite layouts and removed JSON-backed layouts are not a compatibility target for this branch.

## Operations

After logging into the admin UI, you can:

- click `Backup SQLite` to download a physical SQLite backup

API endpoints:

- `GET /healthz`
- `GET /readyz`
- `GET /version`
- `GET /api/v1/audit`
- `GET /api/v1/system/backup.sqlite3`
- `GET /api/v1/mounts/{mount}/history`

`hdcaster` records online source runtime snapshots every 15 seconds and stores recent history in SQLite for mountpoint detail queries.
Mountpoint detail pages include decoded reference-station metadata, MSM classifications, and richer relay runtime health.

Default bootstrap admin:

- username: `admin`
- password: `admin123456`

Open:

- Admin UI: `http://127.0.0.1:8080`
- NTRIP caster: `127.0.0.1:2101`

## Deployment

Deployment assets are included in-repo:

- `Dockerfile`
- `docker-compose.yml`
- `docs/deploy.md`

Recommended production layout:

- expose `2101/tcp` directly for NTRIP traffic
- keep `8080/tcp` behind `nginx` or `caddy`
- store SQLite at `/var/lib/hdcaster/state.db`
- install `sqlite3` and `ca-certificates` on the host

Example versioned build:

```bash
cd hdcaster
VERSION=$(git describe --always --dirty 2>/dev/null || echo dev)
COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

GOCACHE=/tmp/hdcaster-gocache go build \
  -ldflags="-X main.version=${VERSION} -X main.commit=${COMMIT} -X main.buildTime=${BUILD_TIME}" \
  -o hdcaster ./cmd/hdcaster
```

For full deployment steps, see `docs/deploy.md`.

## Project layout

- `cmd/hdcaster`: binary entrypoint
- `internal/ntrip`: protocol parser and TCP server
- `internal/runtime`: in-memory online session fan-out
- `internal/api`: admin JSON API
- `internal/storage`: SQLite persistence layer
- `internal/model`: domain model
- `internal/security`: password hashing
- `internal/web`: embedded frontend handler and static admin assets
- `docs`: design and acceptance notes

## Verification

```bash
cd hdcaster
GOCACHE=/tmp/hdcaster-gocache go test ./...
GOCACHE=/tmp/hdcaster-gocache go build ./cmd/hdcaster
```

## Notes

- SQLite backend is implemented without third-party Go dependencies and uses the system `sqlite3` CLI at runtime.
- Runtime persistence is SQLite-only; JSON is no longer used as a storage backend.
- API transport remains JSON, but the storage layer no longer relies on JSON columns or `sqlite3 -json` output parsing.
