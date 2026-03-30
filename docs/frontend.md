# HD Caster Frontend

This document describes the embedded static admin UI served by `hdcaster`.

## Layout

- `internal/web/static/index.html`: single-page shell
- `internal/web/static/styles.css`: responsive visual system
- `internal/web/static/app.js`: state management, renderers, and API client
- `internal/web/handler.go`: embedded SPA file server

## Page Structure

The page is split into six visible zones:

1. Overview dashboard
2. Online source and mountpoint lists
3. Client/source user management
4. Block IP management
5. Runtime quota settings
6. Mountpoint detail panel

## API Contract

The frontend is written against `/api/v1/...` and falls back to local data when refresh requests fail.

Expected endpoints:

- `GET /api/v1/overview`
- `GET /api/v1/sources/online`
- `GET /api/v1/mounts`
- `GET /api/v1/mounts/{id}`
- `GET /api/v1/users`
- `POST /api/v1/users`
- `GET /api/v1/blocks`
- `POST /api/v1/blocks`
- `DELETE /api/v1/blocks/{ip}`
- `GET /api/v1/limits`
- `PUT /api/v1/limits`

## Notes

- No npm, node, or CDN dependencies are required.
- The UI is embedded into the Go binary through `//go:embed`.
- The mountpoint detail view includes traffic history, RTCM metadata, and relay runtime details.
