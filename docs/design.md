# HDCaster Design

## Goals

HDCaster is a lightweight NTRIP caster designed for low-memory VPS deployments. It targets:

- NTRIP Rev1 and Rev2 support
- Relay mode for forwarding an upstream NTRIP stream into a local mountpoint
- Single self-contained Go binary
- Built-in management UI
- Runtime quotas and IP blocking
- Online source and mountpoint visibility
- Source metadata and RTCM stream introspection
- No dependency on sibling repositories

## Technology Choice

Go is selected because it offers:

- Small operational footprint for a single binary service
- Good concurrency model for long-lived TCP streams
- Clean standard-library support for HTTP, TCP, SQLite orchestration, and embed
- Straightforward deployment on Linux VPS

The current implementation uses only Go standard library code in `hdcaster`. SQLite persistence is provided through the system `sqlite3` CLI so no third-party Go driver is required inside the repository.
The active schema is SQLite-only and relation-oriented; removed JSON-backed layouts and earlier experimental SQLite layouts are not treated as compatibility targets.

## Architecture

The service is split into three cooperating planes:

1. NTRIP caster plane
   - Listens on the caster port
   - Handles Rev1 `GET` and `SOURCE`
   - Handles Rev2 `GET` and `POST`
   - Streams RTCM bytes from one source to many clients
   - Tracks live source/client sessions
   - Derives mountpoint runtime stats

2. Relay plane
   - Resolves a configured relay to a local mountpoint
   - Maintains an optional upstream account pool per relay
   - Binds clients to shared upstream sessions from the first valid GGA
   - Reuses an upstream session for nearby clients within the configured radius, default 30 km
   - Leases additional upstream accounts when cluster distance or slot pressure requires another session
   - Injects optional static or client-derived GGA sentences at the configured interval
   - Tracks relay state, last error, connect/disconnect timestamps, pool usage, and shared session counts

3. Admin plane
   - Serves the built-in web UI
   - Exposes JSON APIs under `/api/v1`
   - Supports admin login, user management, block IP rules, mountpoint metadata, relay CRUD, and runtime limits

## Relay Architecture

The relay feature is now client-location aware:

- Each relay still owns one local mountpoint and one upstream endpoint definition.
- A relay may define an account pool instead of a single upstream credential.
- The first client is accepted immediately and remains pending until a valid GGA arrives or a static fallback GGA is configured.
- Clients within the configured radius, default 30 km, are reused onto an existing shared upstream session.
- Each shared upstream session can host multiple location slots, default 2, before the manager leases another upstream account and creates another session.
- When the last client leaves a shared upstream session, the leased account is released immediately.
- If a relay config is updated or disabled, the manager cancels the existing shared sessions and rebuilds runtime state from storage.

See `docs/relay.md` for the detailed module and lifecycle split.

## Core Modules

- `cmd/hdcaster`: process entrypoint
- `internal/app`: dependency wiring and service lifecycle
- `internal/ntrip`: NTRIP request parsing, session handling, RTCM inspection
- `internal/runtime`: in-memory live state and fan-out
- `internal/relay`: upstream account pool, client binding, GGA parsing, cluster reuse, and shared session lifecycle
- `internal/api`: admin API and session-authenticated handlers
- `internal/storage`: SQLite persistence layer and runtime history storage
- `internal/model`: shared domain types
- `internal/security`: password hashing helpers and admin sessions
- `web`: embedded static admin frontend
- `deploy`: deployment assets for `Docker` and reverse proxies

## Frontend / Backend Responsibilities

- The frontend should own presentation, filtering, and form collection for relay configuration and operational state.
- The backend owns validation, persistence, auth checks, relay startup and shutdown, and all protocol-level behavior.
- The frontend should never implement relay protocol behavior itself; it should only call the API and render the current status returned by the backend.
- Relay status is derived from backend state, so the UI can show the authoritative connect state, error text, and timestamps without guessing.
- Relay runtime also exposes retry/backoff timing and per-account health so operators can see why an upstream session is unavailable and which pooled credential is failing.

## Protocol Scope

### Supported flows

- Rev1 sourcetable request: `GET /`
- Rev1 client subscribe: `GET /mount`
- Rev1 source publish: `SOURCE password /mount`
- Rev2 sourcetable request: `GET /` with or without `Ntrip-Version: Ntrip/2.0`
- Rev2 client subscribe: `GET /mount`
- Rev2 source publish: `POST /mount`

### Response behavior

- Rev1 client success returns `ICY 200 OK`
- Rev1 sourcetable returns `SOURCETABLE 200 OK`
- Rev2 success returns normal HTTP response headers
- Auth failures return Rev1/Rev2 appropriate errors

## Authentication Model

- `admin` users log in to the web backend with salted password hashes
- `source` users have write permissions to one or more mountpoints
- `client` users have read permissions to one or more mountpoints
- Wildcard `*` mount permission is supported

Relay credentials are stored with the relay configuration and are only used by the backend when it opens the upstream connection.

## Mountpoint Metadata

Each mountpoint stores editable metadata such as:

- Display name
- Coordinates / location
- Supported constellations
- Advertised RTCM message types
- Notes / description
- Whether it should appear in sourcetable

Runtime stream inspection augments this with:

- Online/offline
- Bytes in/out
- Source IP
- Client count
- Observed RTCM3 message types
- Candidate constellation set inferred from RTCM messages
- MSM family summaries grouped by constellation/system
- Reference-station metadata decoded from `1005/1006/1007/1008/1033`
- Last activity time

Relay-backed mountpoints also inherit a backend-managed description so it is clear that the local mount is being populated by a relay entry.

## RTCM Decode Strategy

To stay lightweight, the initial implementation performs a focused RTCM3 scan:

- Detect frame preamble `0xD3`
- Parse frame length
- Extract RTCM message number from payload bits 0..11
- Count observed message numbers
- Infer candidate constellations using common message-number ranges

This stays lightweight while still decoding the operationally valuable subset:

- `1005/1006` reference-station position, flags, and antenna height
- `1007/1008/1033` antenna and receiver descriptors
- MSM family summaries derived from observed MSM message groups

## MVP Scope

The current relay scope focuses on a practical shared-session slice:

- One relay entry per local mountpoint.
- One upstream endpoint definition per relay, with either a singleton credential or an account pool.
- On-demand binding when the first local client arrives.
- Client GGA parsing into LLA and ECEF.
- 30 km style cluster reuse with configurable radius and slot count.
- Multi-upstream fan-out through a shared-session manager inside the process.
- Rev1 or Rev2 upstream fetches.
- Relay status reporting through the admin API and UI.

Anything beyond that, such as weighted account scheduling, cross-process pooling, or full slot-specific downstream caching, is intentionally out of scope for this pass.

## Known Limitations

- No relay clustering or HA story; the process is the single relay coordinator.
- No weighted or latency-aware account scheduling.
- No background reconnect loop independent of demand.
- Slot-specific downstream cache separation is simpler than commercial `cors-relay`.
- Relay sessions are rebuilt from stored config and current mount state, not from a distributed control plane.
- Storage schema evolution currently prefers forward cleanup over legacy-database compatibility.

## Acceptance Plan

See `docs/acceptance.md`.
