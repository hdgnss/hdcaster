# HDCaster Relay

This document explains the relay-specific control flow inside `hdcaster`.

## Purpose

A relay lets HDCaster subscribe to an upstream NTRIP caster and republish that stream on a local mountpoint. From the point of view of local clients, the relay output behaves like any other mountpoint source.

## Flow

1. An admin saves a relay configuration through the API.
2. `internal/app` persists the relay and syncs relay state.
3. `internal/relay.Manager` indexes the relay by name and local mount.
4. When the first client asks for the local mount, the app asks the relay manager to connect.
5. The manager dials the upstream caster, sends an NTRIP request, validates the response, and registers the relay as a source in the runtime hub.
6. The relay session streams bytes from upstream to the local runtime until the upstream closes, the config changes, or the session is cancelled.
7. When the session ends, relay status is updated so the admin API can show the last error and timestamps.

## Module Split

- `internal/app`: persists relay config, synchronizes relay state, and bridges NTRIP requests to relay startup.
- `internal/relay`: owns upstream connections, session lifecycle, GGA injection, and relay status.
- `internal/runtime`: hosts the in-memory source/client fan-out used by both direct sources and relays.
- `internal/api`: exposes relay CRUD and enabled/disabled actions.
- `internal/storage`: persists relay definitions with the rest of the app config.

## API Surface

- `GET /api/v1/relays`
- `POST /api/v1/relays`
- `PUT /api/v1/relays/{name}/enabled`
- `DELETE /api/v1/relays/{name}`

## Operational Responsibilities

- The frontend renders relay configuration and status.
- The backend validates relay fields, starts and stops sessions, and reports status.
- The relay manager is the only component that talks to upstream casters.

## Practical Constraints

- Each relay points to exactly one local mountpoint and one upstream mountpoint.
- A relay is lazy-started by client demand rather than prewarmed.
- The relay session is deliberately lightweight and does not attempt protocol translation beyond the NTRIP request/response exchange.
- The manager treats disabled mounts and disabled relays as non-runnable and surfaces that in status.
