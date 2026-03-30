# HDCaster Acceptance

## Functional Acceptance

1. Rev1 source can publish using `SOURCE password /MOUNT`.
2. Rev1 client can subscribe using `GET /MOUNT HTTP/1.0`.
3. Rev2 source can publish using `POST /MOUNT` with `Ntrip-Version: Ntrip/2.0`.
4. Rev2 client can subscribe using `GET /MOUNT` with `Ntrip-Version: Ntrip/2.0`.
5. `GET /` returns a valid sourcetable for both Rev1 and Rev2 style requests.
6. Admin can add, update, and delete client/source users from the web API.
7. Admin can change runtime limits for max sources and max clients.
8. Admin can add and remove blocked IP rules.
9. Admin UI shows online sources and mountpoint details.
10. Runtime stats show traffic counters and observed RTCM message types.
11. The current SQLite schema can persist and reload all configured entities in a freshly initialized database.
12. Relay entries can be created, updated, enabled, disabled, and deleted through the admin API.
13. A relay to an upstream caster becomes online when the first local client subscribes to its local mountpoint.
14. Relay status reports the upstream target, the last successful connect time, and any last error.
15. A relay can inject a valid static GGA sentence and periodically resend the latest valid GGA payload.
16. A relay-backed mountpoint is published through the normal NTRIP client path, so a client sees it as a regular local source.
17. A relay can define an upstream account pool and lease separate accounts to different shared sessions.
18. Clients whose first valid GGA is within the configured radius of an existing relay cluster reuse that upstream session.
19. Clients whose first valid GGA is outside the configured radius create or reuse another shared session backed by another pooled account.
20. When the last client leaves a shared relay session, the leased pooled account is released.
21. Mountpoint detail shows decoded reference-station metadata from `1005/1006/1007/1008/1033`, including antenna and receiver information when present.
22. Mountpoint detail shows MSM family summaries grouped by constellation or system.
23. Relay status shows retry/backoff timing and per-account health so an operator can identify upstream failures and unhealthy accounts.

## Non-Functional Acceptance

1. Service starts from a single Go binary.
2. No runtime dependency on sibling repositories.
3. Admin UI is served by the binary itself.
4. Typical idle memory profile remains suitable for low-memory VPS use.
5. Relay startup and shutdown stay inside the same process and do not require an external worker.
6. Repository includes deployment assets for Linux service management and container packaging.
7. Relay failures remain observable while retrying and do not disappear as silent session drops.

## Suggested Manual Verification

1. Start `hdcaster` with default config.
2. Log into admin UI with the bootstrap admin account.
3. Create one source user, one client user, and one mountpoint.
4. Publish RTCM bytes over Rev1 and verify online source appears in UI.
5. Connect a Rev1 client and verify streamed bytes are received.
6. Repeat publish/subscribe with Rev2.
7. Block the source IP and verify new connections are rejected.
8. Set max sources or clients to `0` or `1` and verify quota enforcement.
9. Confirm sourcetable reflects configured public mountpoints.
10. Create a relay through `POST /api/v1/relays` that points at a reachable upstream caster.
11. Confirm the relay-backed local mount appears in the mount list, with a backend-managed description.
12. Subscribe a local client to the relay mount and verify the relay transitions from idle or connecting to online.
13. Confirm the upstream bytes are forwarded to the client and the relay status shows timestamps and no error.
14. Update the relay to use an invalid password or unreachable host and verify the status reports an error on the next connect attempt.
15. Disable the relay and confirm the relay session stops and the local mount no longer reconnects while disabled.
16. Re-enable the relay and confirm it can reconnect on demand.
17. Configure a relay with at least two upstream accounts in its pool.
18. Connect two local clients with nearby GGA positions and verify the relay status shows one shared session and one leased account.
19. Connect another local client with a GGA position farther than 30 km and verify the relay status shows another shared session and a second leased account.
20. Disconnect all clients from one cluster and verify the leased account count drops after the shared session is torn down.
21. Bring up the provided `docker-compose.yml` file and verify the container starts automatically after reboot.
22. Build the provided `Dockerfile` and verify `/healthz`, `/readyz`, and `/version` respond inside the container.
23. Feed a stream containing `1005/1006/1033` and verify mountpoint detail shows decoded位置、天线和接收机信息。
24. Feed MSM messages and verify mountpoint detail shows grouped MSM family summaries.
25. Point a relay at an unreachable or unauthorized upstream and verify the admin UI shows retry/backoff and unhealthy account details.

## Automated Verification

- Unit tests for NTRIP request parsing
- Unit tests for RTCM frame scanning
- Unit tests for SQLite store and password helpers
- Unit tests for SQLite runtime history and backup
- Unit tests for relay request building, response parsing, account-pool binding, and cluster reuse decisions
- Schema verification that normalized SQLite tables are present and legacy JSON columns are absent from newly initialized databases
