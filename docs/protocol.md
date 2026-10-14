# Engine wire protocol

Status: v2 (`PROTO_VERSION = 2`) implemented over ZeroMQ (`enginelib/src/wire.rs`,
`enginelib/src/transport/*`). The message set mirrors
`enginelib/src/nucleus/*` one-to-one. Items under **Planned** are not built yet.

## Roles

- **Server** (`engine` bin `server`) — owns a RocksDB store and the in-memory
  queues/leases (`ServerAPI`). Servers share no state.
- **Client** (`transport::client::Client`) — workers (`Lease`/`Renew`/
  `Complete`/`CancelLease`) and submitters/operators (`Query`). One protocol;
  the role is just which messages a connection sends.
- **Proxy** (`engine` bin `proxy`) — discovery only: tells a client which
  server to use. Never on the task hot path.

## Transport

ZeroMQ over TCP. ZeroMQ does the framing; each message is one
postcard-encoded envelope:

```
Envelope { req_id: u64, session: u128, body: Request | Response | ProxyRequest | ProxyResponse }
```

- Server: `ROUTER` bound on `host`. Clients: `DEALER`. Proxy: `ROUTER` bound on
  `proxy_host`; servers heartbeat to it from a `DEALER`.
- Requests are multiplexed per connection; the response echoes `req_id` and
  may arrive out of order (a waiting `Lease` never stalls a `Renew`).
- Addresses without a scheme are TCP: `[::1]:50051` → `tcp://[::1]:50051`.
  IPv6 is enabled on every socket.
- Messages larger than `max_message_bytes` drop the connection.

Server internals: one I/O thread owns the ROUTER and only moves bytes. Each
message is decoded and handled on the tokio runtime (DB work on the blocking
pool); replies return to the I/O thread over an inproc PUSH/PULL pair. At most
`max_inflight` requests (default 65 536, parked `Lease` waits included) are
handled at once; at the cap the I/O thread stops reading the ROUTER until a
handler finishes, so ZeroMQ's high-water marks and TCP push back on clients.
Each session may have at most `max_inflight_per_session` (default 1024) of
those; beyond it a request is refused at once with `Error{InvalidArgument,
"TooManyRequests"}` rather than parked, so one client can't hold the whole
budget.

Client: `call` waits for its own reply; replies to requests issued with `send`
that arrive meanwhile are kept for `recv`. Late replies to timed-out `call`s are
dropped.

## Handshake

```
C → S  Hello   { proto_ver, auth: Auth, caps: Caps }
S → C  Welcome { proto_ver, server_id, session: u128, lease_ttl_secs, task_types: [TaskTypeInfo] }
     | Error   { kind, message }

Auth = { uid, challenge, admin_token: Option<String> }
Caps = { runners: [Cpu | Hip], max_inflight }
```

- `uid` + `challenge` go through `core:auth_event` (default handler: allow; a
  mod can verify `challenge` as a proxy-issued ticket).
- `admin_token`, if present, goes through `core:admin_auth_event`; success
  grants `Delete`/`Purge`. No token → never admin. The core handler grants
  admin only when the server has `auth_token` configured and it matches; with
  none configured, only a mod handling the event can grant it.
- The `uid` is bound to the session: the ROUTER identity **plus** the random
  `session` key from `Welcome`, which the client sends in `Envelope::session`
  on every later request (zero on the handshake, responses and proxy
  traffic). The routing identity alone is peer-chosen, so a peer reusing a
  disconnected client's identity without the key gets nothing; a fresh
  `Hello` under that identity only replaces the session (the original client
  re-handshakes). Later messages don't carry the uid. Any request before
  `Hello`, or with a wrong key → `Error{InvalidArgument, "NotAuthenticated"}`. Sessions idle for `session_idle_secs` are dropped; the
  client re-`Hello`s automatically on `NotAuthenticated`.
- Version mismatch → `Error{NotSupported}`.

## Messages

| Request | Response | Nucleus |
|---|---|---|
| `Lease { task_type, max_tasks, wait_ms }` | `Leased` | `lease_timeout` |
| `Renew { leases: [LeaseRef] }` | `Renewed { results: [Result<expires_at_ms>] }` | `renew` |
| `Complete { results: [TaskResult], then_lease: Option<LeaseReq> }` | `Completed { results: [Result<()>], next: Option<Leased> }` | `complete` |
| `CancelLease { leases: [LeaseRef] }` | `Cancelled { results: [Result<()>] }` | `cancel` |
| `Query(Query)` | `Query(QueryResponse)` | `query`, `submit_batch` |

```
LeaseRef   { task_type, task_id, token }
TaskResult { lease: LeaseRef, bytes }
LeaseReq   { task_type, max_tasks }
Leased     { task_type, expires_at_ms, tasks: [LeasedItem { task_id, token, bytes }] }
```

Semantics:

- **Lease** waits up to `wait_ms` (clamped to 60 000) for the first task, then takes whatever else
  is already queued, up to `max_tasks` (capped at 65 536). Every task gets its
  own fencing token. Nothing queued in time → `Error{NotFound, "QueueEmpty"}`.
- **Renew**, **Complete**, **CancelLease** are batched with one result per
  entry, in order, so one stale lease doesn't fail the batch.
- **Complete** verifies each result with the task type's `verify`. Retrying a
  completion whose identical result is already stored succeeds (safe after a
  lost reply).
- **Lease piggyback**: set `then_lease` and the server returns the next batch
  in `Completed.next` (never waits; `None` if the queue is empty). Or omit it
  and send a separate `Lease`.
- **CancelLease** cancels the lease, never the task: it goes back to the queue.
- Wrong token → `InvalidArgument "LeaseTokenMismatch"`; no lease → `NotFound`.
- A lease belongs to the session `uid` that leased it: `Renew`/`Complete`/
  `CancelLease` from another uid → `InvalidArgument "LeaseNotOwned"`. Tokens
  are sequential (they fence, they are not secrets), so this is the access
  check; it is only as strong as `core:auth_event`'s uid verification.

## Query

Structured, not SQL text (a CLI can translate SQL-ish input client-side).
Submit is `Insert`. Typed queries only accept registered task types.

```
Insert       { task_type, payloads }          -> Ids [Result<task_id>]    (one WriteBatch)
Get          { task_type, ids }               -> Records [{ task_id, status, bytes }]
Scan         { task_type, scope, after, limit } -> Page { records: [{ task_id, bytes }], next }
EstimateKeys                                  -> Estimate (store-wide)
EstimateSize { task_type, scope }             -> Estimate (bytes)
Delete       { task_type, scope, ids }        -> Done   (admin; in-flight pending → TaskInFlight)
Purge        { task_type, scope }             -> Done   (admin; pending: skips in-flight)
Stats                                         -> Stats

scope  = Pending | Finished
status = Pending | Leased | Finished | Unknown   (Get returns the payload, or the result once finished)
Stats  = { server_id, uptime_ms, inserted, leased, completed, cancelled,
           queued, active_leases, sessions }
```

- Everything is O(seek): no unbounded scans; `Scan` pages are capped at 10 000.
  Exception: `Purge` of `Pending` walks the type's pending records.
- Pending `Delete`/`Purge` never remove the record of a queued or leased task
  (it would be lost on reap or restart). `Delete` refuses the whole request
  with `InvalidArgument "TaskInFlight: <id>"`; `Purge` skips those records,
  which then finish normally. Both run with the type's refill blocked, so a
  task can't be queued between the in-flight check and the delete.
- `Get` reports a task that finishes mid-request as `Finished`, never `Unknown`.
- `Stats.completed` counts successful `Complete` results, including idempotent
  retries. Poll it for a live throughput counter.

## Errors

`Error { kind, message }` with `kind` from `enginelib/src/error.rs`:
`NotFound`, `NotSupported`, `InvalidArgument`, `IOError`, `Unknown`.

## Proxy

```
C → P  Resolve  { task_type: Option<(ns, name)>, auth }
P → C  Resolved { servers: [ServerInfo], ticket: Option<bytes>, ttl_secs }
S → P  Register { info: ServerInfo, token }    // heartbeat every heartbeat_secs
P → S  Registered | Error { InvalidArgument, "RegistrationRefused" }

ServerInfo = { server_id, endpoint, task_types, load }   // load = queued + leased
```

- `Register.token` must equal the proxy's `proxy_token` (constant-time
  compare); otherwise the registration is refused and the server is never
  handed out, so nobody can advertise a fake server and collect clients'
  `Hello` credentials. **A proxy without `proxy_token` refuses every
  registration** (logged at startup). The token travels in clear: keep proxy
  traffic on a trusted network.
- `Resolved.servers` holds live servers serving `task_type`, least loaded
  first. A server missing three heartbeats drops out.
- `Client::connect_via_proxy` resolves once, then connects to the first server
  that accepts the handshake, using `ticket` (if any) as `auth.challenge`. The
  client keeps no discovery cache: to fail over, call it again when the server
  stops answering. `ttl_secs` is advisory, for callers that cache `resolve`.
- Forwarded auth: the core proxy issues no ticket; that is a mod's job.
- A server bound to a wildcard address must set `advertise` to the address
  clients should dial.

## Configuration (`config.toml`)

```toml
host = "[::1]:50051"           # server bind address
server_id = 0
advertise = "10.0.0.5:50051"   # announced to the proxy (default: bound endpoint)
proxy = "10.0.0.2:50050"       # register with this proxy (default: none)
proxy_host = "[::1]:50050"     # proxy bin bind address
proxy_token = "..."            # shared server <-> proxy registration secret
heartbeat_secs = 5
max_message_bytes = 67108864
session_idle_secs = 900
auth_token = "..."             # admin token
lease_ttl_secs = 300
reap_interval_secs = 15
queue_capacity = 8192
```

## Planned

- **Chunked storage**: tasks stored and leased in chunks with 12-byte ids
  `[server_id u16][chunk_seq u64][idx u16]`. That cuts DB operations and
  lease bookkeeping per task, and lets `Get`/`Complete` be routed to the
  owning server (`Error{Moved}`). The wire shapes above stay valid: tasks in a
  chunk would share a token.
- **Per-type settings** (`chunk_size`, `max_payload`, `max_result`,
  `lease_ttl_secs`), with precedence `config.toml` > mod (`TaskLimits` on
  `Task`) > engine default, surfaced in `TaskTypeInfo`.
- **Stats push** over a ZeroMQ PUB socket instead of polling.
- **Discovery cache** in the client: keep the `Resolved` answer for `ttl_secs`
  and re-resolve on expiry or disconnect automatically.
- **Ticket scheme** for forwarded auth (HMAC vs. ed25519).
