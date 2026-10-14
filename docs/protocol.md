# Engine wire protocol

Status: v4 (`PROTO_VERSION = 4`) implemented over ZeroMQ (`enginelib/src/wire.rs`,
`enginelib/src/transport/*`), with chunked task storage (see
[Chunked storage](#chunked-storage)). The message set mirrors
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

Auth         = { uid, challenge, admin_token: Option<String> }
Caps         = { runners: [Cpu | Hip], max_inflight }
TaskTypeInfo = { task_type, chunk_size, max_payload: Option<u32>, max_result: Option<u32> }
```

`TaskTypeInfo` carries the effective [per-type settings](#per-type-settings).

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

- **The lease unit is a chunk.** **Lease** waits up to `wait_ms` (clamped to
  60 000) for the first chunk, then takes whatever else is already queued, whole chunks at a time,
  until it holds at least `max_tasks` tasks (capped at 65 536). Chunks are
  never split, so it may return up to `chunk_size - 1` tasks more than
  `max_tasks`; size requests in multiples of `TaskTypeInfo.chunk_size`. All
  tasks of one chunk share one fencing token. A chunk that was partially
  completed before (cancel, reap, restart) is leased with only its open tasks.
  Nothing queued in time → `Error{NotFound, "QueueEmpty"}`.
- **Renew**, **Complete**, **CancelLease** are batched with one result per
  entry, in order, so one stale lease doesn't fail the batch.
- **Renew** and **CancelLease** act on the whole chunk lease named by an
  entry's `(task_id, token)`; entries of the same chunk lease share one
  outcome. Cancelling gives every still-open task of the chunk back.
- **Complete** verifies each result with the task type's `verify` (and
  `max_result`). It may cover any subset of a chunk lease (partial complete);
  the chunk stays leased until its last open task is done. Retrying a
  completion whose identical result is already stored succeeds, with or
  without a live lease (safe after a lost reply); a different result for a
  done task → `NotFound "TaskNotLeased"`.
- **Lease piggyback**: set `then_lease` and the server returns the next batch
  in `Completed.next` (never waits; `None` if the queue is empty). Or omit it
  and send a separate `Lease`.
- **CancelLease** cancels the lease, never the task: it goes back to the queue.
- Wrong token → `InvalidArgument "LeaseTokenMismatch"`; no lease → `NotFound`;
  malformed task id → `InvalidArgument "InvalidTaskId"`; another server's id →
  `NotFound "WrongServer"`.
- A chunk lease belongs to the session `uid` that leased it: `Renew`/`Complete`/
  `CancelLease` naming it from another uid → `InvalidArgument "LeaseNotOwned"`.
  Tokens are sequential (they fence, they are not secrets), so this is the
  access check; it is only as strong as `core:auth_event`'s uid verification.

## Query

Structured, not SQL text (a CLI can translate SQL-ish input client-side).
Submit is `Insert`. Typed queries only accept registered task types.

```
Insert       { task_type, payloads }          -> Ids [Result<task_id>]    (one WriteBatch)
Get          { task_type, ids }               -> Records [{ task_id, status, bytes }]
Scan         { task_type, scope, after, limit } -> Page { records: [{ task_id, bytes }], next }
EstimateKeys                                  -> Estimate (store-wide)
EstimateSize { task_type, scope }             -> Estimate (bytes)
Delete       { task_type, scope, ids }        -> Done   (admin; in flight → TaskInFlight;
                                                           finished of a pending chunk → ChunkNotFinished)
Purge        { task_type, scope }             -> Done   (admin; skips in-flight / partially done chunks)
Stats                                         -> Stats

scope  = Pending | Finished
status = Pending | Leased | Finished | Unknown   (Get returns the payload, or the result once finished)
Stats  = { server_id, uptime_ms, inserted, leased, completed, cancelled,
           queued, active_leases, sessions }
```

- Everything is O(seek): no unbounded scans; `Scan` pages are capped at 10 000.
  Exception: `Purge` (either scope) walks the type's pending chunk keys.
- `Insert` packs the verified payloads of one call into fresh chunks of
  `chunk_size` (a partial last chunk is fine) and writes them in one
  `WriteBatch`; ids are returned only once that write is durable. Chunks are
  never shared across calls, so batch your inserts (a single-payload insert
  costs a whole chunk record). Payloads over `max_payload` →
  `InvalidArgument "PayloadTooLarge"`.
- `Get` answers per task: `Finished` (result), `Leased` / `Pending` (payload),
  `Unknown` (no such task, malformed id, or another server's id). A task that
  finishes mid-request is reported `Finished`, never `Unknown`.
- `Scan` pages over **tasks**, in task id order: `Pending` lists tasks without
  a result, `Finished` tasks with one. `after` / `next` are task ids. Only the
  tasks on the page are copied out of a chunk record.
- `Delete` works at **chunk granularity**: each id deletes its whole chunk
  record in that scope.
- `Delete`/`Purge` never remove the records of a queued or leased chunk (they
  would be lost on reap or restart). `Delete` refuses the whole request with
  `InvalidArgument "TaskInFlight: <id>"`; `Purge` skips those chunks, which
  then finish normally. Both run with the type's refill blocked, so a chunk
  can't be queued (and gain results) between the check and the delete.
- A chunk's finished record is the only record of which of its tasks are done
  while its pending record exists. So `Delete` of `Finished` refuses a chunk
  that is still pending (`InvalidArgument "ChunkNotFinished: <id>"`; delete
  the pending chunk first, or let it finish), and `Purge` of `Finished` keeps
  the finished records of pending chunks that are in flight or partially done.
  `Delete`/`Purge` of `Pending` leave the finished record of a partially done
  chunk in place; it then counts as finished and can be deleted.
- `Stats` counters are per task (`queued` / `active_leases` count open tasks,
  not chunks). `Stats.completed` counts successful `Complete` results,
  including idempotent retries. Poll it for a live throughput counter.

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
queue_capacity = 65536         # approx. tasks per in-memory queue (held as chunks)

[tasks."ns:name"]              # per-type overrides (optional)
chunk_size = 1024
max_payload = 65536
max_result = 65536
```

## Chunked storage

Every task is still an individual payload with its own verified, stored
result; only the **storage and lease unit** is a chunk of up to `chunk_size`
tasks. That replaces ~3 RocksDB ops, an 80-char id and a lease entry per task
with a few ops and one lease per chunk.

### Task ids

12 bytes, hex encoded as the wire `task_id` (24 lowercase hex chars):

```
[server_id u16][chunk_seq u64][idx u16]      e.g. 0003 000000000000002a 0007
```

- `server_id` — config `server_id`. A server only accepts its own ids.
- `chunk_seq` — strictly increasing, **server-wide** (shared by all task types:
  one allocator and meta key, ids unique across types, and one `safe_upto`
  watermark for every refill cursor). Persisted in the default column family
  as `m:chunk_seq` (big-endian u64 high-water mark), reserved in blocks of
  65 536 *before* use, so a restart resumes above every seq ever handed out
  and never reuses an id.
- `idx` — the task's index inside its chunk.

### Records

| Key | Column family | Value |
|---|---|---|
| `t:<ns>:<name>:<chunk_seq 16 hex>` | `pending` | payloads |
| `f:<ns>:<name>:<chunk_seq 16 hex>` | `finished` | results + done bitmap |
| `m:chunk_seq` | default | seq high-water mark |

`chunk_seq` is zero-padded hex, so key order is numeric order. Encodings
(`enginelib/src/chunk.rs`, little-endian `u32` lengths):

```
pending   [0x01][n u32]([len u32][payload])*n
finished  [0x01][n u32][done bitmap ceil(n/8) bytes]([len u32][result])*popcount(done)
```

- The pending record is immutable. A chunk's open tasks are the indexes not
  set in the finished record's done bitmap (no separate open bitmap to
  rewrite on every partial complete).
- A `Complete` merges its results into the finished record (read-modify-write
  only if the chunk already had results) in one `WriteBatch`; when the last
  open task is done, the same batch deletes the pending record.
- `pending` is delete-heavy and compacts on deletion; `finished` is
  append-mostly and stores values in blob files (`min_blob_size` 4 KiB). Both
  use bloom filters (refill probes `finished` for every chunk it loads).

### In-memory state (per task type)

- Queue of chunks (`TypeQueue`), capacity `queue_capacity / chunk_size`
  chunks; each entry carries the chunk's payloads and open-index bitmap.
- Dedup set of `chunk_seq`: a chunk is in it **iff** it is queued or leased.
- Leases keyed by `chunk_seq`, one fencing token each.
- Refill: first drains a *recovery set* (chunks whose dedup entry was cleared
  while their record stayed persisted — a cancel/reap requeue that found the
  queue full, a failed refill send), then advances a forward-only cursor over
  `t:` keys bounded by the allocator's `safe_upto` (the oldest chunk whose
  insert is still being written). Since keys only grow and nothing lands
  below `safe_upto` later, the cursor never rescans from the start. A pass
  that stopped at `safe_upto` while higher seqs were already handed out never
  marks the type idle, and the insert that releases the blocking reservation
  wakes it, so a chunk written above an unfinished insert isn't left waiting.
  A chunk whose load fails on a read error goes to the recovery set; a corrupt
  record is skipped (and logged) until a restart.
- Cancel and reaping requeue the still-open indexes of the chunk directly.

## Per-type settings

| Setting | Meaning | Engine default |
|---|---|---|
| `chunk_size` | tasks per chunk, 1..=65 536 | 1024 |
| `max_payload` | largest accepted payload (bytes) | unlimited |
| `max_result` | largest accepted result (bytes) | unlimited |

Precedence: `config.toml` `[tasks."ns:name"]` > mod (`Task::limits() ->
TaskLimits`, a defaulted trait method) > engine default. Resolved once per
type in `ServerAPI::populate`, surfaced in `Welcome.task_types`.

## Planned

- **Routing by id**: ids carry `server_id`, so `Get`/`Complete` for another
  server could answer `Error{Moved}` with its endpoint (today: `WrongServer`).
- **Per-type `lease_ttl_secs`** alongside the other per-type settings.
- **Stats push** over a ZeroMQ PUB socket instead of polling.
- **Discovery cache** in the client: keep the `Resolved` answer for `ttl_secs`
  and re-resolve on expiry or disconnect automatically.
- **Ticket scheme** for forwarded auth (HMAC vs. ed25519).
