# AGENTS.md

Operational guide for coding agents in this repository.

## Scope

Rust workspace for a distributed task engine with dynamic mods.

Workspace members:
- `engine` (binaries: `server`, `proxy`, `bench`)
- `enginelib` (shared runtime API, events, tasks, plugin loader, task nucleus)
- `enginelib/macros` (proc macros used by mods and core)

> **Repo is mid-rewrite.** The earlier design (gRPC/proto, `client`/`packer`
> binaries, and the RFC1002/1003/1004 design docs) was removed in the "rewrite:
> clean" pass and no longer exists in the tree. Do not cite those RFCs or the
> proto layer — they are gone. The transport is now ZeroMQ; the wire protocol
> is specified in `docs/protocol.md`.

## Start here (fast orientation)

1. `Cargo.toml` (workspace)
2. `enginelib/src/api.rs` (`ServerAPI`, task queues, RocksDB, key helpers)
3. `enginelib/src/nucleus/*` (task lifecycle: submit/lease/renew/complete/cancel/query)
   and `enginelib/src/chunk.rs` (task ids, chunk encodings, seq allocator)
4. `enginelib/src/event.rs` (event dispatch)
5. `enginelib/src/plugin.rs` (mod loader)
6. `enginelib/src/config.rs` (config/token)
7. `enginelib/src/wire.rs` (wire messages) and `enginelib/src/transport/*`
   (ZeroMQ server, client, proxy); spec in `docs/protocol.md`
8. `engine/src/bin/server.rs`, `engine/src/bin/proxy.rs` (entrypoints)

## Build/test commands

- Build all: `cargo build --workspace`
- Test all: `cargo test --workspace`
- Run server: `cargo run -p engine --bin server`
- Run proxy: `cargo run -p engine --bin proxy`
- Needs the system `libzmq` (the `zmq` crate links it via pkg-config).

## Key architecture facts

- Engine state is centralized in `enginelib::api::ServerAPI`.
- Persistence is RocksDB-backed (`ServerAPI::db: Arc<rust_rocksdb::DB>`, store dir `engine_db`).
- Task lifecycle operations live in `enginelib/src/nucleus/*` (`submit`, `lease`, `renew`, `complete`, `cancel`, `query`).
- Core event system is in `enginelib/src/event.rs`; core events in `enginelib/src/events/*`.
- Mods are loaded from `./mods/*.rf` by `enginelib/src/plugin.rs`.
- Transport is ZeroMQ (`enginelib/src/transport/*`): server ROUTER on `host`,
  `transport::client::Client` (DEALER), and a discovery-only proxy (ROUTER on
  `proxy_host`) that servers heartbeat to. Messages are postcard `Envelope`s
  from `enginelib/src/wire.rs`; the request set mirrors `nucleus/*` one-to-one.
- Server I/O: one thread owns the ROUTER socket and only moves bytes; handlers
  run on tokio (DB work on the blocking pool) and send replies back over an
  inproc PUSH/PULL pair. Never touch the ROUTER from a handler (zmq sockets are
  not thread-safe).
- Transport-level guards (`transport/server.rs`): at most `max_inflight`
  handlers run at once (the I/O thread stops reading the ROUTER when full);
  `Renew`/`Complete`/`CancelLease` reject a lease held by another session uid
  (`LeaseNotOwned`, checked against the chunk lease's recorded uid) since
  tokens are guessable. `Delete`/`Purge` run under `LoadState::exclusive`
  (blocks refill) and never touch in-flight chunks.
  Handlers run on the caller's tokio runtime, so on shutdown the I/O thread
  stops reading requests and drains (`drain` in `transport/server.rs`): parked
  `Lease`s get `ServerStopping` (the shutdown branch wins ties), and replies are
  forwarded until every handler has answered. The per-session counter survives
  re-`Hello`.
- The proxy only accepts `Register` carrying its `proxy_token`
  (constant-time compare); **unset token = every registration refused**.

## Data/persistence model (important)

RocksDB is the source of truth. Tasks are stored and leased in **chunks**
(`enginelib/src/chunk.rs`); every task is still an individual payload with its
own verified result. Key layout (helpers in `enginelib/src/api.rs`: `task_key`,
`task_key_prefix`, `finished_task_key`, `finished_task_key_prefix`,
`chunk_seq_from_key`; spec in `docs/protocol.md` "Chunked storage"):
- `t:<namespace>:<task_name>:<chunk_seq 16 hex>` -> pending chunk record (payloads),
  column family `pending` (`api.cf_pending()`)
- `f:<namespace>:<task_name>:<chunk_seq 16 hex>` -> finished chunk record (results +
  done bitmap), column family `finished` (`api.cf_finished()`)
- `m:chunk_seq` (default CF) -> persisted `chunk_seq` high-water mark (`SeqAllocator`)

The pending record is immutable; a chunk's open tasks are the indexes *not* set
in the finished record's done bitmap. Use `api.read_pending` / `api.read_finished`
rather than raw `db.get` (raw keys live in non-default column families).

Task ids (wire `task_id`): 12 bytes `[server_id u16][chunk_seq u64][idx u16]`,
24 hex chars (`chunk::TaskId`, `api.task_id()`, `api.parse_task_id()` — the latter
rejects other servers' ids with NotFound `"WrongServer"`). `chunk_seq` is
server-wide (all task types), strictly increasing, reserved in persisted blocks so
ids are never reused across restarts.

In-memory state on `ServerAPI` (rebuilt from the DB by the refill path), keyed by
`Identifier`:
- `task_queue: TaskQueue` — per-type `Arc<TypeQueue>` { `tx`/`rx` channel of
  `QueuedChunk` (payloads + open bitmap), `dedup: DashSet<chunk_seq>`, queued-task
  counter }. A chunk is in the dedup set **iff** it is in flight (queued **or**
  leased). Channel capacity = `queue_capacity / chunk_size` chunks (min 1).
- `leased_tasks: LeasedTaskQueue` — per-type `Arc<DashMap<chunk_seq, LeaseCell>>`,
  `LeaseCell = Arc<Mutex<ChunkLease>>`. One lease (and one fencing `token`) per
  chunk; `ChunkLease.chunk.open` is the indexes this lease hasn't completed.
  **Lock order:** never lock a `ChunkLease` while holding a DashMap guard (clone
  the `Arc` out first); removing from the map while holding the mutex is fine.
  A lease that ends is marked `gone` under its mutex (`LeasedTaskQueue::end`).
- `load_state: LoadStates` — per-type `LoadState`: forward cursor, recovery set,
  generation/idle tracking, single-refill guard (see "Refill" below).
- `chunk_seq: SeqAllocator`, `settings: DashMap<Identifier, TaskSettings>`.

Config (`config.toml`, loaded by `ServerAPI::default()` via `Config::new()`;
`ServerAPI::open(path, cfg)` takes an explicit store path + config, e.g. for tests):
`host`, `server_id`, `auth_token` (alias `cgrpc_token`), `proxy_token`,
`lease_ttl_secs` (default 300), `reap_interval_secs` (default 15),
`queue_capacity` (tasks, default 65536), and per-type `[tasks."ns:name"]`
{ `chunk_size`, `max_payload`, `max_result` }. A missing `config.toml` means
defaults; one that exists but fails to read/parse makes `Config::new()`/
`from_file` panic (fail closed: defaults would drop `auth_token`).
`Config::try_from_file` returns the error instead.

Derived types (`TaskLimits::derived`, mod-only): a chunk's `t:` record is
`[0x02][start u64][count u64][base]` (`chunk::encode_derived`, `DerivedRange`)
instead of per-task payloads; the worker derives each task from `(base, idx)`
and completes the chunk with one result. To the engine it is a one-slot chunk
(idx 0), so dedup/lease/refill/complete are unchanged. Submitted only through
`submit_derived(base, start, count)` (wire `InsertDerived`; at most 65536 chunks
and 64 MiB of records per call, since each chunk copies `base`); `chunk_size` is
indexes per chunk (may exceed 65536); queue capacity, `max_tasks` and stats
count derived chunks as one task. Spec: `docs/protocol.md` "Derived types".

Per-type settings (`api.settings(&type) -> TaskSettings`): precedence
`config.toml` `[tasks."ns:name"]` > mod `Task::limits() -> TaskLimits` (defaulted
trait method) > engine default (`chunk_size` 1024, no size limits). Resolved in
`populate()` and sent in `Welcome.task_types`.

### Task lifecycle (`enginelib/src/nucleus/*`)

- `submit(bytes, type) -> task_id` / `submit_batch(payloads, type) -> [Result<task_id>]`
  — verifies each payload (+ `max_payload`), packs the valid ones of *this call*
  into fresh chunks of `chunk_size`, reserves seqs, writes all chunks in one
  `WriteBatch`, and only then returns ids. Never merges across calls. Bumps the
  load generation and requests a refill if the queue is below the watermark.
- `ServerAPI::load(type)` / `api.refill(&type)` — one refill pass (see below);
  `try_send`s loadable chunks up to the channel's free capacity. Never awaits.
- `lease` / `lease_timeout` -> `Lease` (one chunk: `chunk_seq`, `payloads`, `open`,
  `token`, `expires_at`; `lease.tasks()` yields `(task_id, payload)`), `try_lease`
  (non-blocking), `lease_many(max_tasks, wait)` (whole chunks until >= max_tasks;
  never splits a chunk). `lease_timeout` fails with NotFound `"QueueEmpty"`.
  Requests a refill below the 25% watermark, both before waiting and again after
  taking a chunk (so workers still parked on a small queue aren't left waiting).
- `renew` / `renew_batch` — extends the chunk lease named by `(task_id, token)`.
- `complete` / `complete_batch(items)` — verifies each result (+ `max_result`),
  groups items per (type, chunk, token) and merges each group into the `f:`
  record in one `WriteBatch` (partial completes allowed). When the chunk's last
  open task is done the same batch deletes the `t:` record and the lease + dedup
  entry are dropped. On write failure nothing changes (lease intact, retryable).
  Idempotent: a done task whose stored result is identical succeeds (with or
  without a live lease); otherwise NotFound `"TaskNotLeased"`, or
  `"LeaseTokenMismatch"` if another lease holds the chunk.
- `cancel` / `cancel_batch` — cancels the **chunk lease, not the tasks**: ends the
  lease and requeues its still-open indexes (`ServerAPI::requeue`). The chunk
  stays in the dedup set; if the channel is full/closed it is un-marked and put in
  the recovery set. Synchronous — no await window.
- Lease fencing: `renew`/`complete`/`cancel` must present the lease token.
  Unknown lease -> NotFound; held under another token -> InvalidArgument
  `"LeaseTokenMismatch"`. Tokens come from a process-global counter: they fence,
  they are not secrets.
- Reaper (`LeasedTaskQueue::reap_expired`): ends expired leases and requeues their
  open indexes like `cancel`.
- `query(Query) -> QueryResult` — raw operator surface over *chunk records*, all
  O(seek): `Get`/`Delete { key }`, `Scan { prefix, after, limit }`,
  `PurgePrefix`, `EstimateKeys`, `EstimateSize`. Routed to the CF by key prefix
  (`t:` pending, `f:` finished, else default). Raw `Delete`/`PurgePrefix` bypass
  dedup/lease state. There is intentionally no raw `Put`.
- Task-level views used by the wire `Query`: `get_tasks` (Finished / Leased /
  Pending / Unknown per task id; reads `t:` before `f:` so a task finishing
  mid-request is never Unknown), `scan_tasks` (pages over tasks, `after`/`next`
  are task ids; copies only the page's tasks), `delete_tasks` / `purge_tasks`
  (chunk granularity; never touch in-flight chunks; run under
  `LoadState::exclusive`). While a chunk's `t:` record exists its `f:` record is
  the only record of which tasks are done: finished-scope `Delete` refuses such
  chunks (`"ChunkNotFinished"`) and `Purge` keeps the `f:` of pending chunks that
  are in flight or partially done (range tombstones between them).

### Refill (`ServerAPI::refill`, `request_refill`, `LoadState` in `api.rs`)

- A pass first drains the **recovery set** (point reads; chunks whose dedup entry
  was cleared while the record stayed persisted), then advances a **forward-only
  cursor** over `t:` keys, bounded above by `SeqAllocator::safe_upto()` (the oldest
  seq whose insert is still being written). Keys only grow and nothing can land
  below `safe_upto` later, so the cursor never rescans from the start.
- Anything that clears a dedup entry of a still-pending chunk must call
  `LoadState::recover(chunk_seq)` *after* clearing it (including a chunk whose
  load failed on a read error; only a corrupt record is skipped until restart).
  `mark_rescan()` (reset the cursor) is only for out-of-band `t:` writes.
- `safe_upto` caps the cursor, so an insert still being written can hide chunks
  other inserts already wrote above it. A pass that stopped there with
  `safe_upto < next` sets `LoadState.capped` and never marks the type idle;
  `submit_batch` calls `ServerAPI::wake_capped` after releasing its reservation
  to refill those types. Anything else that releases a `SeqReservation` must too.
- Loading a chunk reads its `f:` record to compute the open indexes (bloom filter
  keeps the "no results yet" probe cheap).
- `generation` is bumped by `submit` and `recover`; after a pass that reached the
  end with an empty recovery set, refills no-op until it changes.
- Dedup entries are only ever *added* by a refill pass, under the per-type cursor
  lock. `LoadState::exclusive(f)` holds that lock, so inside `f` no chunk can
  become queued: that is what makes "check in-flight, then delete" atomic for
  `delete_tasks`/`purge_tasks`. Keep it that way (requeue keeps the existing
  entry; it never inserts).
- Triggers: `lease()` below the 25% watermark (before waiting and after
  receiving), `submit()` below the watermark, a requeue overflow.
  `request_refill` runs at most one refill per type (`AtomicBool` slot; requests
  that arrive mid-run cause one more pass), on tokio's blocking pool inside a
  runtime, else inline.

## Mod ABI contract (must keep)

Each `.rf` package must contain platform lib at root:
- Linux `mod.so`, macOS `mod.dylib`, Windows `mod.dll`

Exported symbols required:
- `metadata() -> LibraryMetadata`
- `run(api: &mut ServerAPI)`

`#[metadata]` and `#[module]` macros are the preferred way to provide these.

Strict compatibility check is enabled when `GE_STRICT_MODS` is set.

### Wasm mods (experimental, `enginewasm` crate)

Untrusted task mods as core wasm (`./mods/*.wasm`, loaded only by
`engine --features wasm`'s server via `enginewasm::load_mods` before
`populate`). The ABI is `ge_alloc`/`ge_free`/`ge_verify`/`ge_run`, the only
import is `ge.log`, and metadata lives in the `ge:meta` custom section. An
optional WGSL kernel in `ge:wgsl` runs on wgpu (`enginewasm::gpu`) and must
match `ge_run` byte for byte. Sandbox limits (memory, epoch timeout, io caps)
are in `SandboxLimits`. Spec: `docs/wasm.md`. Tests: `enginewasm/tests/sandbox.rs`
(the GPU test skips without an adapter). Worker: `engine/src/bin/wasm_worker.rs`.

## Auth behavior warning

- User auth/admin auth are event-driven.
- Default `AuthEvent` handler currently allows auth (`true`).
- Admin auth checks the `config.toml` token (`auth_token`, aliased from the old
  `cgrpc_token`) in `events/admin_auth_event.rs` with a constant-time compare
  (`subtle`); **unset token = no admin** from the core handler (a mod may still
  grant it by handling `core:admin_auth_event`).

Do not assume secure defaults without verifying handlers/mods.

## Change playbooks

### Task lifecycle/queue change
- Touch: `enginelib/src/nucleus/*`, `enginelib/src/api.rs`, `enginelib/src/chunk.rs`,
  `enginelib/src/task.rs`, `engine/src/bin/server.rs`
- Keep the three representations in sync **at chunk granularity**: RocksDB
  records (`t:` + `f:`), the dedup `DashSet<chunk_seq>`, and `leased_tasks`. A
  chunk must be in the dedup set for exactly as long as it is queued or leased —
  never leave it set with the chunk neither queued nor leased, and when clearing
  it while the `t:` record stays, call `LoadState::recover`.
- A chunk's open indexes = not done in its `f:` record. Queue entries and leases
  carry that bitmap; never requeue indexes that already have results.
- Validate transitions: submitted (chunked) -> queued (refill) -> leased ->
  completed (partial or full; full deletes `t:`), plus lease cancel / reaping
  (requeue only open indexes; overflow -> recovery set) and restart (leases lost,
  `t:` chunks reload with their open indexes, `chunk_seq` resumes above the
  persisted high-water mark).
- Lifecycle tests live in `enginelib/tests/nucleus.rs`; build the API with
  `ServerAPI::open(<temp dir>, cfg)` so tests don't share `./engine_db`.
- Throughput: `cargo run --release -p engine --bin bench -- --tasks N --db <disk dir>`
  (in-process server + inserter/worker threads; `/tmp` may be tmpfs).

### Wire/transport change
- Messages: `enginelib/src/wire.rs` (bump `PROTO_VERSION` on incompatible
  changes; currently 5); server/client/proxy: `enginelib/src/transport/*`; keep
  `docs/protocol.md` in sync.
- Each wire request must map onto a nucleus op so the dedup/lease/RocksDB
  invariants stay enforced in one place. No raw `Put` over the wire.
- End-to-end tests: `enginelib/tests/transport.rs` (real loopback TCP server
  on `tcp://127.0.0.1:*`).

### Event/macro change
- Runtime dispatch: `enginelib/src/event.rs`
- Macro generation: `enginelib/macros/src/lib.rs`
- Core event wrappers: `enginelib/src/events/*.rs`
- The RFC1003/1004 hook-scope docs no longer exist; derive scope from the
  `events/*` wrappers and their `#[event(...)]` attributes (namespace/name/ctx).

### Mod loading issue
- Inspect `enginelib/src/plugin.rs`
- The RFC1002 package-format doc no longer exists; the `.rf` ABI is defined by the
  "Mod ABI contract" section above and enforced in `plugin.rs`.

## Quality gates before finishing

Always run:
- `cargo test --workspace`

If changing API surface or behavior, also run:
- `cargo build --workspace`

Report any warnings/errors that indicate risk (panic paths, serialization, auth flow).

## Notes for future agents

- Existing code has several warnings and `unwrap()` calls in runtime paths.
- Prefer minimal, targeted edits.
- Preserve current external behavior unless task explicitly requests behavior change.
- Event cancellation is in-process, not transport: `#[event(..., cancellable)]` gives
  an event `cancel()`/`is_cancelled()`, and callers branch on the returned `.cancelled`
  flag (see `events/*`). The old gRPC `Status::aborted` mapping no longer applies.
  Auth events are fired by the transport on `Hello` (`core:auth_event`, and
  `core:admin_auth_event` when the client presents an `admin_token`).
