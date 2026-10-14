//! Wire protocol messages, shared by the server, clients and the proxy.
//!
//! Every ZeroMQ message is one postcard-encoded [`Envelope`]. ZeroMQ does the
//! framing, so there is no length prefix. Requests carry a client-chosen
//! `req_id` that the server echoes on the response; responses may arrive out of
//! order. The request set mirrors `nucleus/*` one-to-one. See
//! `docs/protocol.md`.

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::Identifier;
use crate::error::{Error, ErrorKind};

/// v2: authenticated proxy `Register { info, token }`.
/// v3: session key in `Envelope::session` / `Welcome.session`.
/// v4: chunked storage (12-byte task ids, per-type settings in `TaskTypeInfo`).
pub const PROTO_VERSION: u16 = 4;

/// Upper bound on tasks handed out by one `Lease` (or `then_lease`).
pub const MAX_LEASE_BATCH: u32 = 65_536;
/// Upper bound on how long one `Lease` may wait for its first task; larger
/// `wait_ms` values are clamped.
pub const MAX_LEASE_WAIT_MS: u32 = 60_000;
/// Upper bound on records returned by one `Scan` page.
pub const MAX_SCAN_LIMIT: u32 = 10_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope<T> {
    pub req_id: u64,
    /// Session key from `Welcome`, sent with every client -> server request
    /// after the handshake. Zero everywhere else (handshake, responses, proxy).
    pub session: u128,
    pub body: T,
}

pub fn encode<T: Serialize>(req_id: u64, body: &T) -> Result<Vec<u8>, Error> {
    encode_in_session(req_id, 0, body)
}

/// Like [`encode`], carrying a session key.
pub fn encode_in_session<T: Serialize>(
    req_id: u64,
    session: u128,
    body: &T,
) -> Result<Vec<u8>, Error> {
    postcard::to_allocvec(&Envelope {
        req_id,
        session,
        body,
    })
    .map_err(|err| Error::invalid_argument(format!("Failed to encode message: {err}")))
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<Envelope<T>, Error> {
    postcard::from_bytes(bytes)
        .map_err(|err| Error::invalid_argument(format!("Failed to decode message: {err}")))
}

// ---------------------------------------------------------------------------
// Client <-> server
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    /// Must be the first request on a connection. Binds `auth.uid` to it.
    Hello {
        proto_ver: u16,
        auth: Auth,
        caps: Caps,
    },
    /// Lease whole chunks until at least `max_tasks` tasks (or the queue is
    /// empty), waiting at most `wait_ms` for the first chunk.
    Lease {
        task_type: Identifier,
        max_tasks: u32,
        wait_ms: u32,
    },
    /// Extend each listed lease by a full TTL.
    Renew {
        leases: Vec<LeaseRef>,
    },
    /// Finalize tasks with their results. With `then_lease` set, the server also
    /// leases the next tasks and returns them in `Completed::next`.
    Complete {
        results: Vec<TaskResult>,
        then_lease: Option<LeaseReq>,
    },
    /// Give leased tasks back to the queue. Cancels the lease, not the task.
    CancelLease {
        leases: Vec<LeaseRef>,
    },
    Query(Query),
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Auth {
    pub uid: String,
    /// Checked by the `core:auth_event` handlers (a mod may treat this as a
    /// proxy-issued ticket).
    pub challenge: String,
    /// Checked by `core:admin_auth_event`; grants `Delete`/`Purge`.
    pub admin_token: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunnerKind {
    Cpu,
    Hip,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Caps {
    pub runners: Vec<RunnerKind>,
    pub max_inflight: u32,
}

/// Addresses one leased task and the token `lease` handed out for it. Every
/// task of a chunk lease shares the token; renew/cancel act on the whole chunk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseRef {
    pub task_type: Identifier,
    pub task_id: String,
    pub token: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseReq {
    pub task_type: Identifier,
    pub max_tasks: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResult {
    pub lease: LeaseRef,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeasedItem {
    pub task_id: String,
    pub token: u64,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Leased {
    pub task_type: Identifier,
    /// Earliest expiry across `tasks`, unix milliseconds.
    pub expires_at_ms: i64,
    pub tasks: Vec<LeasedItem>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskTypeInfo {
    pub task_type: Identifier,
    /// Tasks per chunk. A `Lease` hands out whole chunks, so it may return up
    /// to `chunk_size - 1` tasks more than `max_tasks`.
    pub chunk_size: u32,
    /// Largest accepted payload in bytes (`None` = unlimited).
    pub max_payload: Option<u32>,
    /// Largest accepted result in bytes (`None` = unlimited).
    pub max_result: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    Welcome {
        proto_ver: u16,
        server_id: u16,
        /// Random per-handshake key; the client must send it in every later
        /// `Envelope::session`. The ZeroMQ routing identity alone is
        /// peer-chosen, so it can't authenticate a session.
        session: u128,
        lease_ttl_secs: u64,
        task_types: Vec<TaskTypeInfo>,
    },
    Leased(Leased),
    /// One entry per requested lease, in order: the new expiry (unix ms).
    Renewed {
        results: Vec<Result<i64, WireError>>,
    },
    /// One entry per submitted result, in order.
    Completed {
        results: Vec<Result<(), WireError>>,
        next: Option<Leased>,
    },
    /// One entry per requested lease, in order.
    Cancelled {
        results: Vec<Result<(), WireError>>,
    },
    Query(QueryResponse),
    Error(WireError),
}

// ---------------------------------------------------------------------------
// Query (operator / CRUD surface, includes submit as `Insert`)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Scope {
    Pending,
    Finished,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Query {
    /// Submit tasks. Every payload is verified; ids come back in payload order.
    Insert {
        task_type: Identifier,
        payloads: Vec<Vec<u8>>,
    },
    /// Status (and payload or result) of individual tasks.
    Get {
        task_type: Identifier,
        ids: Vec<String>,
    },
    /// One page of tasks (not chunks), resuming strictly after the task id
    /// `after`.
    Scan {
        task_type: Identifier,
        scope: Scope,
        after: Option<String>,
        limit: u32,
    },
    /// Store-wide approximate key count.
    EstimateKeys,
    /// Approximate on-disk size of one scope of a task type.
    EstimateSize {
        task_type: Identifier,
        scope: Scope,
    },
    /// Admin. Deletes the whole chunk holding each id. Refused if a named
    /// chunk is queued or leased.
    Delete {
        task_type: Identifier,
        scope: Scope,
        ids: Vec<String>,
    },
    /// Admin. Drops every record of one scope of a task type.
    Purge {
        task_type: Identifier,
        scope: Scope,
    },
    Stats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    Pending,
    Leased,
    Finished,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRecord {
    pub task_id: String,
    pub status: TaskStatus,
    /// The pending payload, or the result once finished.
    pub bytes: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageRecord {
    pub task_id: String,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Stats {
    pub server_id: u16,
    pub uptime_ms: u64,
    pub inserted: u64,
    pub leased: u64,
    /// Successful `Complete` results, including idempotent retries.
    pub completed: u64,
    pub cancelled: u64,
    /// Tasks currently waiting in the in-memory queues.
    pub queued: u64,
    /// Leases currently held by workers.
    pub active_leases: u64,
    pub sessions: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryResponse {
    Ids(Vec<Result<String, WireError>>),
    Records(Vec<TaskRecord>),
    Page {
        records: Vec<PageRecord>,
        next: Option<String>,
    },
    Estimate(u64),
    Done,
    Stats(Stats),
}

// ---------------------------------------------------------------------------
// Proxy (discovery only)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProxyRequest {
    /// Client: which server should I use (for `task_type`, if given)?
    Resolve {
        task_type: Option<Identifier>,
        auth: Auth,
    },
    /// Server heartbeat: announces itself and its current load. `token` must
    /// match the proxy's `proxy_token`, or the registration is refused.
    Register { info: ServerInfo, token: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerInfo {
    pub server_id: u16,
    pub endpoint: String,
    pub task_types: Vec<Identifier>,
    /// Queued + leased tasks; lower is preferred.
    pub load: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProxyResponse {
    /// Candidate servers, least loaded first. Cache for `ttl_secs`.
    Resolved {
        servers: Vec<ServerInfo>,
        ticket: Option<Vec<u8>>,
        ttl_secs: u32,
    },
    Registered,
    Error(WireError),
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Per-entry outcomes of a batched request, in request order.
pub type Results<T> = Vec<Result<T, WireError>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireError {
    pub kind: ErrorKind,
    pub message: String,
}

impl From<Error> for WireError {
    fn from(err: Error) -> Self {
        Self {
            kind: err.kind(),
            message: err.into(),
        }
    }
}

impl From<WireError> for Error {
    fn from(err: WireError) -> Self {
        Error::with_kind(err.kind, err.message)
    }
}

/// Unix milliseconds of a chrono timestamp.
pub fn unix_ms(at: chrono::DateTime<chrono::Utc>) -> i64 {
    at.timestamp_millis()
}

/// Normalizes a configured address into a ZeroMQ endpoint: anything without a
/// scheme is treated as TCP (`[::1]:50051` -> `tcp://[::1]:50051`).
pub fn endpoint(addr: &str) -> String {
    if addr.contains("://") {
        addr.to_string()
    } else {
        format!("tcp://{addr}")
    }
}
