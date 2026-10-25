//! Wire protocol messages, shared by the server, clients and the proxy.
//!
//! Every ZeroMQ message is one postcard-encoded [`Envelope`]. ZeroMQ does the
//! framing, so there is no length prefix. Requests carry a client-chosen
//! `req_id` that the server echoes on the response; responses may arrive out of
//! order. The request set mirrors `nucleus/*` one-to-one. See
//! `docs/protocol.md`.

use serde::ser::{SerializeSeq, SerializeStruct, SerializeStructVariant};
use serde::{Deserialize, Serialize, Serializer};

use crate::Identifier;
use crate::api::Lease;
pub use crate::chunk::DerivedRange;
use crate::chunk::TaskId;
use crate::error::{Error, ErrorKind};

/// v2: authenticated proxy `Register { info, token }`.
/// v3: session key in `Envelope::session` / `Welcome.session`.
/// v4: chunked storage (12-byte task ids, per-type settings in `TaskTypeInfo`).
/// v5: derived task types (`InsertDerived`, `range` on leased/queried tasks).
pub const PROTO_VERSION: u16 = 5;

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

/// Decodes an envelope. `T` may borrow from `bytes` (see [`RequestIn`]).
pub fn decode<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> Result<Envelope<T>, Error> {
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
    #[serde(with = "bytes")]
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeasedItem {
    pub task_id: String,
    pub token: u64,
    /// The task payload; for a derived chunk, the base.
    #[serde(with = "bytes")]
    pub bytes: Vec<u8>,
    /// Derived chunk: the task indexes to derive from `bytes`. Complete the
    /// whole range with one result under `task_id`.
    pub range: Option<DerivedRange>,
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
    /// Derived type: insert with `InsertDerived`; every leased item is a whole
    /// chunk (`range` set) completed with one result. `chunk_size` is then
    /// indexes per chunk, and `max_tasks`/stats count chunks.
    pub derived: bool,
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
        #[serde(with = "bytes::seq")]
        payloads: Vec<Vec<u8>>,
    },
    /// Submit the indexes `start..start + count` of a derived type. `base` is
    /// verified once; the range is cut into chunks of `chunk_size` indexes.
    /// Returns one id per chunk, in range order.
    InsertDerived {
        task_type: Identifier,
        #[serde(with = "bytes")]
        base: Vec<u8>,
        start: u64,
        count: u64,
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
    #[serde(with = "bytes::option")]
    pub bytes: Option<Vec<u8>>,
    /// Derived chunk not finished yet: its index range (`bytes` is the base).
    pub range: Option<DerivedRange>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageRecord {
    pub task_id: String,
    #[serde(with = "bytes")]
    pub bytes: Vec<u8>,
    /// Pending derived chunk: its index range (`bytes` is the base).
    pub range: Option<DerivedRange>,
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

// ---------------------------------------------------------------------------
// Byte fields
// ---------------------------------------------------------------------------

/// `serde(with)` helpers for `Vec<u8>` fields. Postcard encodes a byte string
/// exactly like a sequence of `u8` (varint length, then the bytes), so these
/// change nothing on the wire; they just copy the bytes in one go instead of
/// one element at a time.
pub mod bytes {
    use std::fmt;

    use serde::de::{Deserializer, SeqAccess, Visitor};
    use serde::ser::Serializer;

    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(bytes)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        deserializer.deserialize_byte_buf(ByteBuf)
    }

    struct ByteBuf;

    impl<'de> Visitor<'de> for ByteBuf {
        type Value = Vec<u8>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("bytes")
        }

        fn visit_bytes<E>(self, bytes: &[u8]) -> Result<Vec<u8>, E> {
            Ok(bytes.to_vec())
        }

        fn visit_byte_buf<E>(self, bytes: Vec<u8>) -> Result<Vec<u8>, E> {
            Ok(bytes)
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<u8>, A::Error> {
            let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(4096));
            while let Some(byte) = seq.next_element()? {
                out.push(byte);
            }
            Ok(out)
        }
    }

    /// For `Option<Vec<u8>>`.
    pub mod option {
        use serde::{Deserialize, Deserializer, Serializer};

        #[derive(Deserialize)]
        #[serde(transparent)]
        struct Bytes(#[serde(with = "super")] Vec<u8>);

        pub fn serialize<S: Serializer>(
            bytes: &Option<Vec<u8>>,
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            match bytes {
                Some(bytes) => serializer.serialize_some(&super::Slice(bytes)),
                None => serializer.serialize_none(),
            }
        }

        pub fn deserialize<'de, D: Deserializer<'de>>(
            deserializer: D,
        ) -> Result<Option<Vec<u8>>, D::Error> {
            Ok(Option::<Bytes>::deserialize(deserializer)?.map(|bytes| bytes.0))
        }
    }

    /// For `Vec<Vec<u8>>`.
    pub mod seq {
        use serde::{Deserialize, Deserializer, Serializer};

        #[derive(Deserialize)]
        #[serde(transparent)]
        struct Bytes(#[serde(with = "super")] Vec<u8>);

        pub fn serialize<S: Serializer>(
            items: &[Vec<u8>],
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            serializer.collect_seq(items.iter().map(|bytes| super::Slice(bytes)))
        }

        pub fn deserialize<'de, D: Deserializer<'de>>(
            deserializer: D,
        ) -> Result<Vec<Vec<u8>>, D::Error> {
            let items = Vec::<Bytes>::deserialize(deserializer)?;
            Ok(items.into_iter().map(|bytes| bytes.0).collect())
        }
    }

    /// A borrowed byte string that serializes as bytes.
    pub(crate) struct Slice<'a>(pub &'a [u8]);

    impl serde::Serialize for Slice<'_> {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.serialize_bytes(self.0)
        }
    }
}

// ---------------------------------------------------------------------------
// Server-side views
// ---------------------------------------------------------------------------
//
// Postcard is positional, so a type with the same variants and fields in the
// same order (and compatible field types) shares the encoding. The server uses
// that to decode requests and encode replies without a heap copy per task.
// Each view must mirror its public type exactly; the tests below check it.

/// Just the envelope header and the request variant, without decoding the
/// body (postcard encodes a variant as its index, then its fields, so a unit
/// variant reads only the index). Lets the server pick where to decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub(crate) enum RequestKind {
    Hello,
    Lease,
    Renew,
    Complete,
    CancelLease,
    Query,
}

/// [`Request`] whose bulk fields (`Complete` results, `Insert` payloads)
/// borrow from the received message.
#[derive(Debug, Deserialize)]
pub(crate) enum RequestIn<'a> {
    Hello {
        proto_ver: u16,
        auth: Auth,
        #[allow(dead_code)]
        caps: Caps,
    },
    Lease {
        task_type: Identifier,
        max_tasks: u32,
        wait_ms: u32,
    },
    Renew {
        leases: Vec<LeaseRef>,
    },
    Complete {
        #[serde(borrow)]
        results: Vec<TaskResultIn<'a>>,
        then_lease: Option<LeaseReq>,
    },
    CancelLease {
        leases: Vec<LeaseRef>,
    },
    Query(#[serde(borrow)] QueryIn<'a>),
}

/// Borrowed [`LeaseRef`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub(crate) struct LeaseRefIn<'a> {
    #[serde(borrow)]
    pub task_type: (&'a str, &'a str),
    pub task_id: &'a str,
    pub token: u64,
}

impl LeaseRef {
    pub(crate) fn as_in(&self) -> LeaseRefIn<'_> {
        LeaseRefIn {
            task_type: (&self.task_type.0, &self.task_type.1),
            task_id: &self.task_id,
            token: self.token,
        }
    }
}

/// Borrowed [`TaskResult`].
#[derive(Debug, Clone, Copy, Deserialize)]
pub(crate) struct TaskResultIn<'a> {
    #[serde(borrow)]
    pub lease: LeaseRefIn<'a>,
    pub bytes: &'a [u8],
}

/// [`Query`] whose insert payloads borrow from the received message.
#[derive(Debug, Deserialize)]
pub(crate) enum QueryIn<'a> {
    Insert {
        task_type: Identifier,
        #[serde(borrow)]
        payloads: Vec<&'a [u8]>,
    },
    InsertDerived {
        task_type: Identifier,
        base: &'a [u8],
        start: u64,
        count: u64,
    },
    Get {
        task_type: Identifier,
        ids: Vec<String>,
    },
    Scan {
        task_type: Identifier,
        scope: Scope,
        after: Option<String>,
        limit: u32,
    },
    EstimateKeys,
    EstimateSize {
        task_type: Identifier,
        scope: Scope,
    },
    Delete {
        task_type: Identifier,
        scope: Scope,
        ids: Vec<String>,
    },
    Purge {
        task_type: Identifier,
        scope: Scope,
    },
    Stats,
}

/// A server reply: any [`Response`], or one of the bulk responses written
/// straight from engine state. Encodes exactly like the matching `Response`.
pub(crate) enum Reply {
    Response(Response),
    /// `Response::Leased`.
    Leased(LeasedView),
    /// `Response::Completed`.
    Completed {
        results: Vec<Result<(), WireError>>,
        next: Option<LeasedView>,
    },
    /// `Response::Query(QueryResponse::Ids(..))`.
    Ids(Vec<Result<TaskId, WireError>>),
}

impl From<Response> for Reply {
    fn from(response: Response) -> Self {
        Reply::Response(response)
    }
}

/// [`Leased`] built from the chunk leases themselves: task ids are
/// hex-encoded on the stack and payloads written from the shared chunk data.
pub(crate) struct LeasedView {
    pub task_type: Identifier,
    pub expires_at_ms: i64,
    pub leases: Vec<Lease>,
}

impl LeasedView {
    /// Number of leased tasks.
    pub fn len(&self) -> usize {
        self.leases.iter().map(Lease::len).sum()
    }

    /// Roughly the encoded size, to allocate the reply buffer once.
    pub fn size_hint(&self) -> usize {
        let per_task = TASK_ID_HEX_LEN_WIRE + 10 + 5 + 18;
        let payloads: usize = self
            .leases
            .iter()
            .flat_map(|lease| {
                lease
                    .open
                    .iter()
                    .map(|idx| lease.payloads.get(idx).map_or(0, <[u8]>::len))
            })
            .sum();
        64 + self.len() * per_task + payloads
    }
}

/// A task id on the wire: length prefix plus 24 hex chars.
const TASK_ID_HEX_LEN_WIRE: usize = 1 + crate::chunk::TASK_ID_HEX_LEN;

impl Serialize for Reply {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // Variant indexes of `Response` and `QueryResponse`.
        const LEASED: u32 = 1;
        const COMPLETED: u32 = 3;
        const QUERY: u32 = 5;
        const IDS: u32 = 0;
        struct Ids<'a>(&'a [Result<TaskId, WireError>]);
        impl Serialize for Ids<'_> {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_newtype_variant("QueryResponse", IDS, "Ids", self.0)
            }
        }
        match self {
            Reply::Response(response) => response.serialize(serializer),
            Reply::Leased(leased) => {
                serializer.serialize_newtype_variant("Response", LEASED, "Leased", leased)
            }
            Reply::Completed { results, next } => {
                let mut variant =
                    serializer.serialize_struct_variant("Response", COMPLETED, "Completed", 2)?;
                variant.serialize_field("results", results)?;
                variant.serialize_field("next", next)?;
                variant.end()
            }
            Reply::Ids(ids) => {
                serializer.serialize_newtype_variant("Response", QUERY, "Query", &Ids(ids))
            }
        }
    }
}

impl Serialize for LeasedView {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        struct Tasks<'a>(&'a LeasedView);
        impl Serialize for Tasks<'_> {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
                for lease in &self.0.leases {
                    for idx in lease.open.iter() {
                        seq.serialize_element(&Item {
                            task_id: TaskId::new(lease.server_id, lease.chunk_seq, idx as u16),
                            token: lease.token,
                            bytes: lease.payloads.get(idx).unwrap_or_default(),
                            range: lease.range(),
                        })?;
                    }
                }
                seq.end()
            }
        }
        /// Mirrors [`LeasedItem`].
        struct Item<'a> {
            task_id: TaskId,
            token: u64,
            bytes: &'a [u8],
            range: Option<DerivedRange>,
        }
        impl Serialize for Item<'_> {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let mut item = serializer.serialize_struct("LeasedItem", 4)?;
                item.serialize_field("task_id", &self.task_id)?;
                item.serialize_field("token", &self.token)?;
                item.serialize_field("bytes", &bytes::Slice(self.bytes))?;
                item.serialize_field("range", &self.range)?;
                item.end()
            }
        }
        let mut leased = serializer.serialize_struct("Leased", 3)?;
        leased.serialize_field("task_type", &self.task_type)?;
        leased.serialize_field("expires_at_ms", &self.expires_at_ms)?;
        leased.serialize_field("tasks", &Tasks(self))?;
        leased.end()
    }
}

/// Encodes a reply into a buffer of `capacity` bytes (grown if needed).
pub(crate) fn encode_reply(req_id: u64, reply: &Reply, capacity: usize) -> Result<Vec<u8>, Error> {
    postcard::to_extend(
        &Envelope {
            req_id,
            session: 0,
            body: reply,
        },
        Vec::with_capacity(capacity),
    )
    .map_err(|err| Error::invalid_argument(format!("Failed to encode message: {err}")))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::chunk::{Bitmap, ChunkPayloads, encode_derived, encode_pending};

    fn ty() -> Identifier {
        ("ns".into(), "name".into())
    }

    fn lease_ref(task_id: &str, token: u64) -> LeaseRef {
        LeaseRef {
            task_type: ty(),
            task_id: task_id.into(),
            token,
        }
    }

    fn lease(chunk_seq: u64, record: Vec<u8>, open: &[usize], token: u64) -> Lease {
        let payloads = ChunkPayloads::decode(record).unwrap();
        let mut bitmap = Bitmap::empty(payloads.len());
        for &idx in open {
            bitmap.set(idx);
        }
        Lease {
            task_type: ty(),
            server_id: 7,
            chunk_seq,
            payloads: Arc::new(payloads),
            open: bitmap,
            token,
            expires_at: chrono::Utc::now(),
        }
    }

    fn leases() -> Vec<Lease> {
        let regular = encode_pending([&b"aa"[..], b"", b"cccc"].into_iter());
        let derived = encode_derived(b"base", DerivedRange { start: 5, count: 9 });
        vec![
            lease(1, regular, &[0, 2], 11),
            lease(1 << 40, derived, &[0], 12),
        ]
    }

    /// The owned `Leased` the view stands for.
    fn owned(view: &LeasedView) -> Leased {
        Leased {
            task_type: view.task_type.clone(),
            expires_at_ms: view.expires_at_ms,
            tasks: view
                .leases
                .iter()
                .flat_map(|lease| {
                    lease.tasks().map(|(task_id, bytes)| LeasedItem {
                        task_id,
                        token: lease.token,
                        bytes: bytes.to_vec(),
                        range: lease.range(),
                    })
                })
                .collect(),
        }
    }

    fn view() -> LeasedView {
        LeasedView {
            task_type: ty(),
            expires_at_ms: 1_700_000_000_123,
            leases: leases(),
        }
    }

    #[test]
    fn reply_encodes_like_response() {
        let view = view();
        assert_eq!(view.len(), 3);
        let leased = owned(&view);
        let same = |reply: Reply, response: Response| {
            assert_eq!(
                encode_reply(9, &reply, 0).unwrap(),
                encode(9, &response).unwrap()
            );
        };
        same(Reply::Leased(view), Response::Leased(leased.clone()));

        let results = vec![Ok(()), Err(Error::not_found("TaskNotLeased").into())];
        same(
            Reply::Completed {
                results: results.clone(),
                next: Some(super::tests::view()),
            },
            Response::Completed {
                results: results.clone(),
                next: Some(leased),
            },
        );
        same(
            Reply::Completed {
                results: results.clone(),
                next: None,
            },
            Response::Completed {
                results,
                next: None,
            },
        );

        let id = TaskId::new(7, 42, 3);
        let err: WireError = Error::invalid_argument("PayloadTooLarge").into();
        same(
            Reply::Ids(vec![Ok(id), Err(err.clone())]),
            Response::Query(QueryResponse::Ids(vec![Ok(id.to_string()), Err(err)])),
        );
        same(
            Response::Cancelled { results: vec![] }.into(),
            Response::Cancelled { results: vec![] },
        );
    }

    #[test]
    fn decoded_leased_view_matches() {
        let view = view();
        let leased = owned(&view);
        let bytes = encode_reply(1, &Reply::Leased(view), 0).unwrap();
        match decode::<Response>(&bytes).unwrap().body {
            Response::Leased(decoded) => {
                assert_eq!(decoded.tasks.len(), 3);
                assert_eq!(decoded.tasks[1].bytes, b"cccc");
                assert_eq!(
                    decoded.tasks[2].range,
                    Some(DerivedRange { start: 5, count: 9 })
                );
                assert_eq!(
                    decoded.tasks.iter().map(|t| &t.task_id).collect::<Vec<_>>(),
                    leased.tasks.iter().map(|t| &t.task_id).collect::<Vec<_>>()
                );
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    /// Every `Request` decodes as the matching `RequestKind` and `RequestIn`.
    #[test]
    fn request_in_mirrors_request() {
        let id = TaskId::new(7, 42, 3).to_string();
        let requests = [
            (
                RequestKind::Hello,
                Request::Hello {
                    proto_ver: PROTO_VERSION,
                    auth: Auth {
                        uid: "u".into(),
                        challenge: "c".into(),
                        admin_token: Some("t".into()),
                    },
                    caps: Caps {
                        runners: vec![RunnerKind::Hip],
                        max_inflight: 3,
                    },
                },
            ),
            (
                RequestKind::Lease,
                Request::Lease {
                    task_type: ty(),
                    max_tasks: 5,
                    wait_ms: 6,
                },
            ),
            (
                RequestKind::Renew,
                Request::Renew {
                    leases: vec![lease_ref(&id, 1)],
                },
            ),
            (
                RequestKind::Complete,
                Request::Complete {
                    results: vec![
                        TaskResult {
                            lease: lease_ref(&id, 2),
                            bytes: b"result".to_vec(),
                        },
                        TaskResult {
                            lease: lease_ref("x", 3),
                            bytes: Vec::new(),
                        },
                    ],
                    then_lease: Some(LeaseReq {
                        task_type: ty(),
                        max_tasks: 8,
                    }),
                },
            ),
            (
                RequestKind::CancelLease,
                Request::CancelLease {
                    leases: vec![lease_ref(&id, 4)],
                },
            ),
            (
                RequestKind::Query,
                Request::Query(Query::Insert {
                    task_type: ty(),
                    payloads: vec![b"p1".to_vec(), Vec::new(), vec![0xff; 300]],
                }),
            ),
            (
                RequestKind::Query,
                Request::Query(Query::InsertDerived {
                    task_type: ty(),
                    base: b"base".to_vec(),
                    start: 1,
                    count: 2,
                }),
            ),
            (
                RequestKind::Query,
                Request::Query(Query::Get {
                    task_type: ty(),
                    ids: vec![id.clone()],
                }),
            ),
            (
                RequestKind::Query,
                Request::Query(Query::Scan {
                    task_type: ty(),
                    scope: Scope::Finished,
                    after: Some(id.clone()),
                    limit: 9,
                }),
            ),
            (RequestKind::Query, Request::Query(Query::EstimateKeys)),
            (
                RequestKind::Query,
                Request::Query(Query::EstimateSize {
                    task_type: ty(),
                    scope: Scope::Pending,
                }),
            ),
            (
                RequestKind::Query,
                Request::Query(Query::Delete {
                    task_type: ty(),
                    scope: Scope::Pending,
                    ids: vec![id.clone()],
                }),
            ),
            (
                RequestKind::Query,
                Request::Query(Query::Purge {
                    task_type: ty(),
                    scope: Scope::Finished,
                }),
            ),
            (RequestKind::Query, Request::Query(Query::Stats)),
        ];
        for (kind, request) in requests {
            let bytes = encode_in_session(3, 77, &request).unwrap();
            let head = decode::<RequestKind>(&bytes).unwrap();
            assert_eq!((head.req_id, head.session, head.body), (3, 77, kind));
            let body = decode::<RequestIn>(&bytes).unwrap().body;
            // Field-by-field: the borrowed view must see the same values.
            let same = match (&request, &body) {
                (
                    Request::Hello {
                        proto_ver, auth, ..
                    },
                    RequestIn::Hello {
                        proto_ver: p,
                        auth: a,
                        ..
                    },
                ) => proto_ver == p && auth.uid == a.uid && auth.admin_token == a.admin_token,
                (
                    Request::Lease {
                        task_type,
                        max_tasks,
                        wait_ms,
                    },
                    RequestIn::Lease {
                        task_type: t,
                        max_tasks: m,
                        wait_ms: w,
                    },
                ) => task_type == t && max_tasks == m && wait_ms == w,
                (Request::Renew { leases }, RequestIn::Renew { leases: l })
                | (Request::CancelLease { leases }, RequestIn::CancelLease { leases: l }) => {
                    leases == l
                }
                (
                    Request::Complete {
                        results,
                        then_lease,
                    },
                    RequestIn::Complete {
                        results: r,
                        then_lease: t,
                    },
                ) => {
                    results.len() == r.len()
                        && results
                            .iter()
                            .zip(r)
                            .all(|(a, b)| a.lease.as_in() == b.lease && a.bytes == b.bytes)
                        && then_lease.as_ref().map(|l| l.max_tasks)
                            == t.as_ref().map(|l| l.max_tasks)
                }
                (Request::Query(query), RequestIn::Query(q)) => match (query, q) {
                    (
                        Query::Insert {
                            task_type,
                            payloads,
                        },
                        QueryIn::Insert {
                            task_type: t,
                            payloads: p,
                        },
                    ) => task_type == t && payloads.iter().map(Vec::as_slice).eq(p.iter().copied()),
                    (
                        Query::InsertDerived {
                            base, start, count, ..
                        },
                        QueryIn::InsertDerived {
                            base: b,
                            start: s,
                            count: c,
                            ..
                        },
                    ) => base == b && start == s && count == c,
                    (Query::Get { ids, .. }, QueryIn::Get { ids: i, .. })
                    | (Query::Delete { ids, .. }, QueryIn::Delete { ids: i, .. }) => ids == i,
                    (
                        Query::Scan {
                            scope,
                            after,
                            limit,
                            ..
                        },
                        QueryIn::Scan {
                            scope: s,
                            after: a,
                            limit: l,
                            ..
                        },
                    ) => scope == s && after == a && limit == l,
                    (Query::EstimateSize { scope, .. }, QueryIn::EstimateSize { scope: s, .. })
                    | (Query::Purge { scope, .. }, QueryIn::Purge { scope: s, .. }) => scope == s,
                    (Query::EstimateKeys, QueryIn::EstimateKeys)
                    | (Query::Stats, QueryIn::Stats) => true,
                    _ => false,
                },
                _ => false,
            };
            assert!(same, "{request:?} decoded as {body:?}");
        }
    }

    /// The `bytes` helpers leave the encoding of a plain `Vec<u8>` unchanged.
    #[test]
    fn byte_fields_encode_like_vec() {
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct Plain {
            one: Vec<u8>,
            opt: Option<Vec<u8>>,
            none: Option<Vec<u8>>,
            many: Vec<Vec<u8>>,
        }
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct Fast {
            #[serde(with = "bytes")]
            one: Vec<u8>,
            #[serde(with = "bytes::option")]
            opt: Option<Vec<u8>>,
            #[serde(with = "bytes::option")]
            none: Option<Vec<u8>>,
            #[serde(with = "bytes::seq")]
            many: Vec<Vec<u8>>,
        }
        let big: Vec<u8> = (0..=255).cycle().take(1000).collect();
        let plain = Plain {
            one: big.clone(),
            opt: Some(b"x".to_vec()),
            none: None,
            many: vec![Vec::new(), big.clone()],
        };
        let fast = Fast {
            one: big.clone(),
            opt: Some(b"x".to_vec()),
            none: None,
            many: vec![Vec::new(), big],
        };
        let encoded = postcard::to_allocvec(&plain).unwrap();
        assert_eq!(postcard::to_allocvec(&fast).unwrap(), encoded);
        assert_eq!(postcard::from_bytes::<Fast>(&encoded).unwrap(), fast);
    }
}
