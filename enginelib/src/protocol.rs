//! Wire protocol shared by the ZeroMQ server and client.
//!
//! Everything here is pure `serde` — no transport dependency — so both the
//! `engine` server/client and any other consumer share one source of truth for
//! the wire format. Messages are postcard-serialized via the project serializer
//! ([`crate::api::to_allocvec`] / [`crate::api::from_bytes`]).
//!
//! The protocol is **batch-first**: a singular submit/lease/complete is just a
//! batch of size one, so a single ZeroMQ message can carry many tasks. That is
//! the key lever for sustaining 100k+ tasks/s — it collapses the per-message
//! ZeroMQ + syscall cost across a whole batch.

use serde::{Deserialize, Serialize};

use crate::Identifier;
use crate::api::StoredTask;
use crate::error::Error;
use crate::nucleus::query::{Query, QueryResult};

/// A request plus the caller's auth token, as sent on the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    /// Caller auth token, checked server-side before dispatch.
    pub auth: String,
    /// The requested operation.
    pub req: Request,
}

/// A client → server operation. Batch variants carry N items in one message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    /// Submit N task payloads of one type. Singular submit = a one-element vec.
    SubmitBatch {
        task_type: Identifier,
        tasks: Vec<Vec<u8>>,
    },
    /// Lease up to `max` tasks of one type for `user_id` (long-poll: blocks for
    /// the first, then drains the rest without waiting).
    LeaseBatch {
        task_type: Identifier,
        user_id: String,
        max: u32,
    },
    /// Complete N leased tasks: `(task_id, result_bytes)` pairs.
    CompleteBatch {
        task_type: Identifier,
        results: Vec<(String, Vec<u8>)>,
    },
    /// Renew a single lease's TTL.
    Renew {
        task_type: Identifier,
        task_id: String,
    },
    /// Cancel a single lease (requeues the task).
    Cancel {
        task_type: Identifier,
        task_id: String,
    },
    /// Administrative / CRUD query over the task store.
    Query(Query),
}

/// A server → client reply.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    /// Ids assigned to a `SubmitBatch`, in request order.
    Submitted { task_ids: Vec<String> },
    /// Tasks handed out by a `LeaseBatch` (may be fewer than `max`).
    Leased(Vec<StoredTask>),
    /// A `CompleteBatch` finished; `ok` is how many were completed.
    Completed { ok: u32 },
    /// A `Renew` succeeded.
    Renewed,
    /// A `Cancel` succeeded.
    Cancelled,
    /// A `Query` result.
    Query(QueryResult),
    /// The transport is at capacity. Mutations were not executed and may be
    /// retried after the suggested delay.
    Overloaded { retry_after_ms: u64 },
    /// The operation failed.
    Err(Error),
}

/// postcard-encode any wire value, mapping serializer failure to [`Error`].
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, Error> {
    crate::api::to_allocvec(value).map_err(|err| Error::new(format!("encode failed: {err}")))
}

/// postcard-decode a wire value, mapping malformed input to an invalid-argument
/// [`Error`].
pub fn decode<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> Result<T, Error> {
    crate::api::from_bytes(bytes)
        .map_err(|err| Error::invalid_argument(format!("decode failed: {err}")))
}

impl From<Error> for Response {
    fn from(err: Error) -> Self {
        Response::Err(err)
    }
}
