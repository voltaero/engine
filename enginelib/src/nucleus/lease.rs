use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;

use crate::api::{ChunkLease, Lease, QueuedChunk, TypeQueue, next_lease_token};
use crate::{Identifier, api::ServerAPI, error::Error};

/// Leases the next queued chunk of `task_type` (all of its open tasks, under
/// one token), waiting as long as it takes for one to arrive. Prefer
/// [`lease_timeout`] for anything driven by a client.
pub async fn lease(
    api: Arc<ServerAPI>,
    task_type: Identifier,
    user_id: String,
) -> Result<Lease, Error> {
    lease_inner(api, task_type, user_id, None).await
}

/// Like [`lease`], but gives up after `timeout` with a NotFound "QueueEmpty".
pub async fn lease_timeout(
    api: Arc<ServerAPI>,
    task_type: Identifier,
    user_id: String,
    timeout: Duration,
) -> Result<Lease, Error> {
    lease_inner(api, task_type, user_id, Some(timeout)).await
}

/// Leases the next queued chunk if one is ready right now; never waits.
pub fn try_lease(
    api: &Arc<ServerAPI>,
    task_type: &Identifier,
    user_id: &str,
) -> Result<Option<Lease>, Error> {
    let queue = queue_of(api, task_type)?;
    refill_if_low(api, task_type, &queue);
    match queue.rx.try_recv() {
        Ok(chunk) => Ok(Some(grant(
            api,
            task_type,
            &queue,
            user_id.to_string(),
            chunk,
        ))),
        Err(async_channel::TryRecvError::Empty) => Ok(None),
        Err(err) => Err(Error::new(format!("Task queue closed: {err}"))),
    }
}

/// Leases whole chunks until at least `max_tasks` tasks are leased or the
/// queue is empty. Waits up to `wait` for the first chunk only. Chunks are
/// never split, so the total may exceed `max_tasks` by up to one chunk.
pub async fn lease_many(
    api: Arc<ServerAPI>,
    task_type: Identifier,
    user_id: String,
    max_tasks: usize,
    wait: Duration,
) -> Result<Vec<Lease>, Error> {
    let first = lease_timeout(api.clone(), task_type.clone(), user_id.clone(), wait).await?;
    let mut total = first.len();
    let mut leases = vec![first];
    while total < max_tasks {
        match try_lease(&api, &task_type, &user_id)? {
            Some(lease) => {
                total += lease.len();
                leases.push(lease);
            }
            None => break,
        }
    }
    Ok(leases)
}

fn queue_of(api: &ServerAPI, task_type: &Identifier) -> Result<Arc<TypeQueue>, Error> {
    api.task_queue
        .get(task_type)
        .ok_or(Error::not_found("TaskTypeNotFound"))
}

/// Tops the queue up from RocksDB once it runs low.
fn refill_if_low(api: &Arc<ServerAPI>, task_type: &Identifier, queue: &TypeQueue) {
    if ServerAPI::below_watermark(queue.rx.len(), queue.tx.capacity()) {
        ServerAPI::request_refill(api, task_type);
    }
}

async fn lease_inner(
    api: Arc<ServerAPI>,
    task_type: Identifier,
    user_id: String,
    timeout: Option<Duration>,
) -> Result<Lease, Error> {
    let queue = queue_of(&api, &task_type)?;
    // Requested before awaiting so a worker parked on an empty queue is woken
    // by the refill it triggered.
    refill_if_low(&api, &task_type, &queue);

    let received = match timeout {
        Some(timeout) => tokio::time::timeout(timeout, queue.rx.recv())
            .await
            .map_err(|_| Error::not_found("QueueEmpty"))?,
        None => queue.rx.recv().await,
    };
    let chunk = received.map_err(|err| Error::new(format!("Task queue closed: {err}")))?;
    // Ask again after taking a chunk: other workers may be parked on this queue
    // with their own pre-wait requests already folded into the refill that
    // produced this chunk, and a small queue can't hold enough for all of them.
    refill_if_low(&api, &task_type, &queue);
    Ok(grant(&api, &task_type, &queue, user_id, chunk))
}

/// Records the lease of a just-received chunk. No await: the chunk goes from
/// "queued" straight to "leased" (it stays in the dedup set throughout).
fn grant(
    api: &ServerAPI,
    task_type: &Identifier,
    queue: &TypeQueue,
    user_id: String,
    chunk: QueuedChunk,
) -> Lease {
    queue.note_received(&chunk);
    let given_at = Utc::now();
    let lease = Lease {
        task_type: task_type.clone(),
        server_id: api.server_id(),
        chunk_seq: chunk.chunk_seq,
        payloads: chunk.payloads.clone(),
        open: chunk.open.clone(),
        token: next_lease_token(),
        expires_at: api.lease_expiry(given_at),
    };
    api.leased_tasks.leases_or_default(task_type).insert(
        chunk.chunk_seq,
        Arc::new(Mutex::new(ChunkLease {
            chunk,
            user_id,
            token: lease.token,
            given_at,
            expires_at: lease.expires_at,
            gone: false,
        })),
    );
    lease
}
