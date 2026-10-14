use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;

use crate::api::{Lease, LeasedTask, next_lease_token};
use crate::{Identifier, api::ServerAPI, error::Error};

/// Leases the next queued task of `task_type`, waiting as long as it takes for
/// one to arrive. Prefer [`lease_timeout`] for anything driven by a client.
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

async fn lease_inner(
    api: Arc<ServerAPI>,
    task_type: Identifier,
    user_id: String,
    timeout: Option<Duration>,
) -> Result<Lease, Error> {
    // Clone the receiver out of the map guard before awaiting; recv() on an
    // empty queue would otherwise hold the shard lock indefinitely.
    let (receiver, capacity) = {
        let queue = api
            .task_queue
            .tasks
            .get(&task_type)
            .ok_or(Error::not_found("TaskTypeNotFound"))?;
        (queue.1.clone(), queue.0.capacity())
    };

    // Top the queue up from RocksDB once it runs low. Requested before awaiting
    // so a worker parked on an empty queue is woken by the refill it triggered.
    if ServerAPI::below_watermark(receiver.len(), capacity) {
        ServerAPI::request_refill(&api, &task_type);
    }

    let received = match timeout {
        Some(timeout) => tokio::time::timeout(timeout, receiver.recv())
            .await
            .map_err(|_| Error::not_found("QueueEmpty"))?,
        None => receiver.recv().await,
    };
    let task = received.map_err(|err| Error::new(format!("Task queue closed: {err}")))?;

    // Ask again after taking a task: other workers may be parked on this queue
    // with their own pre-wait requests already folded into the refill that
    // produced this task, and a small queue can't hold enough for all of them.
    if ServerAPI::below_watermark(receiver.len(), capacity) {
        ServerAPI::request_refill(&api, &task_type);
    }

    // No await from here on: the task goes from "queued" straight to "leased"
    // (its task_id stays in the dedup set throughout).
    let stored_task = Arc::new(task);
    let given_at = Utc::now();
    let lease = Lease {
        task: stored_task.clone(),
        token: next_lease_token(),
        expires_at: api.lease_expiry(given_at),
    };
    api.leased_tasks.leases_or_default(&task_type).insert(
        stored_task.task_id.clone(),
        LeasedTask {
            stored_task,
            user_id,
            token: lease.token,
            given_at,
            expires_at: lease.expires_at,
        },
    );

    Ok(lease)
}
