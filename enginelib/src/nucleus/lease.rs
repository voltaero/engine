use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use chrono::Utc;

use crate::api::{LeasedTask, StoredTask};
use crate::{Identifier, api::ServerAPI, error::Error};

pub async fn lease(
    api: Arc<ServerAPI>,
    task_type: Identifier,
    user_id: String,
) -> Result<Arc<StoredTask>, Error> {
    // Clone the receiver out of the map guard before awaiting; recv() on an
    // empty queue would otherwise hold the shard lock indefinitely.
    let receiver = api
        .task_queue
        .tasks
        .get(&task_type)
        .ok_or(Error::not_found("TaskTypeNotFound"))?
        .1
        .clone();

    let task = receiver
        .recv()
        .await
        .map_err(|err| Error::new(format!("Task queue closed: {err}")))?;

    let stored_task = Arc::new(task);
    api.leased_tasks
        .tasks
        .entry(task_type)
        .or_default()
        .push(LeasedTask {
            stored_task: stored_task.clone(),
            user_id,
            given_at: Utc::now(),
        });

    Ok(stored_task)
}

/// How long `lease_batch` waits for the first task before returning empty.
/// Bounds the damage of a dead client: a handler parked on an empty queue gives
/// up after this window instead of sitting on `recv()` forever and having a
/// future task handed to a peer that disconnected long ago (which would sit
/// leased to nobody for the whole TTL). Clients treat an empty reply as "poll
/// again".
const LEASE_LONG_POLL_SECS: u64 = 30;

/// Lease up to `max` tasks of one type in a single call.
///
/// Long-poll: awaits the first task (up to [`LEASE_LONG_POLL_SECS`]) so idle
/// workers don't busy-poll, then drains the rest of what's already queued
/// without waiting. Returns 0..=`max` tasks; empty means the poll window
/// elapsed (or `max == 0`) and the caller should lease again. Reads only the
/// in-memory channel — the loader is what keeps that channel fed from the DB.
pub async fn lease_batch(
    api: Arc<ServerAPI>,
    task_type: Identifier,
    user_id: String,
    max: u32,
) -> Result<Vec<Arc<StoredTask>>, Error> {
    lease_batch_limited(
        api,
        task_type,
        user_id,
        max,
        usize::MAX,
        Duration::from_secs(LEASE_LONG_POLL_SECS),
    )
    .await
}

/// Transport-facing lease with explicit response-byte and long-poll bounds.
/// Tasks that would exceed the byte budget are put back without changing their
/// dedup state, so the caller never receives an undeliverable lease.
pub async fn lease_batch_limited(
    api: Arc<ServerAPI>,
    task_type: Identifier,
    user_id: String,
    max: u32,
    max_bytes: usize,
    poll_timeout: Duration,
) -> Result<Vec<Arc<StoredTask>>, Error> {
    if max == 0 {
        return Ok(Vec::new());
    }

    // Clone both channel halves out of the map guard before awaiting.
    let (sender, receiver) = {
        let queue = api
            .task_queue
            .tasks
            .get(&task_type)
            .ok_or(Error::not_found("TaskTypeNotFound"))?;
        (queue.0.clone(), queue.1.clone())
    };

    let first = match tokio::time::timeout(poll_timeout, receiver.recv()).await {
        Err(_elapsed) => return Ok(Vec::new()),
        Ok(recv) => recv.map_err(|err| Error::new(format!("Task queue closed: {err}")))?,
    };

    let mut used_bytes = estimated_wire_bytes(&first);
    if used_bytes > max_bytes {
        requeue_unleased(&api, &task_type, &sender, first);
        return Err(Error::invalid_argument(
            "Stored task exceeds the configured lease response limit",
        ));
    }

    let mut out: Vec<Arc<StoredTask>> = vec![Arc::new(first)];
    while (out.len() as u32) < max {
        match receiver.try_recv() {
            Ok(task) => {
                let task_bytes = estimated_wire_bytes(&task);
                if used_bytes.saturating_add(task_bytes) > max_bytes {
                    requeue_unleased(&api, &task_type, &sender, task);
                    break;
                }
                used_bytes = used_bytes.saturating_add(task_bytes);
                out.push(Arc::new(task));
            }
            Err(_) => break,
        }
    }

    {
        let now = Utc::now();
        let mut leases = api.leased_tasks.tasks.entry(task_type).or_default();
        for stored_task in &out {
            leases.push(LeasedTask {
                stored_task: stored_task.clone(),
                user_id: user_id.clone(),
                given_at: now,
            });
        }
    }

    Ok(out)
}

fn estimated_wire_bytes(task: &StoredTask) -> usize {
    task.bytes
        .len()
        .saturating_add(task.task_id.len())
        .saturating_add(task.task_type.0.len())
        .saturating_add(task.task_type.1.len())
        .saturating_add(64)
}

fn requeue_unleased(
    api: &Arc<ServerAPI>,
    task_type: &Identifier,
    sender: &async_channel::Sender<StoredTask>,
    task: StoredTask,
) {
    if let Err(err) = sender.try_send(task)
        && let Some(queue) = api.task_queue.tasks.get(task_type)
    {
        queue.2.remove(&err.into_inner().task_id);
        queue.3.store(true, Ordering::Release);
    }
}
