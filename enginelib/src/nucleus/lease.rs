use std::sync::Arc;

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
    if max == 0 {
        return Ok(Vec::new());
    }

    // Clone the receiver out of the map guard before awaiting (see `lease`).
    let receiver = api
        .task_queue
        .tasks
        .get(&task_type)
        .ok_or(Error::not_found("TaskTypeNotFound"))?
        .1
        .clone();

    let mut out: Vec<Arc<StoredTask>> = Vec::new();
    let first = match tokio::time::timeout(
        std::time::Duration::from_secs(LEASE_LONG_POLL_SECS),
        receiver.recv(),
    )
    .await
    {
        Err(_elapsed) => return Ok(Vec::new()),
        Ok(recv) => recv.map_err(|err| Error::new(format!("Task queue closed: {err}")))?,
    };
    out.push(Arc::new(first));
    while (out.len() as u32) < max {
        match receiver.try_recv() {
            Ok(task) => out.push(Arc::new(task)),
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
