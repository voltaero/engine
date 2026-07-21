use std::sync::Arc;

use crate::api::{finished_task_key, task_key};
use crate::{Identifier, Registry, api::ServerAPI, error::Error};

pub fn complete(
    api: Arc<ServerAPI>,
    task_type: Identifier,
    task_id: String,
    task_bytes: &[u8],
) -> Result<(), Error> {
    let task = api
        .task_registry
        .get(&task_type)
        .ok_or(Error::not_found("TaskTypeNotFound"))?;
    if !task.verify(task_bytes) {
        return Err(Error::invalid_argument("Failed to verify task bytes"));
    }

    // A task can only be completed while it is leased to a worker; drop the
    // lease so the reaper can't later re-enqueue it.
    let mut leased = false;
    if let Some(mut leases) = api.leased_tasks.tasks.get_mut(&task_type) {
        let before = leases.len();
        leases.retain(|lease| lease.stored_task.task_id != task_id);
        leased = leases.len() != before;
    }
    if !leased {
        return Err(Error::not_found("TaskNotLeased"));
    }

    // The lease is already gone, so every remaining exit path must clear the
    // dedup entry. On success it prevents load() from re-enqueuing a task whose
    // pending record is deleted; on DB failure it lets a later load() re-enqueue
    // the *still-persisted* pending record, instead of stranding it (no lease, no
    // channel slot, but blocked from re-enqueue by the dedup entry) until restart.
    let clear_dedup = || {
        if let Some(queue) = api.task_queue.tasks.get(&task_type) {
            queue.2.remove(&task_id);
        }
    };

    // Persist the finished record before deleting the pending one so a crash
    // between the two writes never loses the result.
    if let Err(err) = api
        .db
        .put(finished_task_key(&task_type, &task_id), task_bytes)
    {
        clear_dedup();
        return Err(Error::io_error(format!(
            "Failed to persist finished task: {err}"
        )));
    }
    if let Err(err) = api.db.delete(task_key(&task_type, &task_id)) {
        clear_dedup();
        return Err(Error::io_error(format!("Failed to delete task record: {err}")));
    }

    clear_dedup();

    Ok(())
}
