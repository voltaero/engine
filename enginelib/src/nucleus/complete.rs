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

    // Persist the finished record before deleting the pending one so a crash
    // between the two writes never loses the result.
    api.db
        .put(finished_task_key(&task_type, &task_id), task_bytes)
        .map_err(|err| Error::io_error(format!("Failed to persist finished task: {err}")))?;
    api.db
        .delete(task_key(&task_type, &task_id))
        .map_err(|err| Error::io_error(format!("Failed to delete task record: {err}")))?;

    // Clear the dedup entry last so a concurrent load() cannot re-enqueue a task
    // whose pending record is already gone.
    if let Some(queue) = api.task_queue.tasks.get(&task_type) {
        queue.2.remove(&task_id);
    }

    Ok(())
}
