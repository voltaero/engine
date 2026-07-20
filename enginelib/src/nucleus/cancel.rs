use std::sync::Arc;

use crate::api::task_key;
use crate::{Identifier, api::ServerAPI, error::Error};

pub fn cancel(api: Arc<ServerAPI>, task_type: Identifier, task_id: String) -> Result<(), Error> {
    let mut removed = false;
    if let Some(mut leases) = api.leased_tasks.tasks.get_mut(&task_type) {
        let before = leases.len();
        leases.retain(|lease| lease.stored_task.task_id != task_id);
        removed = leases.len() != before;
    }

    // Delete the persisted record before clearing the dedup entry so a
    // concurrent load() cannot re-enqueue the cancelled task.
    api.db
        .delete(task_key(&task_type, &task_id))
        .map_err(|err| Error::io_error(format!("Failed to delete task record: {err}")))?;
    if let Some(queue) = api.task_queue.tasks.get(&task_type) {
        removed |= queue.2.remove(&task_id).is_some();
    }

    if removed {
        Ok(())
    } else {
        Err(Error::not_found("TaskNotFound"))
    }
}
