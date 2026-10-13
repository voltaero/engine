use std::sync::Arc;

use crate::{Identifier, api::ServerAPI, error::Error};

/// Cancels the *lease* on a task, not the task itself: removes it from the lease
/// map and resends it into the queue so another worker can pick it up. Only the
/// current lease holder (matching `lease_token`) may cancel.
///
/// The persisted record is never touched, and the task_id stays in the dedup set
/// the whole time, so no concurrent `load()` can enqueue a duplicate. The requeue
/// is a non-blocking `try_send` so there is no await window in which the lease is
/// gone but the task isn't yet back in the channel: on success the task moves
/// straight from "leased" to "queued"; if the channel is full or closed we clear
/// the dedup entry instead, dropping the task back to "persisted only" so a later
/// refill recovers it. Either way the task is never stranded.
pub fn cancel(
    api: Arc<ServerAPI>,
    task_type: Identifier,
    task_id: String,
    lease_token: u64,
) -> Result<(), Error> {
    // Remove the lease, capturing the stored task so we can requeue it.
    let stored_task = {
        let leases = api
            .leased_tasks
            .leases(&task_type)
            .ok_or(Error::not_found("TaskNotFound"))?;
        match leases.remove_if(&task_id, |_, lease| lease.token == lease_token) {
            Some((_, lease)) => lease.stored_task,
            None if leases.contains_key(&task_id) => {
                return Err(Error::invalid_argument("LeaseTokenMismatch"));
            }
            None => return Err(Error::not_found("TaskNotFound")),
        }
    };

    let queue = api
        .task_queue
        .tasks
        .get(&task_type)
        .ok_or(Error::not_found("TaskTypeNotFound"))?;

    // Non-blocking requeue: no await means the lease removal and the requeue
    // can't be split by an async cancellation.
    match queue.0.try_send((*stored_task).clone()) {
        // Queued again; task_id stays in the dedup set (still in-flight).
        Ok(()) => Ok(()),
        // Channel full/closed: clear the dedup entry so the still-persisted
        // record is re-enqueued by a later refill. The refill cursor may already
        // be past its key, so force a full rescan. Never left stranded.
        Err(_) => {
            queue.2.remove(&task_id);
            drop(queue);
            api.load_state.get(&task_type).mark_rescan();
            Ok(())
        }
    }
}
