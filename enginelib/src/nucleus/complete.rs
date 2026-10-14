use std::sync::Arc;

use rust_rocksdb::WriteBatch;

use crate::api::{finished_task_key, task_key};
use crate::{Identifier, Registry, api::ServerAPI, error::Error};

/// Finalizes a leased task with its result bytes. Only the current lease holder
/// (matching `lease_token`) may complete.
///
/// Idempotent for retries: if the lease is already gone but the finished record
/// holds exactly `task_bytes` (an earlier call succeeded and its response was
/// lost), this returns Ok without touching anything.
pub fn complete(
    api: Arc<ServerAPI>,
    task_type: Identifier,
    task_id: String,
    lease_token: u64,
    task_bytes: &[u8],
) -> Result<(), Error> {
    let task = api
        .task_registry
        .get(&task_type)
        .ok_or(Error::not_found("TaskTypeNotFound"))?;
    if !task.verify(task_bytes) {
        return Err(Error::invalid_argument("Failed to verify task bytes"));
    }

    // A task can only be completed while it is leased to the caller; drop the
    // lease so the reaper can't later re-enqueue it. Matching on the token fences
    // off a worker whose lease was reaped and the task re-leased to another.
    let leases = api.leased_tasks.leases(&task_type);
    let leased = leases.as_ref().is_some_and(|leases| {
        leases
            .remove_if(&task_id, |_, lease| lease.token == lease_token)
            .is_some()
    });
    if !leased {
        let held_by_other = leases.is_some_and(|leases| leases.contains_key(&task_id));
        if held_by_other {
            return Err(Error::invalid_argument("LeaseTokenMismatch"));
        }
        return already_completed(&api, &task_type, &task_id, task_bytes);
    }

    // The lease is already gone, so every remaining exit path must clear the
    // dedup entry. On success it prevents a refill from re-enqueuing a task whose
    // pending record is deleted; on DB failure it lets a later refill re-enqueue
    // the *still-persisted* pending record, instead of stranding it (no lease, no
    // channel slot, but blocked from re-enqueue by the dedup entry) until restart.
    let clear_dedup = || {
        if let Some(queue) = api.task_queue.tasks.get(&task_type) {
            queue.2.remove(&task_id);
        }
    };

    // Write the finished record and delete the pending one atomically, so a crash
    // can neither lose the result nor leave a finished task still pending.
    let mut batch = WriteBatch::default();
    batch.put(finished_task_key(&task_type, &task_id), task_bytes);
    batch.delete(task_key(&task_type, &task_id));
    if let Err(err) = api.db.write(&batch) {
        clear_dedup();
        // The refill cursor may already be past this key: force a full rescan.
        api.load_state.get(&task_type).mark_rescan();
        // Workers already parked in lease() won't ask again: re-enqueue the
        // still-persisted record now.
        ServerAPI::request_refill(&api, &task_type);
        return Err(Error::io_error(format!(
            "Failed to persist finished task: {err}"
        )));
    }

    clear_dedup();

    Ok(())
}

/// No lease matched: succeed only if this is a retry of a completion that
/// already persisted the identical result.
fn already_completed(
    api: &ServerAPI,
    task_type: &Identifier,
    task_id: &str,
    task_bytes: &[u8],
) -> Result<(), Error> {
    let finished = api
        .db
        .get(finished_task_key(task_type, task_id))
        .map_err(|err| Error::io_error(format!("Failed to read finished task: {err}")))?;
    match finished {
        Some(stored) if stored == task_bytes => Ok(()),
        _ => Err(Error::not_found("TaskNotLeased")),
    }
}
