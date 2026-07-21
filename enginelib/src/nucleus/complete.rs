use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use rust_rocksdb::{WriteBatch, WriteOptions};

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
    // dedup entry. On success it prevents the loader from re-enqueuing a task
    // whose pending record is deleted. On DB failure the pending record still
    // exists but sits *behind* the loader's cursor, so clearing the entry alone
    // would strand it (no lease, no channel slot) until restart — the rescan
    // flag tells the loader to rewind and re-enqueue it.
    let clear_dedup = |rescan: bool| {
        if let Some(queue) = api.task_queue.tasks.get(&task_type) {
            queue.2.remove(&task_id);
            if rescan {
                queue.3.store(true, Ordering::Release);
            }
        }
    };

    // Persist the finished record before deleting the pending one so a crash
    // between the two writes never loses the result.
    if let Err(err) = api
        .db
        .put(finished_task_key(&task_type, &task_id), task_bytes)
    {
        clear_dedup(true);
        return Err(Error::io_error(format!(
            "Failed to persist finished task: {err}"
        )));
    }
    if let Err(err) = api.db.delete(task_key(&task_type, &task_id)) {
        clear_dedup(true);
        return Err(Error::io_error(format!("Failed to delete task record: {err}")));
    }

    clear_dedup(false);

    Ok(())
}

/// Complete a batch of leased tasks in one fused write, returning how many
/// succeeded.
///
/// All `f:` puts and `t:` deletes go into a single RocksDB `WriteBatch` (one WAL
/// append instead of two per task). Only tasks actually leased are written;
/// unleased/duplicate ids are skipped. Deleting the `t:` record is what stops the
/// loader from ever re-enqueuing a completed task; clearing the dedup entry keeps
/// the loader's in-flight set accurate. On write failure the dedup entries are
/// cleared so the loader re-enqueues the still-persisted records rather than
/// stranding them (the rescan flag makes the loader rewind to find them).
pub fn complete_batch(
    api: Arc<ServerAPI>,
    task_type: Identifier,
    results: Vec<(String, Vec<u8>)>,
) -> Result<u32, Error> {
    let task = api
        .task_registry
        .get(&task_type)
        .ok_or(Error::not_found("TaskTypeNotFound"))?;
    for (_, bytes) in &results {
        if !task.verify(bytes) {
            return Err(Error::invalid_argument("Failed to verify task bytes"));
        }
    }

    // Drop the lease for every requested id in one pass; record which were leased.
    let requested: HashSet<&str> = results.iter().map(|(id, _)| id.as_str()).collect();
    let mut leased: HashSet<String> = HashSet::new();
    if let Some(mut leases) = api.leased_tasks.tasks.get_mut(&task_type) {
        leases.retain(|lease| {
            let id = &lease.stored_task.task_id;
            if requested.contains(id.as_str()) {
                leased.insert(id.clone());
                false
            } else {
                true
            }
        });
    }
    if leased.is_empty() {
        return Ok(0);
    }

    let mut batch = WriteBatch::default();
    let mut written: Vec<String> = Vec::with_capacity(leased.len());
    for (task_id, bytes) in &results {
        // `remove` (not `contains`) so a duplicate id in one request is written
        // and counted exactly once.
        if leased.remove(task_id) {
            batch.put(finished_task_key(&task_type, task_id), bytes);
            batch.delete(task_key(&task_type, task_id));
            written.push(task_id.clone());
        }
    }
    let mut wopts = WriteOptions::default();
    wopts.set_sync(false);

    let clear_dedup = |rescan: bool| {
        if let Some(queue) = api.task_queue.tasks.get(&task_type) {
            for task_id in &written {
                queue.2.remove(task_id);
            }
            if rescan {
                queue.3.store(true, Ordering::Release);
            }
        }
    };

    if let Err(err) = api.db.write_opt(&batch, &wopts) {
        clear_dedup(true);
        return Err(Error::io_error(format!(
            "Failed to persist completed batch: {err}"
        )));
    }
    clear_dedup(false);

    Ok(written.len() as u32)
}
