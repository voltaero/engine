use std::collections::HashMap;
use std::sync::Arc;

use crate::api::lock_lease;
use crate::{Identifier, api::ServerAPI, error::Error};

/// Cancels the *lease* on the chunk holding `task_id`, not the tasks: drops the
/// lease and puts the chunk's still-open tasks back into the queue so another
/// worker can pick them up. Leases are per chunk, so this gives back every
/// open task of that lease. Only the current lease holder (matching
/// `lease_token`) may cancel.
///
/// The persisted records are never touched, and the chunk stays in the dedup
/// set the whole time, so no concurrent refill can enqueue a duplicate. The
/// requeue is a non-blocking `try_send` so there is no await window in which
/// the lease is gone but the chunk isn't back in the channel; if the channel is
/// full or closed the chunk is un-marked and handed to the refill's recovery
/// set instead. Either way it is never stranded.
pub fn cancel(
    api: Arc<ServerAPI>,
    task_type: Identifier,
    task_id: String,
    lease_token: u64,
) -> Result<(), Error> {
    let id = api.parse_task_id(&task_id)?;
    cancel_chunk(&api, &task_type, id.chunk_seq, lease_token)
}

/// Batch form of [`cancel`]: one result per entry, in order. Entries of the
/// same chunk lease cancel it once and share the outcome.
pub fn cancel_batch(
    api: &Arc<ServerAPI>,
    leases: impl IntoIterator<Item = (Identifier, String, u64)>,
) -> Vec<Result<(), Error>> {
    let mut done: HashMap<(Identifier, u64, u64), Result<(), Error>> = HashMap::new();
    leases
        .into_iter()
        .map(|(task_type, task_id, token)| {
            let chunk_seq = api.parse_task_id(&task_id)?.chunk_seq;
            done.entry((task_type, chunk_seq, token))
                .or_insert_with_key(|(task_type, chunk_seq, token)| {
                    cancel_chunk(api, task_type, *chunk_seq, *token)
                })
                .clone()
        })
        .collect()
}

fn cancel_chunk(
    api: &Arc<ServerAPI>,
    task_type: &Identifier,
    chunk_seq: u64,
    lease_token: u64,
) -> Result<(), Error> {
    let cell = api
        .leased_tasks
        .cell(task_type, chunk_seq)
        .ok_or(Error::not_found("TaskNotFound"))?;
    let chunk = {
        let mut lease = lock_lease(&cell);
        if lease.gone {
            return Err(Error::not_found("TaskNotFound"));
        }
        if lease.token != lease_token {
            return Err(Error::invalid_argument("LeaseTokenMismatch"));
        }
        api.leased_tasks.end(task_type, &cell, &mut lease);
        lease.chunk.clone()
    };
    // Still in the dedup set: straight from "leased" to "queued" (or, on
    // overflow, to the recovery set).
    ServerAPI::requeue(api, task_type, chunk);
    Ok(())
}
