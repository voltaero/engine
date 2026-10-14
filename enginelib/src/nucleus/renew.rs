use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};

use crate::api::lock_lease;
use crate::{Identifier, api::ServerAPI, error::Error};

/// Extends the lease on the chunk holding `task_id` by a full TTL from now and
/// returns the new expiry. Leases are per chunk, so this renews every task of
/// that lease. Only the current lease holder (matching `lease_token`) may renew.
pub fn renew(
    api: Arc<ServerAPI>,
    task_type: Identifier,
    task_id: &str,
    lease_token: u64,
) -> Result<DateTime<Utc>, Error> {
    let id = api.parse_task_id(task_id)?;
    renew_chunk(&api, &task_type, id.chunk_seq, lease_token)
}

/// Batch form of [`renew`]: one result per entry, in order. Entries of the same
/// chunk lease are renewed once and share the outcome.
pub fn renew_batch(
    api: &Arc<ServerAPI>,
    leases: impl IntoIterator<Item = (Identifier, String, u64)>,
) -> Vec<Result<DateTime<Utc>, Error>> {
    let mut done: HashMap<(Identifier, u64, u64), Result<DateTime<Utc>, Error>> = HashMap::new();
    leases
        .into_iter()
        .map(|(task_type, task_id, token)| {
            let chunk_seq = api.parse_task_id(&task_id)?.chunk_seq;
            done.entry((task_type, chunk_seq, token))
                .or_insert_with_key(|(task_type, chunk_seq, token)| {
                    renew_chunk(api, task_type, *chunk_seq, *token)
                })
                .clone()
        })
        .collect()
}

fn renew_chunk(
    api: &ServerAPI,
    task_type: &Identifier,
    chunk_seq: u64,
    lease_token: u64,
) -> Result<DateTime<Utc>, Error> {
    if api.leased_tasks.leases(task_type).is_none() {
        return Err(Error::not_found("TaskTypeNotFound"));
    }
    let cell = api
        .leased_tasks
        .cell(task_type, chunk_seq)
        .ok_or(Error::not_found("TaskNotFound"))?;
    let mut lease = lock_lease(&cell);
    if lease.gone {
        return Err(Error::not_found("TaskNotFound"));
    }
    if lease.token != lease_token {
        return Err(Error::invalid_argument("LeaseTokenMismatch"));
    }

    let now = Utc::now();
    lease.given_at = now;
    lease.expires_at = api.lease_expiry(now);
    Ok(lease.expires_at)
}
