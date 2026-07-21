use std::sync::Arc;

use rust_rocksdb::{WriteBatch, WriteOptions};

use crate::api::task_key;
use crate::{Identifier, Registry, api::ServerAPI, error::Error};
// t:namespace:task_name:<id> -> Serialized Task Record
// f:namespace:task_name:<id> -> Finished Task Record

/// Submit a single task. Thin wrapper over [`submit_batch`]; returns its id.
pub fn submit(
    api: Arc<ServerAPI>,
    task_bytes: &[u8],
    task_type: Identifier,
) -> Result<String, Error> {
    let mut ids = submit_batch(api, task_type, vec![task_bytes.to_vec()])?;
    Ok(ids
        .pop()
        .expect("submit_batch returns one id per submitted task"))
}

/// Persist a batch of task payloads of one `task_type`.
///
/// **Write-only.** Submit's whole job is to make the tasks durable — one RocksDB
/// `WriteBatch` (one WAL append). It does *not* touch the in-memory queue.
/// Moving persisted records into the lease channel is the loader's job
/// ([`ServerAPI::run_loader`]), which streams them in with backpressure. That
/// keeps the DB the single source of truth: nothing is enqueued that isn't
/// durable, nothing is lost if a producer outruns consumers, and restart
/// recovery is the same load path — never a stranded task.
pub fn submit_batch(
    api: Arc<ServerAPI>,
    task_type: Identifier,
    payloads: Vec<Vec<u8>>,
) -> Result<Vec<String>, Error> {
    let task = api
        .task_registry
        .get(&task_type)
        .ok_or(Error::not_found("TaskTypeNotFound"))?;

    // Verify every payload before persisting anything — a batch is all-or-nothing.
    for bytes in &payloads {
        if !task.verify(bytes) {
            return Err(Error::invalid_argument("Failed to verify task bytes"));
        }
    }

    let ids: Vec<String> = payloads
        .iter()
        .map(|_| druid::Druid::default().to_hex())
        .collect();
    let mut batch = WriteBatch::default();
    for (id, bytes) in ids.iter().zip(&payloads) {
        batch.put(task_key(&task_type, id), bytes);
    }
    let mut wopts = WriteOptions::default();
    // Do not acknowledge submissions until the WAL is durable on stable storage.
    wopts.set_sync(true);
    api.db
        .write_opt(&batch, &wopts)
        .map_err(|err| Error::io_error(format!("Failed to persist task batch: {err}")))?;

    Ok(ids)
}
