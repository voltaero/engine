use std::sync::Arc;

use rust_rocksdb::WriteBatch;

use crate::api::task_key;
use crate::{Identifier, Registry, api::ServerAPI, error::Error};
// t:namespace:task_name:<id> -> Serialized Task Record
// f:namespace:task_name:<id> -> Finished Task Record
pub fn submit(
    api: Arc<ServerAPI>,
    task_bytes: &[u8],
    task_type: Identifier,
) -> Result<String, Error> {
    let task = api
        .task_registry
        .get(&task_type)
        .ok_or(Error::not_found("TaskTypeNotFound"))?;
    if !task.verify(task_bytes) {
        return Err(Error::invalid_argument("Failed to verify task bytes"));
    }
    let task_id = druid::Druid::default().to_hex();
    api.db
        .put(task_key(&task_type, &task_id), task_bytes)
        .map_err(|err| Error::io_error(err.to_string()))?;

    // Wake the refill path: a new record may be loadable (and, since druid keys
    // aren't strictly ordered, possibly behind the cursor). Only kick a refill
    // when the queue is running low; otherwise lease() will as it drains.
    api.load_state.get(&task_type).note_submitted();
    let low = api
        .task_queue
        .tasks
        .get(&task_type)
        .is_some_and(|queue| ServerAPI::below_watermark(queue.0.len(), queue.0.capacity()));
    if low {
        ServerAPI::request_refill(&api, &task_type);
    }
    Ok(task_id)
}

/// Batch form of [`submit`]: verifies every payload, then persists all valid ones
/// in a single `WriteBatch`. Returns one result per payload, in order; a payload
/// that fails verification gets an error and is skipped, the rest still land. If
/// the batch write itself fails, every verified payload gets that error.
pub fn submit_batch(
    api: Arc<ServerAPI>,
    payloads: &[Vec<u8>],
    task_type: Identifier,
) -> Result<Vec<Result<String, Error>>, Error> {
    let task = api
        .task_registry
        .get(&task_type)
        .ok_or(Error::not_found("TaskTypeNotFound"))?;

    let mut batch = WriteBatch::default();
    let mut results: Vec<Result<String, Error>> = Vec::with_capacity(payloads.len());
    for payload in payloads {
        if !task.verify(payload) {
            results.push(Err(Error::invalid_argument("Failed to verify task bytes")));
            continue;
        }
        let task_id = druid::Druid::default().to_hex();
        batch.put(task_key(&task_type, &task_id), payload);
        results.push(Ok(task_id));
    }
    if batch.is_empty() {
        return Ok(results);
    }
    if let Err(err) = api.db.write(&batch) {
        let err = Error::io_error(err.to_string());
        for result in results.iter_mut().filter(|result| result.is_ok()) {
            *result = Err(err.clone());
        }
        return Ok(results);
    }

    // Same refill wake-up as submit().
    api.load_state.get(&task_type).note_submitted();
    let low = api
        .task_queue
        .tasks
        .get(&task_type)
        .is_some_and(|queue| ServerAPI::below_watermark(queue.0.len(), queue.0.capacity()));
    if low {
        ServerAPI::request_refill(&api, &task_type);
    }
    Ok(results)
}
