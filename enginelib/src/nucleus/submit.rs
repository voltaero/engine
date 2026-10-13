use std::sync::Arc;

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
