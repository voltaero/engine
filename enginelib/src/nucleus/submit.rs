use std::sync::Arc;

use crate::{Identifier, api::ServerAPI, task::Task};
// t:namespace:task_name:<id> -> Serialized Task Record
// f:namespace:task_name:<id> -> Finished Task Record
pub fn submit(api: Arc<ServerAPI>, task_bytes: &[u8], task_id: Identifier) {
    // deserialize
    let task = Task::verify(task_bytes);
    // Verify Task
    // upload Task
    let mut db = api.db.put("t", value)
}
