use std::sync::Arc;

use crate::{Identifier, Registry, api::ServerAPI, error::Error};
// t:namespace:task_name:<id> -> Serialized Task Record
// f:namespace:task_name:<id> -> Finished Task Record
#[allow(dead_code)]
pub fn submit(api: Arc<ServerAPI>, task_bytes: &[u8], task_type: Identifier) -> Result<(), Error> {
    let task = api
        .task_registry
        .get(&task_type)
        .ok_or(Error::new("Not found".into()))?;
    if task.verify(task_bytes) {
        let res = api.db.put(
            format!(
                "t:{}:{}:{}",
                task_type.0,
                task_type.1,
                druid::Druid::default().to_hex()
            ),
            task_bytes,
        );
        if res.is_err() {
            return Err(Error::new(res.err().unwrap().to_string()));
        }
    }
    Ok(())
}
