use std::sync::Arc;

use crate::{Identifier, api::ServerAPI, error::Error};

#[allow(dead_code)]
async fn lease(api: Arc<ServerAPI>, task_type: Identifier) -> Result<(), Error> {
    let task = api
        .task_queue
        .tasks
        .get(&task_type)
        .ok_or(Error::new("TaskTypeNotFound".into()))?
        .1
        .recv()
        .await;
    Ok(())
}
