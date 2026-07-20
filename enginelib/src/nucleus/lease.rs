use std::sync::Arc;

use chrono::Utc;

use crate::api::{LeasedTask, StoredTask};
use crate::{Identifier, api::ServerAPI, error::Error};

pub async fn lease(
    api: Arc<ServerAPI>,
    task_type: Identifier,
    user_id: String,
) -> Result<Arc<StoredTask>, Error> {
    // Clone the receiver out of the map guard before awaiting; recv() on an
    // empty queue would otherwise hold the shard lock indefinitely.
    let receiver = api
        .task_queue
        .tasks
        .get(&task_type)
        .ok_or(Error::not_found("TaskTypeNotFound"))?
        .1
        .clone();

    let task = receiver
        .recv()
        .await
        .map_err(|err| Error::new(format!("Task queue closed: {err}")))?;

    let stored_task = Arc::new(task);
    api.leased_tasks
        .tasks
        .entry(task_type)
        .or_default()
        .push(LeasedTask {
            stored_task: stored_task.clone(),
            user_id,
            given_at: Utc::now(),
        });

    Ok(stored_task)
}
