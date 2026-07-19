use std::sync::Arc;

use chrono::{DateTime, Utc};

use crate::{Identifier, api::ServerAPI, error::Error};

#[allow(dead_code)]
fn renew(api: Arc<ServerAPI>, task_type: Identifier, task_id: &str) -> Result<(), Error> {
    let mut leases = api
        .leased_tasks
        .tasks
        .get_mut(&task_type)
        .ok_or(Error::new("TaskTypeNotFound".into()))?;

    let lease = leases
        .iter_mut()
        .find(|lease| lease.stored_task.task_id == task_id)
        .ok_or(Error::new("TaskNotFound".into()))?;

    lease.given_at = Utc::now();

    Ok(())
}
