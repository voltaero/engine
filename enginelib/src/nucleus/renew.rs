use std::sync::Arc;

use chrono::Utc;

use crate::{Identifier, api::ServerAPI, error::Error};

pub fn renew(api: Arc<ServerAPI>, task_type: Identifier, task_id: &str) -> Result<(), Error> {
    let mut leases = api
        .leased_tasks
        .tasks
        .get_mut(&task_type)
        .ok_or(Error::not_found("TaskTypeNotFound"))?;

    let lease = leases
        .iter_mut()
        .find(|lease| lease.stored_task.task_id == task_id)
        .ok_or(Error::not_found("TaskNotFound"))?;

    lease.given_at = Utc::now();

    Ok(())
}
