use std::sync::Arc;

use chrono::{DateTime, Utc};

use crate::{Identifier, api::ServerAPI};

#[allow(dead_code)]
fn renew(api: Arc<ServerAPI>, task_type: Identifier, task_id: &str) {
    api.leased_tasks
        .tasks
        .entry(task_type)
        .and_modify(|leases| {
            if let Some(lease) = leases
                .iter_mut()
                .find(|lease| lease.stored_task.task_id == task_id)
            {
                lease.given_at = Utc::now();
            }
        });
}
