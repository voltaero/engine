use std::sync::Arc;

use chrono::{DateTime, Utc};

use crate::{Identifier, api::ServerAPI, error::Error};

/// Extends the lease on `task_id` by a full TTL from now and returns the new
/// expiry. Only the current lease holder (matching `lease_token`) may renew.
pub fn renew(
    api: Arc<ServerAPI>,
    task_type: Identifier,
    task_id: &str,
    lease_token: u64,
) -> Result<DateTime<Utc>, Error> {
    let leases = api
        .leased_tasks
        .leases(&task_type)
        .ok_or(Error::not_found("TaskTypeNotFound"))?;

    let mut lease = leases
        .get_mut(task_id)
        .ok_or(Error::not_found("TaskNotFound"))?;
    if lease.token != lease_token {
        return Err(Error::invalid_argument("LeaseTokenMismatch"));
    }

    let now = Utc::now();
    lease.given_at = now;
    lease.expires_at = api.lease_expiry(now);

    Ok(lease.expires_at)
}
