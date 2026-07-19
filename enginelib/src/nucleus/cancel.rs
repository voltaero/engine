use std::sync::Arc;

use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use crate::{Identifier, api::ServerAPI, error::Error};

pub fn cancel(api: Arc<ServerAPI>, task_type: Identifier, task_id: String) -> Result<(), Error> {
    api.leased_tasks.tasks.entry(task_type).and_modify(|f| {
        f.par_iter()
            .filter(|f| f.stored_task.task_id != task_id)
            .collect::<Vec<_>>();
    });
    Ok(())
}
