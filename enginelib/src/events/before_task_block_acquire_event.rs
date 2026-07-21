use macros::Event;

use crate::{Identifier, api::ServerAPI};

#[derive(Clone, Debug, Event)]
#[event(namespace = "client", name = "before_task_block_acquire", cancellable)]
pub struct BeforeTaskBlockAcquireEvent {
    pub cancelled: bool,
    pub id: Identifier,
    pub task_ids: Vec<String>,
}

impl BeforeTaskBlockAcquireEvent {
    pub fn fire(api: &ServerAPI, task_ids: Vec<String>) -> Self {
        let mut event = BeforeTaskBlockAcquireEvent {
            cancelled: false,
            id: (
                "client".to_string(),
                "before_task_block_acquire".to_string(),
            ),
            task_ids,
        };
        api.event_bus.fire(&mut event);
        event
    }

    pub fn check(api: &ServerAPI, task_ids: Vec<String>) -> bool {
        Self::fire(api, task_ids).cancelled
    }
}
