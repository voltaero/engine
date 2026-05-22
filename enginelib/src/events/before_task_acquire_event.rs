use macros::Event;

use crate::{Identifier, api::ServerAPI};

#[derive(Clone, Debug, Event)]
#[event(namespace = "client", name = "before_task_acquire", cancellable)]
pub struct BeforeTaskAcquireEvent {
    pub cancelled: bool,
    pub id: Identifier,
    pub task_id: String,
}

impl BeforeTaskAcquireEvent {
    pub fn fire(api: &ServerAPI, task_id: String) -> Self {
        let mut event = BeforeTaskAcquireEvent {
            cancelled: false,
            id: ("client".to_string(), "before_task_acquire".to_string()),
            task_id,
        };
        api.event_bus.fire(&mut event);
        event
    }

    pub fn check(api: &ServerAPI, task_id: String) -> bool {
        Self::fire(api, task_id).cancelled
    }
}
