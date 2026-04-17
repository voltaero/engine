use macros::Event;

use crate::{Identifier, api::EngineAPI};

#[derive(Clone, Debug, Event)]
#[event(namespace = "server", name = "before_task_acquire", cancellable)]
pub struct ServerBeforeTaskAcquireEvent {
    pub cancelled: bool,
    pub id: Identifier,
    pub uid: String,
    pub task_id: String,
}

impl ServerBeforeTaskAcquireEvent {
    pub fn fire(api: &EngineAPI, uid: String, task_id: String) -> Self {
        let mut event = ServerBeforeTaskAcquireEvent {
            cancelled: false,
            id: ("server".to_string(), "before_task_acquire".to_string()),
            uid,
            task_id,
        };
        api.event_bus.fire(&mut event);
        event
    }

    pub fn check(api: &EngineAPI, uid: String, task_id: String) -> bool {
        Self::fire(api, uid, task_id).cancelled
    }
}
