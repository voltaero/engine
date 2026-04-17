use std::sync::{Arc, RwLock};

use macros::Event;

use crate::{Identifier, api::EngineAPI};

#[derive(Clone, Debug, Event)]
#[event(namespace = "server", name = "before_task_create", cancellable)]
pub struct ServerBeforeTaskCreateEvent {
    pub cancelled: bool,
    pub id: Identifier,
    pub task_id: String,
    pub payload: Arc<RwLock<Vec<u8>>>,
}

impl ServerBeforeTaskCreateEvent {
    pub fn fire(api: &EngineAPI, task_id: String, payload: Arc<RwLock<Vec<u8>>>) -> Self {
        let mut event = ServerBeforeTaskCreateEvent {
            cancelled: false,
            id: ("server".to_string(), "before_task_create".to_string()),
            task_id,
            payload,
        };
        api.event_bus.fire(&mut event);
        event
    }

    pub fn check(api: &EngineAPI, task_id: String, payload: Arc<RwLock<Vec<u8>>>) -> bool {
        Self::fire(api, task_id, payload).cancelled
    }
}
