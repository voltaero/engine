use std::sync::{Arc, RwLock};

use macros::Event;

use crate::{Identifier, api::EngineAPI};

#[derive(Clone, Debug, Event)]
#[event(namespace = "server", name = "task_created")]
pub struct ServerTaskCreatedEvent {
    pub cancelled: bool,
    pub id: Identifier,
    pub task_id: String,
    pub instance_id: String,
    pub payload: Arc<RwLock<Vec<u8>>>,
}

impl ServerTaskCreatedEvent {
    pub fn fire(
        api: &EngineAPI,
        task_id: String,
        instance_id: String,
        payload: Arc<RwLock<Vec<u8>>>,
    ) {
        let mut event = ServerTaskCreatedEvent {
            cancelled: false,
            id: ("server".to_string(), "task_created".to_string()),
            task_id,
            instance_id,
            payload,
        };
        api.event_bus.fire(&mut event);
    }
}
