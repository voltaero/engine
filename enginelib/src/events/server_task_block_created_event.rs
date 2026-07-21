use std::sync::{Arc, RwLock};

use macros::Event;

use crate::{Identifier, api::ServerAPI};

#[derive(Clone, Debug, Event)]
#[event(namespace = "server", name = "task_block_created")]
pub struct ServerTaskBlockCreatedEvent {
    pub cancelled: bool,
    pub id: Identifier,
    pub task_id: String,
    pub instance_ids: Vec<String>,
    pub payloads: Vec<Arc<RwLock<Vec<u8>>>>,
}

impl ServerTaskBlockCreatedEvent {
    pub fn fire(
        api: &ServerAPI,
        task_id: String,
        instance_ids: Vec<String>,
        payloads: Vec<Arc<RwLock<Vec<u8>>>>,
    ) {
        let mut event = ServerTaskBlockCreatedEvent {
            cancelled: false,
            id: ("server".to_string(), "task_block_created".to_string()),
            task_id,
            instance_ids,
            payloads,
        };
        api.event_bus.fire(&mut event);
    }
}
