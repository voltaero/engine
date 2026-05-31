use macros::Event;

use crate::{Identifier, api::ServerAPI};

#[derive(Clone, Debug, Event)]
#[event(namespace = "server", name = "task_block_published")]
pub struct ServerTaskBlockPublishedEvent {
    pub cancelled: bool,
    pub id: Identifier,
    pub uid: String,
    pub task_id: String,
    pub instance_ids: Vec<String>,
}

impl ServerTaskBlockPublishedEvent {
    pub fn fire(api: &ServerAPI, uid: String, task_id: String, instance_ids: Vec<String>) {
        let mut event = ServerTaskBlockPublishedEvent {
            cancelled: false,
            id: ("server".to_string(), "task_block_published".to_string()),
            uid,
            task_id,
            instance_ids,
        };
        api.event_bus.fire(&mut event);
    }
}
