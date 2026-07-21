use macros::Event;

use crate::{Identifier, api::ServerAPI};

#[derive(Clone, Debug, Event)]
#[event(namespace = "server", name = "task_block_acquired")]
pub struct ServerTaskBlockAcquiredEvent {
    pub cancelled: bool,
    pub id: Identifier,
    pub uid: String,
    pub task_id: String,
    pub instance_ids: Vec<String>,
}

impl ServerTaskBlockAcquiredEvent {
    pub fn fire(api: &ServerAPI, uid: String, task_id: String, instance_ids: Vec<String>) {
        let mut event = ServerTaskBlockAcquiredEvent {
            cancelled: false,
            id: ("server".to_string(), "task_block_acquired".to_string()),
            uid,
            task_id,
            instance_ids,
        };
        api.event_bus.fire(&mut event);
    }
}
