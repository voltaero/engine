use macros::Event;

use crate::{Identifier, api::ServerAPI};

#[derive(Clone, Debug, Event)]
#[event(namespace = "server", name = "task_published")]
pub struct ServerTaskPublishedEvent {
    pub cancelled: bool,
    pub id: Identifier,
    pub uid: String,
    pub task_id: String,
    pub instance_id: String,
}

impl ServerTaskPublishedEvent {
    pub fn fire(api: &ServerAPI, uid: String, task_id: String, instance_id: String) {
        let mut event = ServerTaskPublishedEvent {
            cancelled: false,
            id: ("server".to_string(), "task_published".to_string()),
            uid,
            task_id,
            instance_id,
        };
        api.event_bus.fire(&mut event);
    }
}
