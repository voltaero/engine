use macros::Event;

use crate::{Identifier, api::EngineAPI};

#[derive(Clone, Debug, Event)]
#[event(namespace = "server", name = "task_acquired")]
pub struct ServerTaskAcquiredEvent {
    pub cancelled: bool,
    pub id: Identifier,
    pub uid: String,
    pub task_id: String,
    pub instance_id: String,
}

impl ServerTaskAcquiredEvent {
    pub fn fire(api: &EngineAPI, uid: String, task_id: String, instance_id: String) {
        let mut event = ServerTaskAcquiredEvent {
            cancelled: false,
            id: ("server".to_string(), "task_acquired".to_string()),
            uid,
            task_id,
            instance_id,
        };
        api.event_bus.fire(&mut event);
    }
}
