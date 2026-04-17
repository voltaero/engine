use std::sync::{Arc, RwLock};

use macros::Event;

use crate::{Identifier, api::EngineAPI};

#[derive(Clone, Debug, Event)]
#[event(namespace = "client", name = "task_acquired")]
pub struct TaskAcquiredEvent {
    pub cancelled: bool,
    pub id: Identifier,
    pub task_id: String,
    pub instance_id: String,
    pub payload: Arc<RwLock<Vec<u8>>>,
}

impl TaskAcquiredEvent {
    pub fn fire(
        api: &EngineAPI,
        task_id: String,
        instance_id: String,
        payload: Arc<RwLock<Vec<u8>>>,
    ) {
        let mut event = TaskAcquiredEvent {
            cancelled: false,
            id: ("client".to_string(), "task_acquired".to_string()),
            task_id,
            instance_id,
            payload,
        };
        api.event_bus.fire(&mut event);
    }
}
