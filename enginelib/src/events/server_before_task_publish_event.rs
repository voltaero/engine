use std::sync::{Arc, RwLock};

use macros::Event;

use crate::{Identifier, api::EngineAPI};

#[derive(Clone, Debug, Event)]
#[event(namespace = "server", name = "before_task_publish", cancellable)]
pub struct ServerBeforeTaskPublishEvent {
    pub cancelled: bool,
    pub id: Identifier,
    pub uid: String,
    pub task_id: String,
    pub instance_id: String,
    pub payload: Arc<RwLock<Vec<u8>>>,
}

impl ServerBeforeTaskPublishEvent {
    pub fn fire(
        api: &EngineAPI,
        uid: String,
        task_id: String,
        instance_id: String,
        payload: Arc<RwLock<Vec<u8>>>,
    ) -> Self {
        let mut event = ServerBeforeTaskPublishEvent {
            cancelled: false,
            id: ("server".to_string(), "before_task_publish".to_string()),
            uid,
            task_id,
            instance_id,
            payload,
        };
        api.event_bus.fire(&mut event);
        event
    }

    pub fn check(
        api: &EngineAPI,
        uid: String,
        task_id: String,
        instance_id: String,
        payload: Arc<RwLock<Vec<u8>>>,
    ) -> bool {
        Self::fire(api, uid, task_id, instance_id, payload).cancelled
    }
}
