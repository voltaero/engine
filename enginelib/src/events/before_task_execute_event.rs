use std::sync::{Arc, RwLock};

use macros::Event;

use crate::{Identifier, api::EngineAPI};

#[derive(Clone, Debug, Event)]
#[event(namespace = "client", name = "before_task_execute", cancellable)]
pub struct BeforeTaskExecuteEvent {
    pub cancelled: bool,
    pub id: Identifier,
    pub task_id: String,
    pub instance_id: String,
    pub payload: Arc<RwLock<Vec<u8>>>,
}

impl BeforeTaskExecuteEvent {
    pub fn fire(
        api: &EngineAPI,
        task_id: String,
        instance_id: String,
        payload: Arc<RwLock<Vec<u8>>>,
    ) -> Self {
        let mut event = BeforeTaskExecuteEvent {
            cancelled: false,
            id: ("client".to_string(), "before_task_execute".to_string()),
            task_id,
            instance_id,
            payload,
        };
        api.event_bus.fire(&mut event);
        event
    }

    pub fn check(
        api: &EngineAPI,
        task_id: String,
        instance_id: String,
        payload: Arc<RwLock<Vec<u8>>>,
    ) -> bool {
        Self::fire(api, task_id, instance_id, payload).cancelled
    }
}
