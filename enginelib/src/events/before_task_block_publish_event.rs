use std::sync::{Arc, RwLock};

use macros::Event;

use crate::{Identifier, api::ServerAPI};

#[derive(Clone, Debug, Event)]
#[event(namespace = "client", name = "before_task_block_publish", cancellable)]
pub struct BeforeTaskBlockPublishEvent {
    pub cancelled: bool,
    pub id: Identifier,
    pub task_id: String,
    pub instance_ids: Vec<String>,
    pub payloads: Vec<Arc<RwLock<Vec<u8>>>>,
}

impl BeforeTaskBlockPublishEvent {
    pub fn fire(
        api: &ServerAPI,
        task_id: String,
        instance_ids: Vec<String>,
        payloads: Vec<Arc<RwLock<Vec<u8>>>>,
    ) -> Self {
        let mut event = BeforeTaskBlockPublishEvent {
            cancelled: false,
            id: (
                "client".to_string(),
                "before_task_block_publish".to_string(),
            ),
            task_id,
            instance_ids,
            payloads,
        };
        api.event_bus.fire(&mut event);
        event
    }

    pub fn check(
        api: &ServerAPI,
        task_id: String,
        instance_ids: Vec<String>,
        payloads: Vec<Arc<RwLock<Vec<u8>>>>,
    ) -> bool {
        Self::fire(api, task_id, instance_ids, payloads).cancelled
    }
}
