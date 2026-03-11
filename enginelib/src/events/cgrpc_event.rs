use std::sync::{Arc, RwLock};

use macros::Event;

use crate::{Identifier, api::EngineAPI};

#[derive(Clone, Debug, Event)]
#[event(namespace = "core", name = "cgrpc_event", cancellable)]
pub struct CgrpcEvent {
    pub cancelled: bool,
    pub id: Identifier,
    pub handler_id: Identifier,
    pub payload: Vec<u8>,
    pub output: Arc<RwLock<Vec<u8>>>,
}

impl CgrpcEvent {
    pub fn fire(
        api: &mut EngineAPI,
        handler_id: Identifier,
        payload: Vec<u8>,
        output: Arc<RwLock<Vec<u8>>>,
    ) {
        api.event_bus.fire(&mut CgrpcEvent {
            cancelled: false,
            id: ("core".to_string(), "cgrpc_event".to_string()),
            handler_id,
            payload,
            output,
        });
    }
}
