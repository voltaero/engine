use macros::Event;

use crate::{Identifier, api::EngineAPI};

#[derive(Clone, Debug, Event)]
#[event(namespace = "server", name = "start")]
pub struct ServerStartEvent {
    pub cancelled: bool,
    pub id: Identifier,
}

impl ServerStartEvent {
    pub fn fire(api: &EngineAPI) {
        let mut event = ServerStartEvent {
            cancelled: false,
            id: ("server".to_string(), "start".to_string()),
        };
        api.event_bus.fire(&mut event);
    }
}
