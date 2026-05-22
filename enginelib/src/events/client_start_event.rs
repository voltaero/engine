use macros::Event;

use crate::{Identifier, api::ServerAPI};

#[derive(Clone, Debug, Event)]
#[event(namespace = "client", name = "start")]
pub struct ClientStartEvent {
    pub cancelled: bool,
    pub id: Identifier,
}

impl ClientStartEvent {
    pub fn fire(api: &ServerAPI) {
        let mut event = ClientStartEvent {
            cancelled: false,
            id: ("client".to_string(), "start".to_string()),
        };
        api.event_bus.fire(&mut event);
    }
}
