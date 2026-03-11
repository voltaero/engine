use std::sync::Arc;

use macros::Event;

use crate::{Identifier, api::EngineAPI, plugin::LibraryMetadata};

#[derive(Clone, Debug, Event)]
#[event(namespace = "core", name = "start_event")]
pub struct StartEvent {
    pub cancelled: bool,
    pub id: Identifier,
    pub modules: Vec<Arc<LibraryMetadata>>,
}

impl StartEvent {
    pub fn fire(api: &mut EngineAPI) {
        let modules = api
            .lib_manager
            .libraries
            .values()
            .map(|lib| lib.metadata.clone())
            .collect();

        api.event_bus.fire(&mut StartEvent {
            cancelled: false,
            id: ("core".to_string(), "start_event".to_string()),
            modules,
        });
    }
}
