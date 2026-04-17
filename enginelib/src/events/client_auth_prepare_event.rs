use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use macros::Event;

use crate::{Identifier, api::EngineAPI};

#[derive(Clone, Debug, Event)]
#[event(namespace = "client", name = "auth_prepare")]
pub struct ClientAuthPrepareEvent {
    pub cancelled: bool,
    pub id: Identifier,
    pub headers: Arc<RwLock<HashMap<String, String>>>,
}

impl ClientAuthPrepareEvent {
    pub fn fire(api: &EngineAPI, headers: Arc<RwLock<HashMap<String, String>>>) {
        let mut event = ClientAuthPrepareEvent {
            cancelled: false,
            id: ("client".to_string(), "auth_prepare".to_string()),
            headers,
        };
        api.event_bus.fire(&mut event);
    }
}
