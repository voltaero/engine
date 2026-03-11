use std::sync::{Arc, RwLock};

use macros::{Event, event_handler};
use sled::Db;

use crate::{Identifier, api::EngineAPI};

#[derive(Clone, Debug, Event)]
#[event(namespace = "core", name = "admin_auth_event", cancellable)]
pub struct AdminAuthEvent {
    pub cancelled: bool,
    pub id: Identifier,
    pub payload: String,
    pub target: Identifier,
    pub db: Db,
    pub output: Arc<RwLock<bool>>,
}
// Event trait auto-implemented by derive macro

impl AdminAuthEvent {
    pub fn fire(
        api: &mut EngineAPI,
        payload: String,
        target: Identifier,
        db: Db,
        output: Arc<RwLock<bool>>,
    ) {
        api.event_bus.fire(&mut AdminAuthEvent {
            cancelled: false,
            id: ("core".to_string(), "admin_auth_event".to_string()),
            payload,
            target,
            db,
            output,
        });
    }

    pub fn check(api: &mut EngineAPI, payload: String, target: Identifier, db: Db) -> bool {
        let output = Arc::new(RwLock::new(false));
        Self::fire(api, payload, target, db, output.clone());
        *output.read().unwrap()
    }
}

#[event_handler(
    namespace = "core",
    name = "admin_auth_event",
    ctx = api.cfg.config_toml.cgrpc_token.clone()
)]
fn admin_auth_handler(event: &mut AdminAuthEvent, token: &Option<String>) {
    match token.as_deref() {
        None => *event.output.write().unwrap() = true,
        Some(token) if token == event.payload.as_str() => *event.output.write().unwrap() = true,
        Some(_) => {}
    }
}
