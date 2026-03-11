use std::sync::{Arc, RwLock};

use crate::{Identifier, api::EngineAPI};
use macros::{Event, event_handler};
use sled::Db;

#[derive(Clone, Debug, Event)]
#[event(namespace = "core", name = "auth_event", cancellable)]
pub struct AuthEvent {
    pub cancelled: bool,
    pub id: Identifier,
    pub uid: String,
    pub challenge: String,
    pub db: Db,
    pub output: Arc<RwLock<bool>>,
}
impl AuthEvent {
    pub fn fire(
        api: &mut EngineAPI,
        uid: String,
        challenge: String,
        db: Db,
        output: Arc<RwLock<bool>>,
    ) {
        api.event_bus.fire(&mut AuthEvent {
            cancelled: false,
            id: ("core".to_string(), "auth_event".to_string()),
            uid,
            challenge,
            db,
            output,
        });
    }

    pub fn check(api: &mut EngineAPI, uid: String, challenge: String, db: Db) -> bool {
        let output = Arc::new(RwLock::new(false));
        Self::fire(api, uid, challenge, db, output.clone());
        *output.read().unwrap()
    }
}

#[event_handler(namespace = "core", name = "auth_event")]
fn auth_handler(event: &mut AuthEvent) {
    *event.output.write().unwrap() = true;
}
