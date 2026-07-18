use std::sync::{Arc, RwLock};

use crate::{Identifier, api::ServerAPI};
use macros::{Event, event_handler};

#[derive(Clone, Debug, Event)]
#[event(namespace = "core", name = "auth_event", cancellable)]
pub struct AuthEvent {
    pub cancelled: bool,
    pub id: Identifier,
    pub uid: String,
    pub challenge: String,
    pub db: Arc<rust_rocksdb::DB>,
    pub output: Arc<RwLock<bool>>,
}
impl AuthEvent {
    pub fn fire(
        api: &ServerAPI,
        uid: String,
        challenge: String,
        db: rust_rocksdb::DB,
        output: Arc<RwLock<bool>>,
    ) {
        api.event_bus.fire(&mut AuthEvent {
            cancelled: false,
            id: ("core".to_string(), "auth_event".to_string()),
            uid,
            challenge,
            db: Arc::new(db),
            output,
        });
    }

    pub fn check(api: &ServerAPI, uid: String, challenge: String, db: rust_rocksdb::DB) -> bool {
        let output = Arc::new(RwLock::new(false));
        Self::fire(api, uid, challenge, db, output.clone());
        *output.read().unwrap()
    }
}

#[event_handler(namespace = "core", name = "auth_event")]
fn auth_handler(event: &mut AuthEvent) {
    *event.output.write().unwrap() = true;
}
