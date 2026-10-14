use std::sync::{Arc, RwLock};

use macros::{Event, event_handler};
use rust_rocksdb::DB;
use subtle::ConstantTimeEq;

use crate::{Identifier, api::ServerAPI};

#[derive(Clone, Debug, Event)]
#[event(namespace = "core", name = "admin_auth_event", cancellable)]
pub struct AdminAuthEvent {
    pub cancelled: bool,
    pub id: Identifier,
    pub payload: String,
    pub target: Identifier,
    pub db: Arc<DB>,
    pub output: Arc<RwLock<bool>>,
}
// Event trait auto-implemented by derive macro

impl AdminAuthEvent {
    pub fn fire(
        api: &ServerAPI,
        payload: String,
        target: Identifier,
        db: Arc<DB>,
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

    pub fn check(api: &ServerAPI, payload: String, target: Identifier, db: Arc<DB>) -> bool {
        let output = Arc::new(RwLock::new(false));
        Self::fire(api, payload, target, db, output.clone());
        *output.read().unwrap()
    }
}

#[event_handler(
    namespace = "core",
    name = "admin_auth_event",
    ctx = api.cfg.config_toml.auth_token.clone()
)]
fn admin_auth_handler(event: &mut AdminAuthEvent, token: &Option<String>) {
    match token.as_deref() {
        None => *event.output.write().unwrap() = true,
        Some(token) if constant_time_eq(token.as_bytes(), event.payload.as_bytes()) => {
            *event.output.write().unwrap() = true
        }
        Some(_) => {}
    }
}

/// Compares two byte strings in time independent of where they first differ, so
/// the admin token can't be recovered byte-by-byte from response timing. Only the
/// length is allowed to leak.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.ct_eq(b).into()
}

#[cfg(test)]
mod tests {
    use super::constant_time_eq;

    #[test]
    fn constant_time_eq_matches_only_identical_bytes() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(constant_time_eq(b"", b""));
        assert!(!constant_time_eq(b"secret", b"secreT"));
        assert!(!constant_time_eq(b"secret", b"secret!"));
        assert!(!constant_time_eq(b"", b"x"));
    }
}
