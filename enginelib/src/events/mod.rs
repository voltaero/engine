use auth_event::AuthEvent;

use crate::api::{self, EngineAPI};
use crate::{Identifier, RegisterAdminAuthEventHandler, RegisterAuthEventHandler};
use std::sync::{Arc, Mutex, RwLock};
pub mod admin_auth_event;
pub mod auth_event;
pub mod cgrpc_event;
pub mod start_event;
pub fn ID(namespace: &str, id: &str) -> Identifier {
    (namespace.to_string(), id.to_string())
}
