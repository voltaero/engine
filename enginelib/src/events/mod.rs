pub mod admin_auth_event;
pub mod auth_event;
pub mod cgrpc_event;
pub mod start_event;
use std::sync::{Arc, RwLock};

use sled::Db;

use crate::{Identifier, api::EngineAPI};

pub fn ID(namespace: &str, id: &str) -> Identifier {
    (namespace.to_string(), id.to_string())
}

pub struct Events;

#[allow(non_snake_case)]
impl Events {
    pub fn init_auth(_api: &mut EngineAPI) {}

    pub fn CheckAuth(api: &mut EngineAPI, uid: String, challenge: String, db: Db) -> bool {
        auth_event::AuthEvent::check(api, uid, challenge, db)
    }

    pub fn CheckAdminAuth(api: &mut EngineAPI, payload: String, target: Identifier, db: Db) -> bool {
        admin_auth_event::AdminAuthEvent::check(api, payload, target, db)
    }

    pub fn CgrpcEvent(
        api: &mut EngineAPI,
        handler_id: Identifier,
        payload: Vec<u8>,
        output: Arc<RwLock<Vec<u8>>>,
    ) {
        cgrpc_event::CgrpcEvent::fire(api, handler_id, payload, output)
    }

    pub fn StartEvent(api: &mut EngineAPI) {
        start_event::StartEvent::fire(api)
    }
}
