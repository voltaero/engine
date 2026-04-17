pub mod admin_auth_event;
pub mod auth_event;
pub mod before_task_acquire_event;
pub mod before_task_execute_event;
pub mod before_task_publish_event;
pub mod cgrpc_event;
pub mod client_auth_prepare_event;
pub mod client_start_event;
pub mod server_before_task_acquire_event;
pub mod server_before_task_create_event;
pub mod server_before_task_publish_event;
pub mod server_start_event;
pub mod server_task_acquired_event;
pub mod server_task_created_event;
pub mod server_task_published_event;
pub mod start_event;
pub mod task_acquired_event;
use std::collections::HashMap;
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

    pub fn ClientStart(api: &EngineAPI) {
        client_start_event::ClientStartEvent::fire(api)
    }

    pub fn ClientAuthPrepare(api: &EngineAPI, headers: Arc<RwLock<HashMap<String, String>>>) {
        client_auth_prepare_event::ClientAuthPrepareEvent::fire(api, headers)
    }

    pub fn BeforeTaskAcquire(api: &EngineAPI, task_id: String) -> bool {
        before_task_acquire_event::BeforeTaskAcquireEvent::check(api, task_id)
    }

    pub fn TaskAcquired(
        api: &EngineAPI,
        task_id: String,
        instance_id: String,
        payload: Arc<RwLock<Vec<u8>>>,
    ) {
        task_acquired_event::TaskAcquiredEvent::fire(api, task_id, instance_id, payload)
    }

    pub fn BeforeTaskExecute(
        api: &EngineAPI,
        task_id: String,
        instance_id: String,
        payload: Arc<RwLock<Vec<u8>>>,
    ) -> bool {
        before_task_execute_event::BeforeTaskExecuteEvent::check(api, task_id, instance_id, payload)
    }

    pub fn BeforeTaskPublish(
        api: &EngineAPI,
        task_id: String,
        instance_id: String,
        payload: Arc<RwLock<Vec<u8>>>,
    ) -> bool {
        before_task_publish_event::BeforeTaskPublishEvent::check(api, task_id, instance_id, payload)
    }

    pub fn ServerStart(api: &EngineAPI) {
        server_start_event::ServerStartEvent::fire(api)
    }

    pub fn ServerBeforeTaskCreate(
        api: &EngineAPI,
        task_id: String,
        payload: Arc<RwLock<Vec<u8>>>,
    ) -> bool {
        server_before_task_create_event::ServerBeforeTaskCreateEvent::check(api, task_id, payload)
    }

    pub fn ServerTaskCreated(
        api: &EngineAPI,
        task_id: String,
        instance_id: String,
        payload: Arc<RwLock<Vec<u8>>>,
    ) {
        server_task_created_event::ServerTaskCreatedEvent::fire(api, task_id, instance_id, payload)
    }

    pub fn ServerBeforeTaskAcquire(api: &EngineAPI, uid: String, task_id: String) -> bool {
        server_before_task_acquire_event::ServerBeforeTaskAcquireEvent::check(api, uid, task_id)
    }

    pub fn ServerTaskAcquired(
        api: &EngineAPI,
        uid: String,
        task_id: String,
        instance_id: String,
    ) {
        server_task_acquired_event::ServerTaskAcquiredEvent::fire(api, uid, task_id, instance_id)
    }

    pub fn ServerBeforeTaskPublish(
        api: &EngineAPI,
        uid: String,
        task_id: String,
        instance_id: String,
        payload: Arc<RwLock<Vec<u8>>>,
    ) -> bool {
        server_before_task_publish_event::ServerBeforeTaskPublishEvent::check(
            api,
            uid,
            task_id,
            instance_id,
            payload,
        )
    }

    pub fn ServerTaskPublished(api: &EngineAPI, uid: String, task_id: String, instance_id: String) {
        server_task_published_event::ServerTaskPublishedEvent::fire(api, uid, task_id, instance_id)
    }
}
