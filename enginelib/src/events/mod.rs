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
pub mod server_task_block_acquired_event;
pub mod server_task_block_created_event;
pub mod server_task_block_published_event;
pub mod start_event;
pub mod task_acquired_event;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use sled::Db;

use crate::{Identifier, api::ServerAPI};

pub fn ID(namespace: &str, id: &str) -> Identifier {
    (namespace.to_string(), id.to_string())
}

pub struct Events;

#[allow(non_snake_case)]
impl Events {
    pub fn init_auth(_api: &mut ServerAPI) {}

    pub fn CheckAuth(api: &ServerAPI, uid: String, challenge: String, db: Db) -> bool {
        auth_event::AuthEvent::check(api, uid, challenge, db)
    }

    pub fn CheckAdminAuth(
        api: &ServerAPI,
        payload: String,
        target: Identifier,
        db: Db,
    ) -> bool {
        admin_auth_event::AdminAuthEvent::check(api, payload, target, db)
    }

    pub fn CgrpcEvent(
        api: &mut ServerAPI,
        handler_id: Identifier,
        payload: Vec<u8>,
        output: Arc<RwLock<Vec<u8>>>,
    ) {
        cgrpc_event::CgrpcEvent::fire(api, handler_id, payload, output)
    }

    pub fn StartEvent(api: &mut ServerAPI) {
        start_event::StartEvent::fire(api)
    }

    pub fn ClientStart(api: &ServerAPI) {
        client_start_event::ClientStartEvent::fire(api)
    }

    pub fn ClientAuthPrepare(api: &ServerAPI, headers: Arc<RwLock<HashMap<String, String>>>) {
        client_auth_prepare_event::ClientAuthPrepareEvent::fire(api, headers)
    }

    pub fn BeforeTaskAcquire(api: &ServerAPI, task_id: String) -> bool {
        before_task_acquire_event::BeforeTaskAcquireEvent::check(api, task_id)
    }

    pub fn TaskAcquired(
        api: &ServerAPI,
        task_id: String,
        instance_id: String,
        payload: Arc<RwLock<Vec<u8>>>,
    ) {
        task_acquired_event::TaskAcquiredEvent::fire(api, task_id, instance_id, payload)
    }

    pub fn BeforeTaskExecute(
        api: &ServerAPI,
        task_id: String,
        instance_id: String,
        payload: Arc<RwLock<Vec<u8>>>,
    ) -> bool {
        before_task_execute_event::BeforeTaskExecuteEvent::check(api, task_id, instance_id, payload)
    }

    pub fn BeforeTaskPublish(
        api: &ServerAPI,
        task_id: String,
        instance_id: String,
        payload: Arc<RwLock<Vec<u8>>>,
    ) -> bool {
        before_task_publish_event::BeforeTaskPublishEvent::check(api, task_id, instance_id, payload)
    }

    pub fn ServerStart(api: &ServerAPI) {
        server_start_event::ServerStartEvent::fire(api)
    }

    pub fn ServerBeforeTaskCreate(
        api: &ServerAPI,
        task_id: String,
        payload: Arc<RwLock<Vec<u8>>>,
    ) -> bool {
        server_before_task_create_event::ServerBeforeTaskCreateEvent::check(api, task_id, payload)
    }

    pub fn ServerTaskBlockCreated(
        api: &ServerAPI,
        task_id: String,
        instance_ids: Vec<String>,
        payloads: Vec<Arc<RwLock<Vec<u8>>>>,
    ) {
        server_task_block_created_event::ServerTaskBlockCreatedEvent::fire(
            api,
            task_id,
            instance_ids,
            payloads,
        )
    }

    pub fn ServerBeforeTaskAcquire(api: &ServerAPI, uid: String, task_id: String) -> bool {
        server_before_task_acquire_event::ServerBeforeTaskAcquireEvent::check(api, uid, task_id)
    }

    pub fn ServerTaskBlockAcquired(
        api: &ServerAPI,
        uid: String,
        task_id: String,
        instance_ids: Vec<String>,
    ) {
        server_task_block_acquired_event::ServerTaskBlockAcquiredEvent::fire(
            api,
            uid,
            task_id,
            instance_ids,
        )
    }

    pub fn ServerBeforeTaskPublish(
        api: &ServerAPI,
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

    pub fn ServerTaskBlockPublished(
        api: &ServerAPI,
        uid: String,
        task_id: String,
        instance_ids: Vec<String>,
    ) {
        server_task_block_published_event::ServerTaskBlockPublishedEvent::fire(
            api,
            uid,
            task_id,
            instance_ids,
        )
    }
}
