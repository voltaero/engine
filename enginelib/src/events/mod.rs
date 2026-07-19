pub mod admin_auth_event;
pub mod auth_event;
pub mod before_task_block_acquire_event;
pub mod before_task_block_execute_event;
pub mod before_task_block_publish_event;
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
pub mod task_block_acquired_event;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use rust_rocksdb::DB;

use crate::{Identifier, api::ServerAPI};

#[allow(non_snake_case)]
pub fn ID(namespace: &str, id: &str) -> Identifier {
    (namespace.to_string(), id.to_string())
}
#[allow(non_snake_case)]
pub fn ID_from_string(id: &String) -> Identifier {
    (
        id.split(":").take(1).collect(),
        id.split(":").skip(1).collect(),
    )
}
pub struct Events;

#[allow(non_snake_case)]
impl Events {
    pub fn init_auth(_api: &mut ServerAPI) {}

    pub fn CheckAuth(
        api: &ServerAPI,
        uid: String,
        challenge: String,
        db: rust_rocksdb::DB,
    ) -> bool {
        auth_event::AuthEvent::check(api, uid, challenge, db)
    }

    pub fn CheckAdminAuth(api: &ServerAPI, payload: String, target: Identifier, db: DB) -> bool {
        admin_auth_event::AdminAuthEvent::check(api, payload, target, Arc::new(db))
    }

    pub fn CgrpcEvent(
        api: &ServerAPI,
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

    pub fn BeforeTaskBlockAcquire(api: &ServerAPI, task_ids: Vec<String>) -> bool {
        before_task_block_acquire_event::BeforeTaskBlockAcquireEvent::check(api, task_ids)
    }

    pub fn TaskBlockAcquired(
        api: &ServerAPI,
        task_id: String,
        instance_ids: Vec<String>,
        payloads: Vec<Arc<RwLock<Vec<u8>>>>,
    ) {
        task_block_acquired_event::TaskBlockAcquiredEvent::fire(
            api,
            task_id,
            instance_ids,
            payloads,
        )
    }

    pub fn BeforeTaskBlockExecute(
        api: &ServerAPI,
        task_id: String,
        instance_ids: Vec<String>,
        payloads: Vec<Arc<RwLock<Vec<u8>>>>,
    ) -> bool {
        before_task_block_execute_event::BeforeTaskBlockExecuteEvent::check(
            api,
            task_id,
            instance_ids,
            payloads,
        )
    }

    pub fn BeforeTaskBlockPublish(
        api: &ServerAPI,
        task_id: String,
        instance_ids: Vec<String>,
        payloads: Vec<Arc<RwLock<Vec<u8>>>>,
    ) -> bool {
        before_task_block_publish_event::BeforeTaskBlockPublishEvent::check(
            api,
            task_id,
            instance_ids,
            payloads,
        )
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
