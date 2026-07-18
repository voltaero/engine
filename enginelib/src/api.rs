use chrono::Utc;
use dashmap::{DashMap, DashSet};
use tokio::{spawn, time::interval};
use tracing::{Level, debug, info, instrument};

use crate::{
    Identifier, Registry,
    config::Config,
    event::{EngineEventHandlerRegistry, EventBus},
    plugin::LibraryManager,
};
pub use postcard;
pub use postcard::from_bytes;
pub use postcard::to_allocvec;
use std::{
    collections::HashMap,
    sync::RwLock,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub struct ServerAPI {
    pub cfg: RwLock<Config>,         // RW
    pub event_bus: EventBus,         // RW
    pub lib_manager: LibraryManager, // RW
    pub db: rust_rocksdb::DB,
}
impl Default for ServerAPI {
    fn default() -> Self {
        let path = "./engine_db";
        let mut opts = rust_rocksdb::Options::default();
        opts.create_if_missing(true);

        let db = rust_rocksdb::DB::open(&opts, path).unwrap();
        Self {
            cfg: RwLock::new(Config::default()),
            event_bus: EventBus::default(),
            lib_manager: LibraryManager::default(),
            db: db,
        }
    }
}
