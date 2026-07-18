use chrono::Utc;
use dashmap::{DashMap, DashSet};
use tokio::{spawn, time::interval};
use tracing::{Level, debug, info, instrument};

use crate::Task;
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
    // pub task_queue: TaskQueue,             // RW
    // pub leased_tasks: LeasedTaskQueue,     // RW
    pub task_registry: EngineTaskRegistry, // RW
}
impl Default for ServerAPI {
    fn default() -> Self {
        let path = "./engine_db";
        let mut opts = rust_rocksdb::Options::default();
        opts.create_if_missing(true);

        let db = rust_rocksdb::DB::open(&opts, path).unwrap();

        use std::sync::OnceLock;

        static INIT: OnceLock<()> = OnceLock::new();
        INIT.get_or_init(|| {
            #[cfg(debug_assertions)]
            let _ = tracing_subscriber::FmtSubscriber::builder()
                // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
                // will be written to stdout.
                .with_max_level(Level::DEBUG)
                // builds the subscriber.
                .try_init();
            #[cfg(not(debug_assertions))]
            let _ = tracing_subscriber::FmtSubscriber::builder()
                // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
                // will be written to stdout.
                .with_max_level(Level::INFO)
                // builds the subscriber.
                .try_init();
        });

        Self {
            cfg: RwLock::new(Config::default()),
            event_bus: EventBus::default(),
            lib_manager: LibraryManager::default(),
            db: db,
            task_registry: EngineTaskRegistry::default(),
        }
    }
}
impl ServerAPI {
    pub fn init(api: &mut Self) {
        api.lib_manager.load_modules(api);
    }
}
#[derive(Default, Clone, Debug)]
pub struct EngineTaskRegistry {
    pub tasks: HashMap<Identifier, Arc<dyn Task>>,
}
impl Registry<dyn Task> for EngineTaskRegistry {
    #[instrument]
    fn register(&mut self, task: Arc<dyn Task>, identifier: Identifier) {
        // Insert the task into the hashmap with (mod_id, identifier) as the key
        debug!(
            "TaskRegistry: Registering task {}.{}",
            identifier.0, identifier.1
        );
        self.tasks.insert(identifier, task);
    }

    fn get(&self, identifier: &Identifier) -> Option<Box<dyn Task>> {
        self.tasks.get(identifier).map(|obj| obj.clone_box())
    }
}
