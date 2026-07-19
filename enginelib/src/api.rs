use crate::api;
use crate::task::Task;
use crate::{Identifier, Registry, config::Config, event::EventBus, plugin::LibraryManager};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
pub use postcard;
pub use postcard::from_bytes;
pub use postcard::to_allocvec;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::{spawn, sync::RwLock, time::interval};
use tracing::{Level, debug, instrument};

pub struct ServerAPI {
    pub cfg: Config,                 // RW
    pub event_bus: EventBus,         // RW
    pub lib_manager: LibraryManager, // RW
    pub db: rust_rocksdb::DB,
    pub task_queue: TaskQueue,             // RW
    pub leased_tasks: LeasedTaskQueue,     // RW
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

        let mut k = Self {
            cfg: Config::default(),
            event_bus: EventBus::default(),
            lib_manager: LibraryManager::default(),
            db,
            task_registry: EngineTaskRegistry::default(),
            task_queue: TaskQueue::default(),
            leased_tasks: LeasedTaskQueue::default(),
        };
        LibraryManager::load_modules(&mut k);
        crate::event::register_inventory_handlers(&mut k);
        return k;
    }
}

#[derive(Default, Clone, Debug)]
pub struct EngineTaskRegistry {
    pub tasks: DashMap<Identifier, Arc<dyn Task>>,
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
#[derive(Debug, Default, Clone)]
pub struct TaskQueue {
    pub tasks: DashMap<
        Identifier,
        (
            async_channel::Sender<StoredTask>,
            async_channel::Receiver<StoredTask>,
        ),
    >,
}
#[derive(Debug, Default, Clone)]
pub struct LeasedTaskQueue {
    pub tasks: DashMap<Identifier, Vec<LeasedTask>>,
}
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StoredTask {
    pub bytes: Vec<u8>,
    pub task_id: String,
    pub task_type: Identifier,
}
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct LeasedTask {
    pub stored_task: Arc<StoredTask>,
    pub user_id: String,
    pub given_at: DateTime<Utc>,
}

impl LeasedTask {
    fn expired(&self) -> bool {
        if self.given_at.timestamp() + 36000 >= Utc::now().timestamp() {
            return false;
        }
        return true;
    }
}

impl LeasedTaskQueue {
    fn reap_expired(api: &Arc<ServerAPI>) {
        api.leased_tasks.tasks.retain(|_, tasks| {
            tasks.retain(|task| !task.expired());
            !tasks.is_empty()
        });
    }
}
