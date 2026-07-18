use crate::task::Task;
use crate::{Identifier, Registry, config::Config, event::EventBus, plugin::LibraryManager};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
pub use postcard;
pub use postcard::from_bytes;
pub use postcard::to_allocvec;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

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
        k
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
    pub id: String,
}
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct LeasedTask {
    pub stored_task: Arc<StoredTask>,
    pub user_id: String,
    pub given_at: DateTime<Utc>,
}
