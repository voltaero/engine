use crate::error::Error;
use crate::task::Task;
use crate::{Identifier, Registry, config::Config, event::EventBus, plugin::LibraryManager};
use chrono::{DateTime, Utc};
use dashmap::{DashMap, DashSet};
pub use postcard;
pub use postcard::from_bytes;
pub use postcard::to_allocvec;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{Level, debug, instrument};

// t:namespace:task_name:<id> -> Serialized Task Record
pub fn task_key_prefix(task_type: &Identifier) -> String {
    format!("t:{}:{}:", task_type.0, task_type.1)
}

pub fn task_key(task_type: &Identifier, task_id: &str) -> String {
    format!("t:{}:{}:{}", task_type.0, task_type.1, task_id)
}

// f:namespace:task_name:<id> -> Finished Task Record
pub fn finished_task_key(task_type: &Identifier, task_id: &str) -> String {
    format!("f:{}:{}:{}", task_type.0, task_type.1, task_id)
}

pub fn finished_task_key_prefix(task_type: &Identifier) -> String {
    format!("f:{}:{}:", task_type.0, task_type.1)
}

pub fn task_id_from_key(key: &str) -> Option<&str> {
    key.splitn(4, ':').nth(3)
}

pub struct ServerAPI {
    pub cfg: Config,                 // RW
    pub event_bus: EventBus,         // RW
    pub lib_manager: LibraryManager, // RW
    pub db: Arc<rust_rocksdb::DB>,
    pub task_queue: TaskQueue,             // RW
    pub leased_tasks: LeasedTaskQueue,     // RW
    pub task_registry: EngineTaskRegistry, // RW
}

impl Default for ServerAPI {
    fn default() -> Self {
        let path = "./engine_db";
        let mut opts = rust_rocksdb::Options::default();
        opts.create_if_missing(true);

        let db = Arc::new(rust_rocksdb::DB::open(&opts, path).unwrap());

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
impl ServerAPI {
    pub async fn load(api: &Arc<Self>, task_type: Identifier) -> Result<(), Error> {
        const LOAD_BATCH_SIZE: usize = 4096;
        let prefix = task_key_prefix(&task_type);

        // Collect the batch while holding the map guard, but never await under it:
        // send() on a full channel would otherwise block all writers to this shard.
        let (sender, batch) = {
            let k = api
                .task_queue
                .tasks
                .get(&task_type)
                .ok_or(Error::not_found("TaskTypeNotFound"))?;

            let mut batch = Vec::new();
            for result in api.db.prefix_iterator(prefix.as_bytes()) {
                if batch.len() >= LOAD_BATCH_SIZE {
                    break;
                }
                let (key, value) = match result {
                    Ok(kv) => kv,
                    Err(err) => {
                        eprintln!("RocksDB read error: {err}");
                        continue;
                    }
                };
                if !key.starts_with(prefix.as_bytes()) {
                    break;
                }
                let Ok(key) = String::from_utf8(key.to_vec()) else {
                    continue;
                };
                let Some(task_id) = task_id_from_key(&key) else {
                    continue;
                };
                if k.2.insert(task_id.to_string()) {
                    batch.push(StoredTask {
                        bytes: value.into(),
                        task_id: task_id.to_string(),
                        task_type: task_type.clone(),
                    });
                }
            }
            (k.0.clone(), batch)
        };

        let mut batch = batch.into_iter();
        while let Some(task) = batch.next() {
            if let Err(err) = sender.send(task).await {
                // Un-mark everything that never made it into the queue so a
                // later load() can retry those tasks.
                if let Some(k) = api.task_queue.tasks.get(&task_type) {
                    k.2.remove(&err.0.task_id);
                    for task in batch {
                        k.2.remove(&task.task_id);
                    }
                }
                return Err(Error::new(format!("Failed to send stored task: {err}")));
            }
        }

        Ok(())
    }
    pub fn populate(api: &Arc<Self>) {
        api.task_registry.tasks.iter().for_each(|f| {
            let key = f.key();
            let (tx, rx) = async_channel::bounded(8192); // Add to config or make unbound ?
            api.task_queue
                .tasks
                .entry(key.clone())
                .or_insert((tx, rx, DashSet::default()));
            api.leased_tasks.tasks.entry(key.clone()).or_default();
            // task reg should be populated by mods
        });
    }
    pub fn init() -> Arc<Self> {
        let api = Arc::new(Self::default());
        Self::populate(&api);
        let dapi = api.clone();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_secs(3600));
                LeasedTaskQueue::reap_expired(&dapi);
            }
        });
        api
    }
}

#[derive(Default, Clone, Debug)]
pub struct EngineTaskRegistry {
    pub tasks: DashMap<Identifier, Arc<dyn Task>>,
}
impl Registry<dyn Task> for EngineTaskRegistry {
    #[instrument(skip_all, fields(namespace = %identifier.0, name = %identifier.1))]
    fn register(&mut self, task: Arc<dyn Task>, identifier: Identifier) {
        // Insert the task into the hashmap with (mod_id, identifier) as the key
        debug!(
            "TaskRegistry: Registering task {}.{}",
            identifier.0, identifier.1
        );
        self.tasks.insert(identifier, task);
    }
    #[instrument(skip_all, fields(namespace = %identifier.0, name = %identifier.1))]
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
            dashmap::DashSet<String>,
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

const LEASE_TTL_SECS: i64 = 3600;

impl LeasedTask {
    fn expired(&self) -> bool {
        self.given_at.timestamp() + LEASE_TTL_SECS < Utc::now().timestamp()
    }
}

impl LeasedTaskQueue {
    fn reap_expired(api: &Arc<ServerAPI>) {
        let mut reaped: Vec<(Identifier, String)> = Vec::new();
        api.leased_tasks.tasks.retain(|task_type, tasks| {
            tasks.retain(|task| {
                if task.expired() {
                    reaped.push((task_type.clone(), task.stored_task.task_id.clone()));
                    false
                } else {
                    true
                }
            });
            !tasks.is_empty()
        });
        // Clear reaped ids from the dedup set outside the leased_tasks locks so
        // load() can re-enqueue the tasks from their still-persisted records.
        for (task_type, task_id) in reaped {
            if let Some(queue) = api.task_queue.tasks.get(&task_type) {
                queue.2.remove(&task_id);
            }
        }
    }
}
