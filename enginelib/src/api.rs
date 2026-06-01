use chrono::Utc;
use crossbeam::queue::ArrayQueue;
use dashmap::DashMap;
use tokio::{spawn, sync::RwLock, time::interval};
use tracing::{Level, debug, error, info, instrument};

use crate::{
    Identifier, Registry,
    config::Config,
    event::{EngineEventHandlerRegistry, EventBus},
    plugin::LibraryManager,
    task::{LeasedTaskQueue, StoredTask, StoredTaskBlock, Task, TaskQueue},
};
pub use postcard;
pub use postcard::from_bytes;
pub use postcard::to_allocvec;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

pub struct ServerAPI {
    pub cfg: Config,                   // RW
    pub task_queue: TaskQueue,         // RW
    pub leased_tasks: LeasedTaskQueue, // RW
    pub active_task_ids: DashMap<Identifier, HashSet<String>>,
    pub task_registry: EngineTaskRegistry, // RW
    pub event_bus: EventBus,               // RW
    pub db: sled::Db,                      // R
    pub lib_manager: LibraryManager,       // RW
    // Serializes only sled refills per Identifier. Active task ids keep refills
    // from duplicating tasks already queued in memory or between recv+lease.
    pub fill_locks: DashMap<Identifier, Arc<tokio::sync::Mutex<()>>>,
}

impl Default for ServerAPI {
    fn default() -> Self {
        Self {
            cfg: Config::default(),
            task_queue: TaskQueue::default(),
            db: sled::open("engine_db").unwrap(),
            lib_manager: LibraryManager::default(),
            task_registry: EngineTaskRegistry::default(),
            event_bus: EventBus {
                event_handler_registry: EngineEventHandlerRegistry {
                    event_handlers: HashMap::new(),
                },
            },
            leased_tasks: LeasedTaskQueue::default(),
            active_task_ids: DashMap::new(),
            fill_locks: DashMap::new(),
        }
    }
}
impl ServerAPI {
    fn temporary_db_path(prefix: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicUsize, Ordering};

        static DB_COUNTER: AtomicUsize = AtomicUsize::new(0);
        let db_id = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
        let root = std::env::var_os("ENGINE_TEST_DB_ROOT")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| {
                std::env::current_dir()
                    .unwrap_or_else(|_| std::env::temp_dir())
                    .join("target")
                    .join("tmp")
            });
        let _ = std::fs::create_dir_all(&root);
        root.join(format!("{}-{}-{}", prefix, std::process::id(), db_id))
    }

    pub fn test_default() -> Self {
        let db_path = Self::temporary_db_path("enginelib-test-db");

        Self {
            cfg: Config::new(),
            leased_tasks: LeasedTaskQueue::default(),
            task_queue: TaskQueue::default(),
            db: sled::Config::new()
                .path(db_path)
                .temporary(true)
                .flush_every_ms(None)
                .open()
                .unwrap(),
            lib_manager: LibraryManager::default(),
            task_registry: EngineTaskRegistry::default(),
            event_bus: EventBus {
                event_handler_registry: EngineEventHandlerRegistry {
                    event_handlers: HashMap::new(),
                },
            },
            fill_locks: DashMap::new(),
            active_task_ids: DashMap::new(),
        }
    }
    /// Ensure the task_queue + leased_tasks entries exist for this Identifier.
    /// Idempotent — calling repeatedly is a no-op once registered.
    pub fn ensure_task_channel(&self, id: Identifier) {
        self.task_queue.tasks.entry(id.clone()).or_insert_with(|| {
            let (s, r) = async_channel::unbounded();
            (r, s)
        });
        self.leased_tasks.tasks.entry(id.clone()).or_default();
        self.active_task_ids.entry(id).or_default();
    }

    pub fn init(api: &mut Self) {
        Self::setup_logger();
        api.cfg = Config::new();
        Self::init_db(api);
        let mut new_lib_manager = LibraryManager::default();
        new_lib_manager.load_modules(api);
        api.lib_manager = new_lib_manager;
        for (id, _tsk) in api.task_registry.tasks.clone() {
            let (s, r) = async_channel::unbounded();
            api.task_queue.tasks.entry(id.clone()).insert((r, s));
            api.leased_tasks.tasks.entry(id.clone()).or_default();
            api.active_task_ids.entry(id.clone()).or_default();
        }

        Self::init_events(api);
    }

    /// Client-side ServerAPI with a temp sled (client doesn't use db/task_queue
    /// at all; the temp dir is just to satisfy the struct field).
    pub fn default_client() -> Self {
        let db_path = Self::temporary_db_path("enginelib-client-db");
        Self {
            cfg: Config::new(),
            task_queue: TaskQueue::default(),
            leased_tasks: LeasedTaskQueue::default(),
            task_registry: EngineTaskRegistry::default(),
            event_bus: EventBus {
                event_handler_registry: EngineEventHandlerRegistry {
                    event_handlers: HashMap::new(),
                },
            },
            db: sled::Config::new()
                .path(db_path)
                .temporary(true)
                .flush_every_ms(None)
                .open()
                .unwrap(),
            lib_manager: LibraryManager::default(),
            fill_locks: DashMap::new(),
            active_task_ids: DashMap::new(),
        }
    }

    /// Client init: logger + inventory event handlers. Skips load_modules
    /// because client mods are loaded via a different path and module
    /// validation happens against server metadata.
    pub fn init_client(api: &mut Self) {
        Self::setup_logger();
        Self::init_events(api);
    }

    fn init_events(api: &mut Self) {
        crate::event::register_inventory_handlers(api);
    }
    pub fn init_chron(api: Arc<RwLock<Self>>) {
        let t = api.try_read().unwrap().cfg.config_toml.clean_tasks;
        spawn(clear_sled_periodically(api, t));
    }
    // type:namespace:task:id
    pub const TASKS_PREFIX: &'static str = "tasks:";
    pub const SOLVED_PREFIX: &'static str = "solved:";

    fn state_key(prefix: &str, task_id: &Identifier, id: &str) -> Vec<u8> {
        format!("{}{}\u{1f}{}:{}", prefix, task_id.0, task_id.1, id).into_bytes()
    }

    fn state_prefix(prefix: &str, task_id: &Identifier) -> Vec<u8> {
        format!("{}{}\u{1f}{}:", prefix, task_id.0, task_id.1).into_bytes()
    }

    pub fn task_key(task_id: &Identifier, id: &str) -> Vec<u8> {
        Self::state_key(Self::TASKS_PREFIX, task_id, id)
    }

    pub fn solved_key(task_id: &Identifier, id: &str) -> Vec<u8> {
        Self::state_key(Self::SOLVED_PREFIX, task_id, id)
    }

    pub fn task_prefix(task_id: &Identifier) -> Vec<u8> {
        Self::state_prefix(Self::TASKS_PREFIX, task_id)
    }

    pub fn solved_prefix(task_id: &Identifier) -> Vec<u8> {
        Self::state_prefix(Self::SOLVED_PREFIX, task_id)
    }

    pub fn put_queued(&self, task_id: &Identifier, task: &StoredTask) -> sled::Result<()> {
        let bytes = postcard::to_allocvec(task)
            .map_err(|e| sled::Error::Unsupported(format!("postcard: {e}")))?;
        self.db.insert(Self::task_key(task_id, &task.id), bytes)?;
        Ok(())
    }

    pub fn put_queued_batch(&self, task_id: &Identifier, tasks: &[StoredTask]) -> sled::Result<()> {
        let mut batch = sled::Batch::default();
        for task in tasks {
            let bytes = postcard::to_allocvec(task)
                .map_err(|e| sled::Error::Unsupported(format!("postcard: {e}")))?;
            batch.insert(Self::task_key(task_id, &task.id), bytes);
        }
        self.db.apply_batch(batch)
    }

    pub fn put_solved(&self, task_id: &Identifier, task: &StoredTask) -> sled::Result<()> {
        let bytes = postcard::to_allocvec(task)
            .map_err(|e| sled::Error::Unsupported(format!("postcard: {e}")))?;
        self.db.insert(Self::solved_key(task_id, &task.id), bytes)?;
        Ok(())
    }

    pub fn delete_queued(&self, task_id: &Identifier, id: &str) -> sled::Result<bool> {
        Ok(self.db.remove(Self::task_key(task_id, id))?.is_some())
    }

    pub fn delete_solved(&self, task_id: &Identifier, id: &str) -> sled::Result<bool> {
        Ok(self.db.remove(Self::solved_key(task_id, id))?.is_some())
    }

    pub fn scan_queued(&self, task_id: &Identifier) -> impl Iterator<Item = StoredTask> + '_ {
        self.db
            .scan_prefix(Self::task_prefix(task_id))
            .filter_map(|item| item.ok())
            .filter_map(|(_, value)| postcard::from_bytes::<StoredTask>(&value).ok())
    }

    pub fn scan_solved(&self, task_id: &Identifier) -> impl Iterator<Item = StoredTask> + '_ {
        self.db
            .scan_prefix(Self::solved_prefix(task_id))
            .filter_map(|item| item.ok())
            .filter_map(|(_, value)| postcard::from_bytes::<StoredTask>(&value).ok())
    }

    pub fn mark_active_id(&self, task_id: &Identifier, id: String) {
        self.active_task_ids
            .entry(task_id.clone())
            .or_default()
            .insert(id);
    }

    pub fn mark_active_ids(&self, task_id: &Identifier, ids: &[String]) {
        if ids.is_empty() {
            return;
        }

        let mut active = self.active_task_ids.entry(task_id.clone()).or_default();
        for id in ids {
            active.insert(id.clone());
        }
    }

    pub fn clear_active_ids(&self, task_id: &Identifier, ids: &[String]) {
        if ids.is_empty() {
            return;
        }

        if let Some(mut active) = self.active_task_ids.get_mut(task_id) {
            for id in ids {
                active.remove(id);
            }
        }
    }

    pub fn fill_queue(api: &ServerAPI, task_id: Identifier, requested_block_size: u32) {
        let max_block = if requested_block_size > 0 {
            requested_block_size as usize
        } else {
            api.cfg.config_toml.task_block_size.max(1) as usize
        };
        let max_queue = api.cfg.config_toml.task_queue_size as usize;

        let Some(channel) = api.task_queue.tasks.get(&task_id) else {
            return;
        };
        let sender = &channel.1;

        // Soft size lock: bail out if the channel is already at its configured cap.
        // The cap is advisory — a partially-built trailing block may still push us
        // one block past the limit, but no full block is enqueued once we hit it.
        if sender.len() >= max_queue {
            return;
        }

        // Build a leased-id set once per call — O(L) instead of O(N·L) per scan item.
        let leased: HashSet<String> = api
            .leased_tasks
            .tasks
            .get(&task_id)
            .map(|v| v.iter().map(|l| l.stored_task.id.clone()).collect())
            .unwrap_or_default();
        let active: HashSet<String> = api
            .active_task_ids
            .get(&task_id)
            .map(|v| v.iter().cloned().collect())
            .unwrap_or_default();

        let mut block: Vec<StoredTask> = Vec::with_capacity(max_block);

        for item in api.db.scan_prefix(Self::task_prefix(&task_id)) {
            let Ok((_, value)) = item else { continue };
            let Ok(task) = postcard::from_bytes::<StoredTask>(&value) else {
                continue;
            };
            let is_active = active.contains(&task.id)
                || api
                    .active_task_ids
                    .get(&task_id)
                    .map(|ids| ids.contains(&task.id))
                    .unwrap_or(false);
            if leased.contains(&task.id) || is_active {
                continue;
            }

            block.push(task);
            if block.len() == max_block {
                let full = std::mem::replace(&mut block, Vec::with_capacity(max_block));
                let ids: Vec<String> = full.iter().map(|task| task.id.clone()).collect();
                api.mark_active_ids(&task_id, &ids);
                if sender.try_send(StoredTaskBlock { tasks: full }).is_err() {
                    api.clear_active_ids(&task_id, &ids);
                    return; // receiver dropped
                }
                if sender.len() >= max_queue {
                    return;
                }
            }
        }

        if !block.is_empty() {
            let ids: Vec<String> = block.iter().map(|task| task.id.clone()).collect();
            api.mark_active_ids(&task_id, &ids);
            if sender.try_send(StoredTaskBlock { tasks: block }).is_err() {
                api.clear_active_ids(&task_id, &ids);
            }
        }
    }

    fn init_db(api: &mut ServerAPI) {
        api.task_queue = TaskQueue::default();
        api.leased_tasks = LeasedTaskQueue::default();
        api.active_task_ids = DashMap::new();
    }

    pub fn setup_logger() {
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
                .with_max_level(Level::ERROR)
                // builds the subscriber.
                .try_init();
        });
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

pub async fn clear_sled_periodically(api: Arc<RwLock<ServerAPI>>, n_minutes: u64) {
    info!("Lease GC started ({}m interval)", n_minutes);
    let mut interval = interval(Duration::from_secs(n_minutes * 60));
    let ttl = chrono::Duration::seconds(3600);
    loop {
        interval.tick().await;
        let api = api.read().await;
        let now = Utc::now();
        let mut expired: u64 = 0;
        for mut entry in api.leased_tasks.tasks.iter_mut() {
            let before = entry.len();
            entry.retain(|l| now.signed_duration_since(l.given_at) < ttl);
            expired += (before - entry.len()) as u64;
        }
        if expired > 0 {
            info!("Lease GC: expired {} stale lease(s)", expired);
        }
    }
}
