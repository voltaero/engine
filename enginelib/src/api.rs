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
    pub cfg: Config,                       // RW
    pub task_queue: TaskQueue,             // RW
    pub leased_tasks: LeasedTaskQueue,     // RW
    pub task_registry: EngineTaskRegistry, // RW
    pub event_bus: EventBus,               // RW
    pub db: sled::Db,                      // R
    pub lib_manager: LibraryManager,       // RW
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
        }
    }
}
impl ServerAPI {
    pub fn test_default() -> Self {
        // `sled::Config::temporary(true)` defaults to `/dev/shm` on Linux when no path is set.
        // Some environments deny writes there, so force a unique temp path.
        use std::sync::atomic::{AtomicUsize, Ordering};

        static DB_COUNTER: AtomicUsize = AtomicUsize::new(0);
        let db_id = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
        let db_path = std::env::temp_dir().join(format!(
            "enginelib-test-db-{}-{}",
            std::process::id(),
            db_id
        ));

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
        }
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
        }

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
    const TASKS_PREFIX: &'static str = "tasks:";
    const SOLVED_PREFIX: &'static str = "solved:";

    fn state_key(prefix: &str, task_id: &Identifier, id: String) -> Vec<u8> {
        format!("{}{}\u{1f}{}:{}", prefix, task_id.0, task_id.1, id).into_bytes()
    }

    fn parse_state_key(prefix: &str, key: &[u8]) -> Option<Identifier> {
        let key = std::str::from_utf8(key).ok()?;
        let rest = key.strip_prefix(prefix)?;
        let (task_id, id) = rest.split_once(":")?;
        let (namespace, task) = task_id.split_once('\u{1f}')?;
        Some((namespace.to_string(), task.to_string()))
    }

    fn fill_queue(api: &ServerAPI, task_id: Identifier) {
        let max_block = api.cfg.config_toml.task_block_size.max(1) as usize;
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

        // Narrower prefix: only this task's records, not every TASKS_PREFIX row.
        let prefix = format!("{}{}\u{1f}{}:", Self::TASKS_PREFIX, task_id.0, task_id.1);
        let mut block: Vec<StoredTask> = Vec::with_capacity(max_block);

        for item in api.db.scan_prefix(prefix.as_bytes()) {
            let Ok((_, value)) = item else { continue };
            let Ok(task) = postcard::from_bytes::<StoredTask>(&value) else {
                continue;
            };
            if leased.contains(&task.id) {
                continue;
            }

            block.push(task);
            if block.len() == max_block {
                let full = std::mem::replace(&mut block, Vec::with_capacity(max_block));
                if sender.try_send(StoredTaskBlock { tasks: full }).is_err() {
                    return; // receiver dropped
                }
                if sender.len() >= max_queue {
                    return;
                }
            }
        }

        if !block.is_empty() {
            let _ = sender.try_send(StoredTaskBlock { tasks: block });
        }
    }

    fn init_db(api: &mut ServerAPI) {
        api.task_queue = TaskQueue::default();
        api.leased_tasks = LeasedTaskQueue::default();
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
    info!("Sled Cron Job Started");
    let mut interval = interval(Duration::from_secs(n_minutes * 60));
    // loop {
    //     interval.tick().await;
    //     info!("Purging Unsolved Tasks");

    //     let now = Utc::now().timestamp();
    //     let mut rw_api = api.write().await;

    //     let mut moved_tasks: Vec<(Identifier, StoredTask)> = Vec::new();
    //     let mut touched_exec: HashSet<Identifier> = HashSet::new();

    //     for (id, task_list) in rw_api.executing_tasks.tasks.iter_mut() {
    //         let before_len = task_list.len();
    //         task_list.retain(|info| {
    //             let age = now - info.given_at.timestamp();
    //             if age > 3600 {
    //                 info!("Task {:?} is older than an hour! Moving...", info);
    //                 moved_tasks.push((
    //                     id.clone(),
    //                     StoredTask {
    //                         id: info.id.clone(),
    //                         bytes: info.bytes.clone(),
    //                     },
    //                 ));
    //                 false
    //             } else {
    //                 true
    //             }
    //         });

    //         if task_list.len() != before_len {
    //             touched_exec.insert(id.clone());
    //         }
    //     }
    // }
}
