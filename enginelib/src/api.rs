use chrono::Utc;
use tokio::{spawn, sync::RwLock, time::interval};
use tracing::{Level, debug, error, info, instrument};

use crate::{
    Identifier, Registry,
    config::Config,
    event::{EngineEventHandlerRegistry, EventBus},
    plugin::LibraryManager,
    task::{ExecutingTaskQueue, SolvedTasks, StoredTask, Task, TaskQueue},
};
pub use postcard;
pub use postcard::from_bytes;
pub use postcard::to_allocvec;
use std::{collections::HashMap, sync::Arc, time::Duration};
pub struct EngineAPI {
    pub cfg: Config,
    pub task_queue: TaskQueue,
    pub executing_tasks: ExecutingTaskQueue,
    pub solved_tasks: SolvedTasks,
    pub task_registry: EngineTaskRegistry,
    pub event_bus: EventBus,
    pub db: sled::Db,
    pub lib_manager: LibraryManager,
    pub client: bool,
}

impl Default for EngineAPI {
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
            solved_tasks: SolvedTasks::default(),
            executing_tasks: ExecutingTaskQueue::default(),
            client: false,
        }
    }
}
impl EngineAPI {
    pub fn default_client() -> Self {
        Self {
            cfg: Config::default(),
            task_queue: TaskQueue::default(),
            db: sled::open("engine_client_db").unwrap(),
            lib_manager: LibraryManager::default(),
            task_registry: EngineTaskRegistry::default(),
            event_bus: EventBus {
                event_handler_registry: EngineEventHandlerRegistry {
                    event_handlers: HashMap::new(),
                },
            },
            solved_tasks: SolvedTasks::default(),
            executing_tasks: ExecutingTaskQueue::default(),
            client: true,
        }
    }
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
            client: false,
            cfg: Config::new(),
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
            solved_tasks: SolvedTasks::default(),
            executing_tasks: ExecutingTaskQueue::default(),
        }
    }
    pub fn init(api: &mut Self) {
        Self::setup_logger();
        api.cfg = Config::new();
        Self::init_db(api);
        let mut new_lib_manager = LibraryManager::default();
        new_lib_manager.load_modules(api);
        api.lib_manager = new_lib_manager;
        for (id, _tsk) in api.task_registry.tasks.iter() {
            api.task_queue.tasks.entry(id.clone()).or_default();
            api.executing_tasks.tasks.entry(id.clone()).or_default();
            api.solved_tasks.tasks.entry(id.clone()).or_default();
        }

        Self::init_events(api);
    }

    pub fn init_client(api: &mut Self) {
        api.client = true;
        Self::setup_logger();
        let mut new_lib_manager = LibraryManager::default();
        new_lib_manager.load_modules(api);
        api.lib_manager = new_lib_manager;
        Self::init_events(api);
    }
    fn init_events(api: &mut Self) {
        crate::event::register_inventory_handlers(api);
    }
    pub fn init_packer(api: &mut Self) {
        Self::setup_logger();
        let mut newLibManager = LibraryManager::default();
        newLibManager.load_modules(api);
    }
    pub fn init_chron(api: Arc<RwLock<Self>>) {
        let t = api.try_read().unwrap().cfg.config_toml.clean_tasks;
        spawn(clear_sled_periodically(api, t));
    }
    pub fn sync_db(api: &mut EngineAPI) {
        // IF THIS FN CAUSES PANIC SOMETHING IS VERY BROKEN

        let tasks_db = postcard::to_allocvec(&api.task_queue.clone()).unwrap();
        api.db.insert("tasks", tasks_db).unwrap();

        let executing_tasks_db = postcard::to_allocvec(&api.executing_tasks.clone()).unwrap();
        api.db
            .insert("executing_tasks", executing_tasks_db)
            .unwrap();

        let solved_tasks_db = postcard::to_allocvec(&api.solved_tasks.clone()).unwrap();
        api.db.insert("solved_tasks", solved_tasks_db).unwrap();
        debug!("Synced In memory db to File db");
    }
    fn init_db(api: &mut EngineAPI) {
        let tasks = api.db.get("tasks");
        let exec_tasks = api.db.get("executing_tasks");
        let solved_tasks = api.db.get("solved_tasks");
        if tasks.is_err() || tasks.unwrap().is_none() {
            let store = postcard::to_allocvec(&api.task_queue.clone()).unwrap();
            api.db.insert("tasks", store).unwrap();
        } else {
            let store = api.db.get("tasks").unwrap().unwrap();
            let res: TaskQueue = postcard::from_bytes(&store).unwrap();
            api.task_queue = res;
        }
        if exec_tasks.is_err() || exec_tasks.unwrap().is_none() {
            let store = postcard::to_allocvec(&api.executing_tasks.clone()).unwrap();
            api.db.insert("executing_tasks", store).unwrap();
        } else {
            let store = api.db.get("executing_tasks").unwrap().unwrap();
            let res: ExecutingTaskQueue = postcard::from_bytes(&store).unwrap();
            api.executing_tasks = res;
        };
        if solved_tasks.is_err() || solved_tasks.unwrap().is_none() {
            let store = postcard::to_allocvec(&api.solved_tasks.clone()).unwrap();
            api.db.insert("solved_tasks", store).unwrap();
        } else {
            let store = api.db.get("solved_tasks").unwrap().unwrap();
            let res: SolvedTasks = postcard::from_bytes(&store).unwrap();
            api.solved_tasks = res;
        };
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
                .with_max_level(Level::INFO)
                // builds the subscriber.
                .try_init();
        });
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

pub async fn clear_sled_periodically(api: Arc<RwLock<EngineAPI>>, n_minutes: u64) {
    //EngineAPI::setup_logger();
    info!("Sled Cron Job Started");
    let mut interval = interval(Duration::from_secs(n_minutes * 60));
    loop {
        interval.tick().await; // Wait for the interval
        info!("Purging Unsolved Tasks");
        let now = Utc::now().timestamp(); // Current timestamp in seconds
        let mut moved_tasks: Vec<(String, String, StoredTask)> = Vec::new();
        let mut rw_api = api.write().await;
        let db = rw_api.db.clone();
        // Load "executing_tasks"
        if let Ok(Some(tsks)) = db.get("executing_tasks") {
            if let Ok(mut s) = postcard::from_bytes::<ExecutingTaskQueue>(&tsks) {
                for ((key1, key2), task_list) in s.tasks.iter_mut() {
                    task_list.retain(|info| {
                        let age = now - info.given_at.timestamp();
                        if age > 3600 {
                            info!("Task {:?} is older than an hour! Moving...", info);
                            moved_tasks.push((
                                key1.clone(),
                                key2.clone(),
                                StoredTask {
                                    id: info.id.clone(),
                                    bytes: info.bytes.clone(),
                                },
                            ));
                            false // Remove old tasks
                        } else {
                            true // Keep tasks that are less than an hour old
                        }
                    });
                }

                // Save updated "executing_tasks"
                if let Ok(updated) = postcard::to_allocvec(&s) {
                    if let Err(e) = db.insert("executing_tasks", updated) {
                        error!("Failed to update executing_tasks in Sled: {:?}", e);
                    }
                }
            }
        }

        // Merge moved tasks into "tasks"
        if !moved_tasks.is_empty() {
            let mut saved_tasks = TaskQueue {
                tasks: HashMap::new(),
            };

            if let Ok(Some(saved_tsks)) = db.get("tasks") {
                if let Ok(existing_tasks) = postcard::from_bytes::<TaskQueue>(&saved_tsks) {
                    saved_tasks = existing_tasks;
                }
            }

            // Add moved tasks
            for (key1, key2, task) in moved_tasks {
                saved_tasks
                    .tasks
                    .entry((key1, key2))
                    .or_default()
                    .push(task);
            }

            // Save updated "tasks" queue
            if let Ok(updated) = postcard::to_allocvec(&saved_tasks) {
                if let Err(e) = db.insert("tasks", updated) {
                    error!("Failed to update tasks in Sled: {:?}", e);
                }
            }
        }
        EngineAPI::init_db(&mut rw_api);
    }
}
