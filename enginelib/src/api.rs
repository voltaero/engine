use chrono::Utc;
use tokio::{spawn, sync::RwLock, time::interval};
use tracing::{Level, debug, error, info, instrument};

use crate::{
    Identifier, Registry,
    config::Config,
    event::{EngineEventHandlerRegistry, EventBus},
    plugin::LibraryManager,
    task::{LeasedTaskQueue, SolvedTasks, StoredTask, Task, TaskQueue},
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
    pub solved_tasks: SolvedTasks,         // RW
    pub task_registry: EngineTaskRegistry, // RW
    pub event_bus: EventBus,               // RW
    pub db: sled::Db,                      // R
    pub lib_manager: LibraryManager,       // RW
    pub client: bool,                      // RW
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
            solved_tasks: SolvedTasks::default(),
            leased_tasks: LeasedTaskQueue::default(),
            client: false,
        }
    }
}
impl ServerAPI {
    // pub fn default_client() -> Self {
    //     Self {
    //         cfg: Config::default(),
    //         task_queue: TaskQueue::default(),
    //         db: sled::open("engine_client_db").unwrap(),
    //         lib_manager: LibraryManager::default(),
    //         task_registry: EngineTaskRegistry::default(),
    //         event_bus: EventBus {
    //             event_handler_registry: EngineEventHandlerRegistry {
    //                 event_handlers: HashMap::new(),
    //             },
    //         },
    //         solved_tasks: SolvedTasks::default(),
    //         executing_tasks: ExecutingTaskQueue::default(),
    //         client: true,
    //     }
    //}
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
            api.leased_tasks.tasks.entry(id.clone()).or_default();
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
    const TASKS_PREFIX: &'static str = "q:tasks:";
    const EXECUTING_PREFIX: &'static str = "q:executing:";
    const SOLVED_PREFIX: &'static str = "q:solved:";

    fn state_key(prefix: &str, id: &Identifier) -> Vec<u8> {
        format!("{}{}\u{1f}{}", prefix, id.0, id.1).into_bytes()
    }

    fn parse_state_key(prefix: &str, key: &[u8]) -> Option<Identifier> {
        let key = std::str::from_utf8(key).ok()?;
        let rest = key.strip_prefix(prefix)?;
        let (namespace, task) = rest.split_once('\u{1f}')?;
        Some((namespace.to_string(), task.to_string()))
    }

    pub fn apply_batch_entries(
        db: &sled::Db,
        entries: Vec<(&'static str, Vec<u8>)>,
    ) -> sled::Result<()> {
        let mut batch = sled::Batch::default();
        for (key, value) in entries {
            batch.insert(key, value);
        }
        db.apply_batch(batch)
    }

    pub fn apply_batch_ops(
        db: &sled::Db,
        ops: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    ) -> sled::Result<()> {
        let mut batch = sled::Batch::default();
        for (key, value) in ops {
            match value {
                Some(v) => batch.insert(key, v),
                None => batch.remove(key),
            }
        }
        db.apply_batch(batch)
    }

    pub fn state_op_tasks(
        id: &Identifier,
        value: &Vec<StoredTask>,
    ) -> Result<(Vec<u8>, Option<Vec<u8>>), postcard::Error> {
        if value.is_empty() {
            Ok((Self::state_key(Self::TASKS_PREFIX, id), None))
        } else {
            Ok((
                Self::state_key(Self::TASKS_PREFIX, id),
                Some(postcard::to_allocvec(value)?),
            ))
        }
    }

    pub fn state_op_executing(
        id: &Identifier,
        value: &Vec<StoredExecutingTask>,
    ) -> Result<(Vec<u8>, Option<Vec<u8>>), postcard::Error> {
        if value.is_empty() {
            Ok((Self::state_key(Self::EXECUTING_PREFIX, id), None))
        } else {
            Ok((
                Self::state_key(Self::EXECUTING_PREFIX, id),
                Some(postcard::to_allocvec(value)?),
            ))
        }
    }

    pub fn state_op_solved(
        id: &Identifier,
        value: &Vec<StoredTask>,
    ) -> Result<(Vec<u8>, Option<Vec<u8>>), postcard::Error> {
        if value.is_empty() {
            Ok((Self::state_key(Self::SOLVED_PREFIX, id), None))
        } else {
            Ok((
                Self::state_key(Self::SOLVED_PREFIX, id),
                Some(postcard::to_allocvec(value)?),
            ))
        }
    }

    pub fn sync_db(api: &mut ServerAPI) {
        // IF THIS FN CAUSES PANIC SOMETHING IS VERY BROKEN
        let mut ops: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();

        for prefix in [
            Self::TASKS_PREFIX,
            Self::EXECUTING_PREFIX,
            Self::SOLVED_PREFIX,
        ] {
            for item in api.db.scan_prefix(prefix.as_bytes()) {
                if let Ok((key, _)) = item {
                    ops.push((key.to_vec(), None));
                }
            }
        }

        for (id, tasks) in &api.task_queue.tasks {
            ops.push(Self::state_op_tasks(id, tasks).unwrap());
        }
        for (id, tasks) in &api.executing_tasks.tasks {
            ops.push(Self::state_op_executing(id, tasks).unwrap());
        }
        for (id, tasks) in &api.solved_tasks.tasks {
            ops.push(Self::state_op_solved(id, tasks).unwrap());
        }

        Self::apply_batch_ops(&api.db, ops).unwrap();
        debug!("Synced in-memory state to keyed sled storage");
    }

    fn init_db(api: &mut ServerAPI) {
        api.task_queue = TaskQueue::default();
        api.executing_tasks = ExecutingTaskQueue::default();
        api.solved_tasks = SolvedTasks::default();

        let mut found_keyed_state = false;

        for item in api.db.scan_prefix(Self::TASKS_PREFIX.as_bytes()) {
            if let Ok((key, value)) = item {
                if let Some(id) = Self::parse_state_key(Self::TASKS_PREFIX, &key) {
                    if let Ok(tasks) = postcard::from_bytes::<Vec<StoredTask>>(&value) {
                        api.task_queue.tasks.insert(id, tasks);
                        found_keyed_state = true;
                    }
                }
            }
        }

        for item in api.db.scan_prefix(Self::EXECUTING_PREFIX.as_bytes()) {
            if let Ok((key, value)) = item {
                if let Some(id) = Self::parse_state_key(Self::EXECUTING_PREFIX, &key) {
                    if let Ok(tasks) = postcard::from_bytes::<Vec<StoredExecutingTask>>(&value) {
                        api.executing_tasks.tasks.insert(id, tasks);
                        found_keyed_state = true;
                    }
                }
            }
        }

        for item in api.db.scan_prefix(Self::SOLVED_PREFIX.as_bytes()) {
            if let Ok((key, value)) = item {
                if let Some(id) = Self::parse_state_key(Self::SOLVED_PREFIX, &key) {
                    if let Ok(tasks) = postcard::from_bytes::<Vec<StoredTask>>(&value) {
                        api.solved_tasks.tasks.insert(id, tasks);
                        found_keyed_state = true;
                    }
                }
            }
        }

        if found_keyed_state {
            return;
        }

        // Legacy fallback migration path.
        let tasks = api.db.get("tasks");
        let exec_tasks = api.db.get("executing_tasks");
        let solved_tasks = api.db.get("solved_tasks");

        if let Ok(Some(store)) = tasks {
            let res: TaskQueue = postcard::from_bytes(&store).unwrap();
            api.task_queue = res;
        }

        if let Ok(Some(store)) = exec_tasks {
            let res: ExecutingTaskQueue = postcard::from_bytes(&store).unwrap();
            api.executing_tasks = res;
        }

        if let Ok(Some(store)) = solved_tasks {
            let res: SolvedTasks = postcard::from_bytes(&store).unwrap();
            api.solved_tasks = res;
        }

        // Migrate legacy snapshot into keyed storage.
        Self::sync_db(api);
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

pub async fn clear_sled_periodically(api: Arc<RwLock<ServerAPI>>, n_minutes: u64) {
    info!("Sled Cron Job Started");
    let mut interval = interval(Duration::from_secs(n_minutes * 60));
    loop {
        interval.tick().await;
        info!("Purging Unsolved Tasks");

        let now = Utc::now().timestamp();
        let mut rw_api = api.write().await;

        let mut moved_tasks: Vec<(Identifier, StoredTask)> = Vec::new();
        let mut touched_exec: HashSet<Identifier> = HashSet::new();

        for (id, task_list) in rw_api.executing_tasks.tasks.iter_mut() {
            let before_len = task_list.len();
            task_list.retain(|info| {
                let age = now - info.given_at.timestamp();
                if age > 3600 {
                    info!("Task {:?} is older than an hour! Moving...", info);
                    moved_tasks.push((
                        id.clone(),
                        StoredTask {
                            id: info.id.clone(),
                            bytes: info.bytes.clone(),
                        },
                    ));
                    false
                } else {
                    true
                }
            });

            if task_list.len() != before_len {
                touched_exec.insert(id.clone());
            }
        }

        let mut touched_tasks: HashSet<Identifier> = HashSet::new();
        for (id, task) in moved_tasks {
            rw_api
                .task_queue
                .tasks
                .entry(id.clone())
                .or_default()
                .push(task);
            touched_tasks.insert(id);
        }

        if touched_exec.is_empty() && touched_tasks.is_empty() {
            continue;
        }

        let mut ops: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();

        for id in touched_exec {
            let value = rw_api
                .executing_tasks
                .tasks
                .get(&id)
                .cloned()
                .unwrap_or_default();
            match ServerAPI::state_op_executing(&id, &value) {
                Ok(op) => ops.push(op),
                Err(e) => error!("Failed to serialize executing_tasks entry: {:?}", e),
            }
        }

        for id in touched_tasks {
            let value = rw_api
                .task_queue
                .tasks
                .get(&id)
                .cloned()
                .unwrap_or_default();
            match ServerAPI::state_op_tasks(&id, &value) {
                Ok(op) => ops.push(op),
                Err(e) => error!("Failed to serialize tasks entry: {:?}", e),
            }
        }

        if let Err(e) = ServerAPI::apply_batch_ops(&rw_api.db, ops) {
            error!("Failed to update task state in Sled batch: {:?}", e);
        }
    }
}
