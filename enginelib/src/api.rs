use crate::error::Error;
use crate::task::Task;
use crate::{Identifier, Registry, config::Config, event::EventBus, plugin::LibraryManager};
use chrono::{DateTime, Utc};
use dashmap::{DashMap, DashSet};
pub use postcard;
pub use postcard::from_bytes;
pub use postcard::to_allocvec;
use rust_rocksdb::{Direction, IteratorMode};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::{Level, debug, error, instrument};

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

impl ServerAPI {
    /// Initialize the global tracing subscriber once (idempotent). Referenced by
    /// the `#[module]` macro's generated `run` prologue as well as `with_path`.
    pub fn setup_logger() {
        use std::sync::OnceLock;
        static INIT: OnceLock<()> = OnceLock::new();
        INIT.get_or_init(|| {
            #[cfg(debug_assertions)]
            let _ = tracing_subscriber::FmtSubscriber::builder()
                .with_max_level(Level::DEBUG)
                .try_init();
            #[cfg(not(debug_assertions))]
            let _ = tracing_subscriber::FmtSubscriber::builder()
                .with_max_level(Level::INFO)
                .try_init();
        });
    }

    /// Open (or create) a `ServerAPI` backed by the RocksDB store at `path`.
    /// [`Default`] uses the standard `./engine_db`; tests pass a temp path so
    /// they don't collide on the shared store.
    pub fn with_path(path: &str) -> Self {
        Self::with_path_and_config(path, Config::default())
    }

    /// Open a database with an explicitly loaded configuration. Production
    /// startup uses this path so transport and auth settings are not discarded.
    pub fn with_path_and_config(path: &str, config: Config) -> Self {
        let mut opts = rust_rocksdb::Options::default();
        opts.create_if_missing(true);

        let db = Arc::new(
            rust_rocksdb::DB::open(&opts, path)
                .expect("Failed to open RocksDB — check that the path is writable and no other process holds the lock"),
        );

        Self::setup_logger();

        let mut k = Self {
            cfg: config,
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

impl Default for ServerAPI {
    fn default() -> Self {
        Self::with_path("./engine_db")
    }
}
impl ServerAPI {
    /// Enqueue up to one batch of persisted-but-unqueued records for `task_type`,
    /// returning how many were enqueued. Used for startup recovery: call it in a
    /// loop until it returns 0 to drain the whole backlog (see
    /// [`ServerAPI::load_all`]).
    pub async fn load(api: &Arc<Self>, task_type: Identifier) -> Result<usize, Error> {
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

        let count = batch.len();
        let mut batch = batch.into_iter();
        while let Some(task) = batch.next() {
            if let Err(err) = sender.send(task).await {
                // Un-mark everything that never made it into the queue, and set
                // the rescan flag so a later load() or the running loader can
                // retry those tasks.
                if let Some(k) = api.task_queue.tasks.get(&task_type) {
                    k.2.remove(&err.0.task_id);
                    for task in batch {
                        k.2.remove(&task.task_id);
                    }
                    k.3.store(true, Ordering::Release);
                }
                return Err(Error::new(format!("Failed to send stored task: {err}")));
            }
        }

        Ok(count)
    }

    /// Drain the entire persisted backlog for `task_type` into the queue by
    /// looping [`load`] until it enqueues nothing. Enqueue is backpressured, so
    /// this streams the backlog in bounded memory. Returns the total enqueued.
    pub async fn load_all(api: &Arc<Self>, task_type: Identifier) -> Result<usize, Error> {
        let mut total = 0;
        loop {
            let n = Self::load(api, task_type.clone()).await?;
            total += n;
            if n == 0 {
                break;
            }
        }
        Ok(total)
    }

    /// Enqueue one batch of records for `task_type` whose key sorts strictly after
    /// `cursor` (or from the start of the prefix when `cursor` is `None`),
    /// returning `(count, new_cursor)`.
    ///
    /// This exploits the fact that `druid` ids are timestamp-ordered (big-endian
    /// nanosecond prefix), so the `t:` keys are lexicographically ordered by
    /// submission time. A newer submit always sorts *after* an older one, so the
    /// loader resumes from a cursor with a direct RocksDB seek — no re-scan of the
    /// prefix, no walking delete-tombstones (completed records sit behind the
    /// cursor), and each record is read exactly once (no duplicate enqueue).
    pub async fn load_after(
        api: &Arc<Self>,
        task_type: &Identifier,
        cursor: Option<String>,
    ) -> Result<(usize, Option<String>), Error> {
        const LOAD_BATCH_SIZE: usize = 4096;
        let prefix = task_key_prefix(task_type);
        let seek = cursor.clone().unwrap_or_else(|| prefix.clone());

        let (sender, batch, last) = {
            let k = api
                .task_queue
                .tasks
                .get(task_type)
                .ok_or(Error::not_found("TaskTypeNotFound"))?;

            let mut batch = Vec::new();
            let mut last = cursor.clone();
            let iter = api
                .db
                .iterator(IteratorMode::From(seek.as_bytes(), Direction::Forward));
            for result in iter {
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
                // `From` is inclusive; never re-emit the cursor key itself.
                if cursor.as_deref() == Some(key.as_str()) {
                    continue;
                }
                if let Some(task_id) = task_id_from_key(&key)
                    && k.2.insert(task_id.to_string())
                {
                    batch.push(StoredTask {
                        bytes: value.into(),
                        task_id: task_id.to_string(),
                        task_type: task_type.clone(),
                    });
                }
                // Advance the cursor past every key seen, so skipped keys aren't
                // revisited next call.
                last = Some(key);
            }
            (k.0.clone(), batch, last)
        };

        let count = batch.len();
        let mut batch = batch.into_iter();
        while let Some(task) = batch.next() {
            if let Err(err) = sender.send(task).await {
                // None of these tasks reached the channel. Clear their in-flight
                // markers and rewind the loader so the persisted records can be
                // recovered if the queue is recreated.
                if let Some(k) = api.task_queue.tasks.get(task_type) {
                    k.2.remove(&err.0.task_id);
                    for task in batch {
                        k.2.remove(&task.task_id);
                    }
                    k.3.store(true, Ordering::Release);
                }
                return Err(Error::new(format!("Failed to send stored task: {err}")));
            }
        }
        Ok((count, last))
    }

    /// Run the loader for `task_type` forever: continuously stream
    /// persisted-but-unqueued records into the bounded channel via [`load_after`].
    ///
    /// This is the queue's engine. `submit` only writes to the DB; this task moves
    /// those records into the lease channel — with backpressure, since
    /// `send().await` blocks while the channel is full. It carries a forward cursor
    /// (works because ids are time-ordered), so in steady state it never re-scans;
    /// startup/restart recovery is the same path with `cursor = None`.
    ///
    /// The one exception: recovery paths (lease reaping, cancel with a full
    /// channel, complete's DB-failure paths) drop a task back to "persisted only"
    /// by clearing its dedup entry — and that record now sits *behind* the cursor.
    /// Those paths set the per-type rescan flag after clearing the entry; when the
    /// flag is set the loader rewinds its cursor and re-walks the prefix. The
    /// dedup set makes the re-scan duplicate-free, so the only cost is the walk
    /// itself, paid only when a recovery actually happened. When the DB is
    /// momentarily caught up it sleeps briefly to avoid busy-spinning.
    pub async fn run_loader(api: Arc<Self>, task_type: Identifier) {
        use std::time::Duration;
        let mut cursor: Option<String> = None;
        loop {
            if let Some(queue) = api.task_queue.tasks.get(&task_type) {
                // Recovery signal: a cleared dedup entry is behind the cursor.
                if queue.3.swap(false, Ordering::AcqRel) {
                    cursor = None;
                }
            }
            match Self::load_after(&api, &task_type, cursor.clone()).await {
                // Keep the cursor even when nothing was enqueued: it advanced
                // past dedup-skipped keys, and dropping it would make the next
                // iteration re-walk them (a busy re-scan after a rewind).
                Ok((0, next)) => {
                    cursor = next;
                    tokio::time::sleep(Duration::from_millis(1)).await
                }
                Ok((_, next)) => cursor = next, // advanced; loop immediately
                Err(err) => {
                    eprintln!("loader error for {task_type:?}: {err}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }

    /// Spawn one [`run_loader`] task per registered task type. Call once, inside a
    /// tokio runtime, after [`populate`].
    pub fn spawn_loaders(api: &Arc<Self>) {
        for entry in api.task_registry.tasks.iter() {
            tokio::spawn(Self::run_loader(api.clone(), entry.key().clone()));
        }
    }
    pub fn populate(api: &Arc<Self>) {
        Self::populate_with(api, 8192);
    }

    /// Like [`populate`] but with a configurable per-type lease-channel capacity.
    /// Used to sweep queue depth in benchmarks.
    pub fn populate_with(api: &Arc<Self>, queue_size: usize) {
        api.task_registry.tasks.iter().for_each(|f| {
            let key = f.key();
            let (tx, rx) = async_channel::bounded(queue_size.max(1));
            api.task_queue.tasks.entry(key.clone()).or_insert((
                tx,
                rx,
                DashSet::default(),
                Arc::new(AtomicBool::new(false)),
            ));
            api.leased_tasks.tasks.entry(key.clone()).or_default();
            // task reg should be populated by mods
        });
    }
    pub fn init() -> Arc<Self> {
        Self::init_with_config(Config::default())
    }

    pub fn init_with_config(config: Config) -> Arc<Self> {
        let api = Arc::new(Self::with_path_and_config("./engine_db", config));
        Self::populate(&api);
        Self::spawn_reaper(&api);
        api
    }

    /// Spawn the background reaper thread: every [`REAP_INTERVAL_SECS`] it reclaims
    /// leases whose TTL has elapsed and requeues their tasks. Called by [`init`];
    /// call it directly when building a `ServerAPI` via [`with_path`] and you want
    /// the same behavior.
    pub fn spawn_reaper(api: &Arc<Self>) {
        let api = api.clone();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_secs(REAP_INTERVAL_SECS));
                LeasedTaskQueue::reap_expired(&api);
            }
        });
    }
}

#[derive(Default, Clone, Debug)]
pub struct EngineTaskRegistry {
    pub tasks: DashMap<Identifier, Arc<dyn Task>>,
}
impl Registry<dyn Task> for EngineTaskRegistry {
    #[instrument(skip_all, fields(namespace = %identifier.0, name = %identifier.1))]
    fn register(&mut self, task: Arc<dyn Task>, identifier: Identifier) {
        // ':' is the DB key separator (`t:ns:name:id`), so an identifier
        // containing it would collide with other types' key prefixes. Refuse it.
        if identifier.0.contains(':') || identifier.1.contains(':') {
            error!(
                "TaskRegistry: refusing to register {}.{}: identifiers must not contain ':'",
                identifier.0, identifier.1
            );
            return;
        }
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
/// Per task type: the bounded lease channel, the in-flight dedup set (ids that
/// are queued or leased — blocks the loader from re-enqueuing them), and the
/// rescan flag. Recovery paths set the flag *after* removing an id from the
/// dedup set to tell [`ServerAPI::run_loader`] its cursor has passed a record
/// that must be re-enqueued, so it rewinds and re-walks the prefix.
pub type TaskQueueEntry = (
    async_channel::Sender<StoredTask>,
    async_channel::Receiver<StoredTask>,
    dashmap::DashSet<String>,
    Arc<AtomicBool>,
);

#[derive(Debug, Default, Clone)]
pub struct TaskQueue {
    pub tasks: DashMap<Identifier, TaskQueueEntry>,
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
/// How often the reap thread wakes. Much smaller than the TTL so an expired
/// lease is noticed within ~a minute of expiring, not up to a whole extra TTL.
const REAP_INTERVAL_SECS: u64 = 60;

impl LeasedTask {
    fn expired(&self) -> bool {
        self.given_at.timestamp() + LEASE_TTL_SECS < Utc::now().timestamp()
    }
}

impl LeasedTaskQueue {
    /// Drop every expired lease and hand its task back to the queue.
    ///
    /// The reaped lease still holds the `StoredTask`, so the task is requeued
    /// directly with a non-blocking `try_send` (dedup entry kept: the task goes
    /// straight from "leased" back to "queued"). Only if the channel is full or
    /// closed does it fall back to clearing the dedup entry and setting the
    /// rescan flag so [`ServerAPI::run_loader`] re-enqueues the still-persisted
    /// record. Public so tests can drive reaping without waiting for the timer.
    pub fn reap_expired(api: &Arc<ServerAPI>) {
        let mut reaped: Vec<(Identifier, Arc<StoredTask>)> = Vec::new();
        api.leased_tasks.tasks.retain(|task_type, tasks| {
            tasks.retain(|task| {
                if task.expired() {
                    reaped.push((task_type.clone(), task.stored_task.clone()));
                    false
                } else {
                    true
                }
            });
            !tasks.is_empty()
        });
        // Requeue outside the leased_tasks locks.
        for (task_type, stored_task) in reaped {
            if let Some(queue) = api.task_queue.tasks.get(&task_type)
                && queue.0.try_send((*stored_task).clone()).is_err()
            {
                // Channel full/closed: drop back to "persisted only" and
                // signal the loader to rescan for the record.
                queue.2.remove(&stored_task.task_id);
                queue.3.store(true, Ordering::Release);
            }
        }
    }
}
