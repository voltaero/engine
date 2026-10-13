use crate::error::Error;
use crate::nucleus::query::prefix_successor;
use crate::task::Task;
use crate::{Identifier, Registry, config::Config, event::EventBus, plugin::LibraryManager};
use chrono::{DateTime, TimeDelta, Utc};
use dashmap::{DashMap, DashSet};
pub use postcard;
pub use postcard::from_bytes;
pub use postcard::to_allocvec;
use rust_rocksdb::{IteratorMode, ReadOptions};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
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
    pub load_state: LoadStates,            // RW
}

impl Default for ServerAPI {
    fn default() -> Self {
        Self::open("./engine_db", Config::new())
    }
}
impl ServerAPI {
    /// Builds a `ServerAPI` over the RocksDB store at `path` with an explicit
    /// config. `default()` is `open("./engine_db", Config::new())`; this exists so
    /// tests and embedders can point at their own store.
    pub fn open(path: impl AsRef<Path>, cfg: Config) -> Self {
        let mut opts = rust_rocksdb::Options::default();
        opts.create_if_missing(true);
        // Every complete() leaves a tombstone in the pending (`t:`) range, which
        // refill scans then have to step over. Mark SSTs that are dense with
        // deletions for compaction early so those tombstones are dropped quickly.
        opts.add_compact_on_deletion_collector_factory(10_000, 5_000, 0.5);

        let db = Arc::new(
            rust_rocksdb::DB::open(&opts, path)
                .expect("Failed to open RocksDB — check that the path is writable and no other process holds the lock"),
        );

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
            cfg,
            event_bus: EventBus::default(),
            lib_manager: LibraryManager::default(),
            db,
            task_registry: EngineTaskRegistry::default(),
            task_queue: TaskQueue::default(),
            leased_tasks: LeasedTaskQueue::default(),
            load_state: LoadStates::default(),
        };
        LibraryManager::load_modules(&mut k);
        crate::event::register_inventory_handlers(&mut k);
        k
    }

    /// Loads pending records of `task_type` from RocksDB into its queue, as many
    /// as currently fit. Equivalent to [`ServerAPI::refill`]; kept for callers
    /// that drive loading by hand. Never awaits, so dropping the future can't
    /// strand a task that was marked in-flight but never sent.
    pub async fn load(api: &Arc<Self>, task_type: Identifier) -> Result<(), Error> {
        api.refill(&task_type).map(|_| ())
    }

    /// Tops up `task_type`'s queue from RocksDB, resuming from the per-type cursor
    /// (see [`LoadState`]). Enqueues at most the channel's free capacity and returns
    /// how many tasks were enqueued. Synchronous and non-blocking on the channel:
    /// it only uses `try_send`, so it is safe to call from any thread.
    pub fn refill(&self, task_type: &Identifier) -> Result<usize, Error> {
        let state = self.load_state.get(task_type);
        // The guard is held across the scan and the try_sends, but nothing here
        // awaits, so it can't park a shard lock.
        let queue = self
            .task_queue
            .tasks
            .get(task_type)
            .ok_or(Error::not_found("TaskTypeNotFound"))?;
        let free = match queue.0.capacity() {
            Some(capacity) => capacity.saturating_sub(queue.0.len()),
            None => DEFAULT_LOAD_BATCH,
        };

        let batch = self.scan_pending(task_type, &queue.2, &state, free)?;

        let mut enqueued = 0;
        let mut batch = batch.into_iter();
        while let Some(task) = batch.next() {
            if let Err(err) = queue.0.try_send(task) {
                // Full (a concurrent cancel took the slot) or closed. Un-mark
                // everything that never made it into the queue; the cursor has
                // already moved past those keys, so request a full rescan or a
                // later refill would never revisit them.
                queue.2.remove(&err.into_inner().task_id);
                for task in batch {
                    queue.2.remove(&task.task_id);
                }
                state.mark_rescan();
                break;
            }
            enqueued += 1;
        }
        Ok(enqueued)
    }

    /// One cursor pass over the pending records of `task_type`: collects up to
    /// `max` records whose task_id was not already in `dedup`, inserting each one
    /// (the caller must enqueue them or un-mark them).
    fn scan_pending(
        &self,
        task_type: &Identifier,
        dedup: &DashSet<String>,
        state: &LoadState,
        max: usize,
    ) -> Result<Vec<StoredTask>, Error> {
        if max == 0 {
            return Ok(Vec::new());
        }
        let prefix = task_key_prefix(task_type);

        // Serializes passes per type, so two refills never race the cursor.
        let mut cursor = state
            .cursor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.rescan.swap(false, Ordering::SeqCst) {
            *cursor = None;
        }
        let generation = state.generation.load(Ordering::SeqCst);
        if cursor.is_none() && state.idle_at.load(Ordering::SeqCst) == generation {
            // A full pass already reached the end and nothing that could make a
            // record loadable has happened since: skip the scan.
            return Ok(Vec::new());
        }
        let from_start = cursor.is_none();

        // Bound the iterator to this type's prefix and start it strictly after
        // the last key examined (appending 0x00 yields the key's immediate
        // successor), so RocksDB seeks straight there instead of walking every
        // in-flight record and tombstone in front of it.
        let mut opts = ReadOptions::default();
        let lower = match cursor.as_ref() {
            Some(after) => {
                let mut lower = after.clone();
                lower.push(0);
                lower
            }
            None => prefix.clone().into_bytes(),
        };
        opts.set_iterate_lower_bound(lower);
        if let Some(upper) = prefix_successor(prefix.as_bytes()) {
            opts.set_iterate_upper_bound(upper);
        }

        let mut batch = Vec::new();
        let mut last_key: Option<Box<[u8]>> = None;
        let mut reached_end = true;
        for result in self.db.iterator_opt(IteratorMode::Start, opts) {
            let (key, value) = match result {
                Ok(kv) => kv,
                Err(err) => {
                    // Keep the cursor at the last good key and retry next pass.
                    error!("RocksDB read error: {err}");
                    reached_end = false;
                    break;
                }
            };
            let task_id = std::str::from_utf8(&key)
                .ok()
                .and_then(task_id_from_key)
                .map(str::to_string);
            last_key = Some(key);
            let Some(task_id) = task_id else {
                continue;
            };
            if dedup.insert(task_id.clone()) {
                batch.push(StoredTask {
                    bytes: value.into(),
                    task_id,
                    task_type: task_type.clone(),
                });
                if batch.len() >= max {
                    reached_end = false;
                    break;
                }
            }
        }

        if reached_end {
            // Task ids are druid hex (nanosecond timestamp first), so keys are only
            // *roughly* time-ordered: a submit can land behind the cursor. Once a
            // pass reaches the end of the prefix, the next one starts over from
            // the prefix start so such keys are never skipped for good.
            *cursor = None;
            // If this pass itself covered the whole prefix, the store holds no
            // loadable record as of `generation`; refills can no-op until a
            // submit / reap / cancel overflow bumps it.
            let idle_at = if from_start { generation } else { NOT_IDLE };
            state.idle_at.store(idle_at, Ordering::SeqCst);
        } else {
            if let Some(key) = last_key {
                *cursor = Some(key.into_vec());
            }
            state.idle_at.store(NOT_IDLE, Ordering::SeqCst);
        }
        Ok(batch)
    }

    /// Asks for an asynchronous refill of `task_type`. At most one refill runs
    /// per type: if one is already running the request is folded into it (it
    /// re-runs once more before releasing the slot). Runs on tokio's blocking
    /// pool when called inside a runtime, inline on the caller's thread otherwise
    /// (e.g. the reaper thread). Never awaits under a DashMap guard.
    pub fn request_refill(api: &Arc<Self>, task_type: &Identifier) {
        let state = api.load_state.get(task_type);
        if state.is_idle() {
            return;
        }
        state.refill_requested.store(true, Ordering::SeqCst);
        let Some(slot) = RefillSlot::acquire(&state) else {
            return;
        };
        let api = api.clone();
        let task_type = task_type.clone();
        let run = move || Self::run_refills(&api, &task_type, slot);
        match tokio::runtime::Handle::try_current() {
            // If the runtime is shutting down the closure is dropped unrun, and
            // dropping `slot` releases the refill flag.
            Ok(handle) => drop(handle.spawn_blocking(run)),
            Err(_) => run(),
        }
    }

    fn run_refills(api: &Arc<Self>, task_type: &Identifier, mut slot: RefillSlot) {
        loop {
            let state = slot.0.clone();
            state.refill_requested.store(false, Ordering::SeqCst);
            if let Err(err) = api.refill(task_type) {
                error!("Refill of {}.{} failed: {err}", task_type.0, task_type.1);
            }
            drop(slot);
            // A request that raced the pass above saw the slot taken and returned;
            // pick it up here instead of losing it.
            if !state.refill_requested.load(Ordering::SeqCst) {
                return;
            }
            match RefillSlot::acquire(&state) {
                Some(next) => slot = next,
                None => return,
            }
        }
    }

    /// Whether a queue holding `len` tasks out of `capacity` is below the refill
    /// low watermark (a quarter of capacity).
    pub fn below_watermark(len: usize, capacity: Option<usize>) -> bool {
        capacity.is_some_and(|capacity| len.saturating_mul(4) < capacity)
    }

    /// The lease TTL from config.
    pub fn lease_ttl(&self) -> TimeDelta {
        let secs = i64::try_from(self.cfg.config_toml.lease_ttl_secs).unwrap_or(i64::MAX);
        TimeDelta::try_seconds(secs).unwrap_or(TimeDelta::MAX)
    }

    /// When a lease granted or renewed at `now` expires.
    pub fn lease_expiry(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        now.checked_add_signed(self.lease_ttl())
            .unwrap_or(DateTime::<Utc>::MAX_UTC)
    }

    pub fn populate(api: &Arc<Self>) {
        let capacity = api.cfg.config_toml.queue_capacity.max(1);
        api.task_registry.tasks.iter().for_each(|f| {
            let key = f.key();
            let (tx, rx) = async_channel::bounded(capacity);
            api.task_queue
                .tasks
                .entry(key.clone())
                .or_insert((tx, rx, DashSet::default()));
            api.leased_tasks.tasks.entry(key.clone()).or_default();
            api.load_state.tasks.entry(key.clone()).or_default();
            // task reg should be populated by mods
        });
    }
    pub fn init() -> Arc<Self> {
        let api = Arc::new(Self::default());
        Self::populate(&api);
        Self::spawn_reaper(&api);
        api
    }

    /// Starts the lease reaper thread, sweeping every `reap_interval_secs`. It
    /// holds only a weak reference and exits once the API is dropped.
    pub fn spawn_reaper(api: &Arc<Self>) {
        let interval = Duration::from_secs(api.cfg.config_toml.reap_interval_secs.max(1));
        let weak = Arc::downgrade(api);
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(interval);
                let Some(api) = weak.upgrade() else {
                    return;
                };
                LeasedTaskQueue::reap_expired(&api);
            }
        });
    }
}

/// Refill batch size for an unbounded queue (all engine queues are bounded).
const DEFAULT_LOAD_BATCH: usize = 4096;
/// `LoadState::idle_at` sentinel: no full pass has proven the store drained.
const NOT_IDLE: u64 = u64::MAX;

/// Per-type state of the cursor-based refill path.
///
/// Refills resume from `cursor` (the last pending key examined) instead of the
/// prefix start, so a deep backlog is walked once rather than on every refill.
/// Because keys are only roughly ordered and the cursor only moves forward, the
/// type falls back to a full rescan from the prefix start whenever a record may
/// have become loadable behind the cursor:
/// - a cursor pass reached the end of the prefix (covers submits whose druid
///   key sorts before the cursor);
/// - a task's dedup entry was cleared with its record still persisted — lease
///   reaping, `cancel` channel overflow, a failed `complete` write, or a refill
///   whose `try_send` failed.
///
/// `generation` is bumped by every such event and by `submit`; `idle_at` records
/// the generation at which a full pass found nothing more to load, letting
/// refills no-op while nothing has changed. Pending records must therefore only
/// be written through `submit` (there is intentionally no raw Put); anything
/// else writing `t:` keys must call [`LoadState::mark_rescan`].
#[derive(Debug)]
pub struct LoadState {
    cursor: Mutex<Option<Vec<u8>>>,
    rescan: AtomicBool,
    generation: AtomicU64,
    idle_at: AtomicU64,
    refilling: AtomicBool,
    refill_requested: AtomicBool,
}
impl Default for LoadState {
    fn default() -> Self {
        Self {
            cursor: Mutex::new(None),
            rescan: AtomicBool::new(false),
            generation: AtomicU64::new(0),
            idle_at: AtomicU64::new(NOT_IDLE),
            refilling: AtomicBool::new(false),
            refill_requested: AtomicBool::new(false),
        }
    }
}
impl LoadState {
    /// Forces the next refill pass to start over from the prefix start. Call
    /// *after* clearing the dedup entries of the affected tasks.
    pub fn mark_rescan(&self) {
        self.rescan.store(true, Ordering::SeqCst);
        self.generation.fetch_add(1, Ordering::SeqCst);
    }

    /// Records that a new pending record was written (by `submit`).
    pub fn note_submitted(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
    }

    fn is_idle(&self) -> bool {
        self.idle_at.load(Ordering::SeqCst) == self.generation.load(Ordering::SeqCst)
    }
}

/// Exclusive right to run a refill for one type; released on drop (including
/// on panic or when a spawned refill is dropped unrun).
struct RefillSlot(Arc<LoadState>);
impl RefillSlot {
    fn acquire(state: &Arc<LoadState>) -> Option<Self> {
        state
            .refilling
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .ok()
            .map(|_| Self(state.clone()))
    }
}
impl Drop for RefillSlot {
    fn drop(&mut self) {
        self.0.refilling.store(false, Ordering::SeqCst);
    }
}

#[derive(Debug, Default)]
pub struct LoadStates {
    pub tasks: DashMap<Identifier, Arc<LoadState>>,
}
impl LoadStates {
    /// The load state of `task_type`, created on first use. The `Arc` is cloned
    /// out so no map guard outlives the call.
    pub fn get(&self, task_type: &Identifier) -> Arc<LoadState> {
        if let Some(state) = self.tasks.get(task_type) {
            return state.clone();
        }
        self.tasks.entry(task_type.clone()).or_default().clone()
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
/// Leases per task type, keyed by task_id so renew/complete/cancel are O(1).
/// The inner maps are behind an `Arc` so callers clone them out and never hold
/// an outer-map guard while working on a type's leases.
#[derive(Debug, Default, Clone)]
pub struct LeasedTaskQueue {
    pub tasks: DashMap<Identifier, Arc<DashMap<String, LeasedTask>>>,
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
    /// Fencing token of this lease; see [`Lease::token`].
    pub token: u64,
    pub given_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

/// A task handed to a worker by `lease`.
#[derive(Debug, Clone)]
pub struct Lease {
    pub task: Arc<StoredTask>,
    /// Fencing token: `renew`/`complete`/`cancel` must present it. Unique per
    /// lease, so a worker whose lease was reaped can't act on the task after it
    /// is re-leased to someone else. Not a secret — authenticate workers
    /// separately.
    pub token: u64,
    pub expires_at: DateTime<Utc>,
}

static NEXT_LEASE_TOKEN: AtomicU64 = AtomicU64::new(1);

/// A process-unique lease token.
pub fn next_lease_token() -> u64 {
    NEXT_LEASE_TOKEN.fetch_add(1, Ordering::Relaxed)
}

impl LeasedTask {
    fn expired(&self, now: DateTime<Utc>) -> bool {
        self.expires_at <= now
    }
}

impl LeasedTaskQueue {
    /// The lease map of `task_type`, if the type is known.
    pub fn leases(&self, task_type: &Identifier) -> Option<Arc<DashMap<String, LeasedTask>>> {
        self.tasks.get(task_type).map(|leases| leases.clone())
    }

    /// The lease map of `task_type`, created on first use.
    pub fn leases_or_default(&self, task_type: &Identifier) -> Arc<DashMap<String, LeasedTask>> {
        if let Some(leases) = self.leases(task_type) {
            return leases;
        }
        self.tasks.entry(task_type.clone()).or_default().clone()
    }

    /// Drops every expired lease, clears the reaped ids from the dedup set and
    /// asks for a refill so the still-persisted records are re-enqueued.
    pub fn reap_expired(api: &Arc<ServerAPI>) {
        let now = Utc::now();
        // Snapshot the per-type maps so no outer-map guard is held while reaping.
        let types: Vec<_> = api
            .leased_tasks
            .tasks
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        for (task_type, leases) in types {
            let mut reaped: Vec<String> = Vec::new();
            leases.retain(|task_id, lease| {
                if lease.expired(now) {
                    reaped.push(task_id.clone());
                    false
                } else {
                    true
                }
            });
            if reaped.is_empty() {
                continue;
            }
            // Clear reaped ids from the dedup set outside the lease locks so the
            // refill path can re-enqueue the tasks from their still-persisted
            // records. The cursor may be past them, so force a full rescan.
            if let Some(queue) = api.task_queue.tasks.get(&task_type) {
                for task_id in &reaped {
                    queue.2.remove(task_id);
                }
            }
            api.load_state.get(&task_type).mark_rescan();
            ServerAPI::request_refill(api, &task_type);
        }
    }
}
