use crate::chunk::{Bitmap, ChunkPayloads, FinishedChunk, MAX_CHUNK_SIZE, SeqAllocator, TaskId};
use crate::config::TaskTypeConfig;
use crate::error::Error;
use crate::task::{Task, TaskLimits};
use crate::{Identifier, Registry, config::Config, event::EventBus, plugin::LibraryManager};
use chrono::{DateTime, TimeDelta, Utc};
use dashmap::{DashMap, DashSet};
pub use postcard;
pub use postcard::from_bytes;
pub use postcard::to_allocvec;
pub use rust_rocksdb;
use rust_rocksdb::{
    BlockBasedOptions, ColumnFamily, ColumnFamilyDescriptor, IteratorMode, Options, ReadOptions,
};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tracing::{Level, debug, error, instrument, warn};

/// Column family holding pending chunk records (`t:` keys). Delete-heavy.
pub const CF_PENDING: &str = "pending";
/// Column family holding finished chunk records (`f:` keys). Append-mostly.
pub const CF_FINISHED: &str = "finished";

/// Engine default for [`TaskSettings::chunk_size`].
pub const DEFAULT_CHUNK_SIZE: u32 = 1024;

// t:namespace:task_name:<chunk_seq 16 hex> -> pending chunk record (CF_PENDING)
pub fn task_key_prefix(task_type: &Identifier) -> String {
    format!("t:{}:{}:", task_type.0, task_type.1)
}

pub fn task_key(task_type: &Identifier, chunk_seq: u64) -> String {
    format!(
        "t:{}:{}:{}",
        task_type.0,
        task_type.1,
        crate::chunk::seq_hex(chunk_seq)
    )
}

// f:namespace:task_name:<chunk_seq 16 hex> -> finished chunk record (CF_FINISHED)
pub fn finished_task_key(task_type: &Identifier, chunk_seq: u64) -> String {
    format!(
        "f:{}:{}:{}",
        task_type.0,
        task_type.1,
        crate::chunk::seq_hex(chunk_seq)
    )
}

pub fn finished_task_key_prefix(task_type: &Identifier) -> String {
    format!("f:{}:{}:", task_type.0, task_type.1)
}

/// The `chunk_seq` of a `t:`/`f:` record key.
pub fn chunk_seq_from_key(key: &[u8]) -> Option<u64> {
    let key = std::str::from_utf8(key).ok()?;
    crate::chunk::seq_from_hex(key.rsplit_once(':')?.1)
}

/// Effective per-type settings, resolved once per type with precedence
/// `config.toml` `[tasks."ns:name"]` > mod (`Task::limits`) > engine default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskSettings {
    /// Tasks per chunk (storage and lease unit), 1..=65536. For a derived type,
    /// task indexes per chunk, 1..=u32::MAX.
    pub chunk_size: u32,
    /// Largest accepted payload in bytes; `None` = unlimited.
    pub max_payload: Option<u32>,
    /// Largest accepted result in bytes; `None` = unlimited.
    pub max_result: Option<u32>,
    /// Derived type (see [`TaskLimits::derived`]): every chunk is one task slot
    /// holding a base payload and an index range.
    pub derived: bool,
}

impl TaskSettings {
    pub fn resolve(config: Option<&TaskTypeConfig>, limits: TaskLimits) -> Self {
        let config = config.copied().unwrap_or_default();
        let chunk_size = config
            .chunk_size
            .or(limits.chunk_size)
            .unwrap_or(DEFAULT_CHUNK_SIZE);
        let chunk_size = if limits.derived {
            chunk_size.max(1)
        } else {
            chunk_size.clamp(1, MAX_CHUNK_SIZE)
        };
        Self {
            chunk_size,
            max_payload: config.max_payload.or(limits.max_payload),
            max_result: config.max_result.or(limits.max_result),
            derived: limits.derived,
        }
    }

    /// Task slots per stored chunk: `chunk_size`, or 1 for a derived type.
    pub fn slots_per_chunk(&self) -> usize {
        if self.derived {
            1
        } else {
            self.chunk_size as usize
        }
    }
}

/// Installs the global tracing subscriber (DEBUG in debug builds, INFO in
/// release). Idempotent; `ServerAPI::open` calls it.
pub fn init_tracing() {
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

pub struct ServerAPI {
    pub cfg: Config,                 // RW
    pub event_bus: EventBus,         // RW
    pub lib_manager: LibraryManager, // RW
    pub db: Arc<rust_rocksdb::DB>,
    pub task_queue: TaskQueue,             // RW
    pub leased_tasks: LeasedTaskQueue,     // RW
    pub task_registry: EngineTaskRegistry, // RW
    pub load_state: LoadStates,            // RW
    /// Server-wide, persisted `chunk_seq` allocator.
    pub chunk_seq: SeqAllocator,
    /// Resolved per-type settings, filled by [`ServerAPI::populate`].
    pub settings: DashMap<Identifier, TaskSettings>,
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
        let parallelism = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.increase_parallelism(parallelism.min(i32::MAX as usize) as i32);

        let mut table = BlockBasedOptions::default();
        table.set_bloom_filter(10.0, false);

        // Pending chunks are written once and deleted when the chunk is fully
        // done: mark SSTs dense with tombstones for early compaction so refill
        // scans don't step over them for long.
        let mut pending = Options::default();
        pending.set_block_based_table_factory(&table);
        pending.add_compact_on_deletion_collector_factory(10_000, 5_000, 0.5);

        // Finished chunks are append-mostly and large: keep the values in blob
        // files so compaction only rewrites keys. The bloom filter makes the
        // refill's "any results yet?" probe of a fresh chunk cheap.
        let mut finished = Options::default();
        finished.set_block_based_table_factory(&table);
        finished.set_enable_blob_files(true);
        finished.set_min_blob_size(4096);
        finished.set_enable_blob_gc(true);

        let db = Arc::new(
            rust_rocksdb::DB::open_cf_descriptors(
                &opts,
                path,
                [
                    ColumnFamilyDescriptor::new(CF_PENDING, pending),
                    ColumnFamilyDescriptor::new(CF_FINISHED, finished),
                ],
            )
            .expect("Failed to open RocksDB — check that the path is writable and no other process holds the lock"),
        );
        let chunk_seq = SeqAllocator::open(&db).expect("Failed to read the chunk_seq meta record");

        init_tracing();

        let mut k = Self {
            cfg,
            event_bus: EventBus::default(),
            lib_manager: LibraryManager::default(),
            db,
            task_registry: EngineTaskRegistry::default(),
            task_queue: TaskQueue::default(),
            leased_tasks: LeasedTaskQueue::default(),
            load_state: LoadStates::default(),
            chunk_seq,
            settings: DashMap::new(),
        };
        LibraryManager::load_modules(&mut k);
        crate::event::register_inventory_handlers(&mut k);
        k
    }

    /// The pending-chunk column family (`t:` keys).
    pub fn cf_pending(&self) -> &ColumnFamily {
        self.db
            .cf_handle(CF_PENDING)
            .expect("pending column family is created at open")
    }

    /// The finished-chunk column family (`f:` keys).
    pub fn cf_finished(&self) -> &ColumnFamily {
        self.db
            .cf_handle(CF_FINISHED)
            .expect("finished column family is created at open")
    }

    /// Column family a raw key (or key prefix) lives in: `t:` pending, `f:`
    /// finished, anything else the default family (`None`).
    pub fn cf_for_key(&self, key: &[u8]) -> Option<&ColumnFamily> {
        if key.starts_with(b"t:") {
            Some(self.cf_pending())
        } else if key.starts_with(b"f:") {
            Some(self.cf_finished())
        } else {
            None
        }
    }

    pub fn server_id(&self) -> u16 {
        self.cfg.config_toml.server_id
    }

    /// The wire task id of task `idx` in chunk `chunk_seq` on this server.
    pub fn task_id(&self, chunk_seq: u64, idx: u16) -> String {
        TaskId::new(self.server_id(), chunk_seq, idx).to_string()
    }

    /// Parses a wire task id owned by this server. Malformed -> InvalidArgument
    /// `"InvalidTaskId"`; another server's id -> NotFound `"WrongServer"`.
    pub fn parse_task_id(&self, task_id: &str) -> Result<TaskId, Error> {
        let id = TaskId::parse(task_id)?;
        if id.server_id != self.server_id() {
            return Err(Error::not_found(format!("WrongServer: {task_id}")));
        }
        Ok(id)
    }

    /// Effective settings of `task_type` (resolved on first use if `populate`
    /// hasn't seen the type).
    pub fn settings(&self, task_type: &Identifier) -> TaskSettings {
        if let Some(settings) = self.settings.get(task_type) {
            return *settings;
        }
        let settings = self.resolve_settings(task_type);
        self.settings.insert(task_type.clone(), settings);
        settings
    }

    fn resolve_settings(&self, task_type: &Identifier) -> TaskSettings {
        let config = self
            .cfg
            .config_toml
            .tasks
            .get(&format!("{}:{}", task_type.0, task_type.1));
        let limits = self
            .task_registry
            .tasks
            .get(task_type)
            .map(|task| task.limits())
            .unwrap_or_default();
        TaskSettings::resolve(config, limits)
    }

    /// Reads the pending record of a chunk.
    pub fn read_pending(
        &self,
        task_type: &Identifier,
        chunk_seq: u64,
    ) -> Result<Option<ChunkPayloads>, Error> {
        let data = self
            .db
            .get_cf(self.cf_pending(), task_key(task_type, chunk_seq))
            .map_err(|err| Error::io_error(format!("Failed to read pending chunk: {err}")))?;
        data.map(ChunkPayloads::decode).transpose()
    }

    /// Reads and decodes the finished record of a chunk.
    pub fn read_finished(
        &self,
        task_type: &Identifier,
        chunk_seq: u64,
    ) -> Result<Option<FinishedChunk>, Error> {
        let data = self
            .db
            .get_pinned_cf(self.cf_finished(), finished_task_key(task_type, chunk_seq))
            .map_err(|err| Error::io_error(format!("Failed to read finished chunk: {err}")))?;
        data.map(|data| FinishedChunk::decode(&data)).transpose()
    }

    /// Builds the queue entry of a pending chunk from its raw `t:` record:
    /// every index without a result yet. `None` when the chunk has nothing left
    /// to do.
    fn load_chunk(
        &self,
        task_type: &Identifier,
        chunk_seq: u64,
        pending: Vec<u8>,
    ) -> Result<Option<QueuedChunk>, LoadError> {
        let payloads = ChunkPayloads::decode(pending).map_err(LoadError::Corrupt)?;
        let finished = self
            .db
            .get_pinned_cf(self.cf_finished(), finished_task_key(task_type, chunk_seq))
            .map_err(|err| {
                LoadError::Read(Error::io_error(format!(
                    "Failed to read finished chunk: {err}"
                )))
            })?;
        let mut open = Bitmap::full(payloads.len());
        let has_finished = finished.is_some();
        if let Some(finished) = finished {
            let done = FinishedChunk::decode_done(&finished).map_err(LoadError::Corrupt)?;
            for idx in done.iter() {
                open.clear(idx);
            }
        }
        if open.none() {
            return Ok(None);
        }
        Ok(Some(QueuedChunk {
            chunk_seq,
            payloads: Arc::new(payloads),
            open,
            has_finished,
        }))
    }

    /// Loads pending chunks of `task_type` from RocksDB into its queue, as many
    /// as currently fit. Equivalent to [`ServerAPI::refill`]; kept for callers
    /// that drive loading by hand. Never awaits, so dropping the future can't
    /// strand a chunk that was marked in-flight but never sent.
    pub async fn load(api: &Arc<Self>, task_type: Identifier) -> Result<(), Error> {
        api.refill(&task_type).map(|_| ())
    }

    /// Tops up `task_type`'s queue from RocksDB: first the recovery set
    /// (reaped / cancelled chunks that didn't fit back into the queue), then the
    /// forward cursor over new chunks. Enqueues at most the channel's free
    /// capacity and returns how many chunks were enqueued. Synchronous and
    /// non-blocking on the channel (`try_send` only), so it is safe to call from
    /// any thread.
    pub fn refill(&self, task_type: &Identifier) -> Result<usize, Error> {
        let state = self.load_state.get(task_type);
        let queue = self
            .task_queue
            .get(task_type)
            .ok_or(Error::not_found("TaskTypeNotFound"))?;
        let free = queue.free_chunks();
        let batch = self.collect_loadable(task_type, &queue, &state, free)?;

        let mut enqueued = 0;
        let mut batch = batch.into_iter();
        while let Some(chunk) = batch.next() {
            if let Err(chunk) = queue.try_push(chunk) {
                // Full (a concurrent cancel took the slot) or closed. Un-mark
                // everything that never made it into the queue and hand it to
                // the recovery set: the cursor is already past those keys.
                for chunk in std::iter::once(chunk).chain(batch) {
                    queue.dedup.remove(&chunk.chunk_seq);
                    state.recover(chunk.chunk_seq);
                }
                break;
            }
            enqueued += 1;
        }
        Ok(enqueued)
    }

    /// One refill pass: collects up to `max` loadable chunks of `task_type`,
    /// inserting each into `queue.dedup` (the caller must enqueue them or
    /// un-mark them).
    fn collect_loadable(
        &self,
        task_type: &Identifier,
        queue: &TypeQueue,
        state: &LoadState,
        max: usize,
    ) -> Result<Vec<QueuedChunk>, Error> {
        if max == 0 {
            return Ok(Vec::new());
        }
        // Serializes passes per type, so two refills never race the cursor.
        let mut cursor = state
            .cursor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.rescan.swap(false, Ordering::SeqCst) {
            *cursor = None;
        }
        let generation = state.generation.load(Ordering::SeqCst);
        if state.idle_at.load(Ordering::SeqCst) == generation {
            // A pass already reached the end with nothing to recover, and no
            // submit / requeue overflow has happened since.
            return Ok(Vec::new());
        }

        let mut batch = Vec::new();

        // 1. Chunks whose dedup entry was cleared while their record stayed
        //    persisted. Point reads, independent of the cursor.
        let recovered: Vec<u64> = {
            let mut set = state.recover_set();
            std::iter::from_fn(|| set.pop_first()).take(max).collect()
        };
        let mut recovered = recovered.into_iter();
        while let Some(chunk_seq) = recovered.next() {
            if !queue.dedup.insert(chunk_seq) {
                continue; // already queued or leased
            }
            let loaded = self
                .db
                .get_cf(self.cf_pending(), task_key(task_type, chunk_seq))
                .map_err(|err| {
                    LoadError::Read(Error::io_error(format!(
                        "Failed to read pending chunk: {err}"
                    )))
                })
                .and_then(|pending| {
                    pending
                        .map(|pending| self.load_chunk(task_type, chunk_seq, pending))
                        .transpose()
                        .map(Option::flatten)
                });
            match loaded {
                Ok(Some(chunk)) => batch.push(chunk),
                // Completed or deleted meanwhile: nothing to load.
                Ok(None) => {
                    queue.dedup.remove(&chunk_seq);
                }
                Err(LoadError::Corrupt(err)) => {
                    // Retrying can't help; a restart re-reads it.
                    error!("Skipping corrupt chunk {chunk_seq}: {err}");
                    queue.dedup.remove(&chunk_seq);
                }
                Err(LoadError::Read(err)) => {
                    error!("Failed to recover chunk {chunk_seq}: {err}");
                    queue.dedup.remove(&chunk_seq);
                    for chunk_seq in std::iter::once(chunk_seq).chain(recovered) {
                        state.recover(chunk_seq);
                    }
                    state.idle_at.store(NOT_IDLE, Ordering::SeqCst);
                    return Ok(batch);
                }
            }
        }
        if batch.len() >= max {
            state.idle_at.store(NOT_IDLE, Ordering::SeqCst);
            return Ok(batch);
        }

        // 2. Forward cursor pass over new chunks. Chunk keys are strictly
        //    increasing and the pass stops below `safe_upto` (the oldest chunk
        //    whose write is still in progress), so no key can ever appear
        //    behind the cursor: it never has to rescan from the start.
        //
        //    Flag the type as capped *before* reading the bounds: a submit that
        //    releases its reservation after this read sees the flag and wakes
        //    the type (`wake_capped`); one that released before is already
        //    reflected in `safe_upto`.
        state.capped.store(true, Ordering::SeqCst);
        let (safe_upto, next) = self.chunk_seq.bounds();
        let prefix = task_key_prefix(task_type);
        let upper = task_key(task_type, safe_upto).into_bytes();
        let lower = match cursor.as_ref() {
            Some(after) => {
                // Appending 0x00 yields the key's immediate successor.
                let mut lower = after.clone();
                lower.push(0);
                lower
            }
            None => prefix.into_bytes(),
        };
        let mut reached_end = true;
        if lower < upper {
            let mut opts = ReadOptions::default();
            opts.set_iterate_lower_bound(lower);
            opts.set_iterate_upper_bound(upper);
            let mut last_key: Option<Box<[u8]>> = None;
            for result in self
                .db
                .iterator_cf_opt(self.cf_pending(), opts, IteratorMode::Start)
            {
                let (key, value) = match result {
                    Ok(kv) => kv,
                    Err(err) => {
                        // Keep the cursor at the last good key and retry next pass.
                        error!("RocksDB read error: {err}");
                        reached_end = false;
                        break;
                    }
                };
                let chunk_seq = chunk_seq_from_key(&key);
                last_key = Some(key);
                let Some(chunk_seq) = chunk_seq else {
                    continue;
                };
                if !queue.dedup.insert(chunk_seq) {
                    continue;
                }
                match self.load_chunk(task_type, chunk_seq, value.into_vec()) {
                    Ok(Some(chunk)) => batch.push(chunk),
                    Ok(None) => {
                        warn!("Pending chunk {chunk_seq} has no open tasks; skipping");
                        queue.dedup.remove(&chunk_seq);
                    }
                    Err(LoadError::Corrupt(err)) => {
                        // Retrying can't help; a restart re-reads it.
                        error!("Skipping corrupt chunk {chunk_seq}: {err}");
                        queue.dedup.remove(&chunk_seq);
                    }
                    Err(LoadError::Read(err)) => {
                        // The cursor moves past this chunk: hand it to the
                        // recovery set so a later pass retries it.
                        error!("Failed to load chunk {chunk_seq}, will retry: {err}");
                        queue.dedup.remove(&chunk_seq);
                        state.recover(chunk_seq);
                    }
                }
                if batch.len() >= max {
                    reached_end = false;
                    break;
                }
            }
            if let Some(key) = last_key {
                *cursor = Some(key.into_vec());
            }
        }

        // Stopped at `safe_upto` with seqs above it already handed out: this
        // type may have chunks written up there that the pass couldn't see.
        // Stay non-idle and keep `capped` set so the submit that releases the
        // blocking reservation wakes this type.
        let capped = reached_end && safe_upto < next;
        if !capped {
            state.capped.store(false, Ordering::SeqCst);
        }
        let idle_at = if reached_end && !capped && state.recover_set().is_empty() {
            generation
        } else {
            NOT_IDLE
        };
        state.idle_at.store(idle_at, Ordering::SeqCst);
        Ok(batch)
    }

    /// Requests a refill of every type whose last pass stopped at `safe_upto`
    /// with seqs above it already handed out. Call after releasing a
    /// [`crate::chunk::SeqReservation`] (`submit_batch` does): the release may
    /// have advanced `safe_upto` past chunks another type already wrote.
    pub fn wake_capped(api: &Arc<Self>) {
        let capped: Vec<Identifier> = api
            .load_state
            .tasks
            .iter()
            .filter(|state| state.capped.swap(false, Ordering::SeqCst))
            .map(|state| state.key().clone())
            .collect();
        for task_type in capped {
            Self::request_refill(api, &task_type);
        }
    }

    /// Puts the still-open tasks of a chunk whose lease ended (cancel, reap)
    /// back into the queue. The chunk stays in the dedup set throughout; if the
    /// queue is full or closed it is un-marked and handed to the recovery set
    /// instead, so a later refill re-reads it. Never strands a chunk.
    pub fn requeue(api: &Arc<Self>, task_type: &Identifier, chunk: QueuedChunk) {
        let Some(queue) = api.task_queue.get(task_type) else {
            return;
        };
        if let Err(chunk) = queue.try_push(chunk) {
            queue.dedup.remove(&chunk.chunk_seq);
            api.load_state.get(task_type).recover(chunk.chunk_seq);
            Self::request_refill(api, task_type);
        }
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

    /// Whether a queue holding `len` chunks out of `capacity` is below the
    /// refill low watermark (a quarter of capacity).
    pub fn below_watermark(len: usize, capacity: Option<usize>) -> bool {
        capacity.is_some_and(|capacity| len.saturating_mul(4) < capacity)
    }

    /// Requests a refill if `task_type`'s queue is below the low watermark.
    pub fn refill_if_low(api: &Arc<Self>, task_type: &Identifier) {
        let low = api
            .task_queue
            .get(task_type)
            .is_some_and(|queue| Self::below_watermark(queue.rx.len(), queue.tx.capacity()));
        if low {
            Self::request_refill(api, task_type);
        }
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

    /// Creates the per-type queues, lease maps, load states and settings of
    /// every registered task type. Call after mods have registered their types.
    pub fn populate(api: &Arc<Self>) {
        let types: Vec<Identifier> = api
            .task_registry
            .tasks
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        for key in types {
            let settings = api.resolve_settings(&key);
            api.settings.insert(key.clone(), settings);
            let capacity = api
                .cfg
                .config_toml
                .queue_capacity
                .div_ceil(settings.slots_per_chunk())
                .max(1);
            api.task_queue
                .tasks
                .entry(key.clone())
                .or_insert_with(|| Arc::new(TypeQueue::new(capacity)));
            api.leased_tasks.tasks.entry(key.clone()).or_default();
            api.load_state.tasks.entry(key).or_default();
        }
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

/// `LoadState::idle_at` sentinel: no pass has proven the store drained.
const NOT_IDLE: u64 = u64::MAX;

/// Why a chunk couldn't be loaded. A failed read may succeed on a retry, so
/// the chunk goes to the recovery set; a corrupt record never will, so it is
/// skipped (and logged) until a restart re-reads it.
enum LoadError {
    Read(Error),
    Corrupt(Error),
}

/// Per-type state of the refill path.
///
/// - `cursor`: the last pending key examined. Chunk keys are strictly
///   increasing and the pass is bounded by [`SeqAllocator::safe_upto`], so every
///   pending chunk at or below the cursor has been loaded at least once; the
///   cursor only moves forward and passes never restart from the prefix start.
/// - `recover`: chunks whose dedup entry was cleared while their record stayed
///   persisted (a requeue that found the queue full, a failed refill
///   `try_send`). Refill drains it with point reads before the cursor pass.
///   Anything that clears a dedup entry of a still-pending chunk must call
///   [`LoadState::recover`].
/// - `generation` is bumped by `submit` and by `recover`; `idle_at` records the
///   generation at which a pass found nothing more to load, letting refills
///   no-op while nothing has changed.
/// - `capped`: the last pass stopped at `safe_upto` while higher seqs were
///   already handed out (another insert still being written), so this type may
///   have chunks the pass couldn't see. Such a pass never marks the type idle,
///   and [`ServerAPI::wake_capped`] refills it once the reservation is released.
///
/// Pending records must only be written through `submit` (there is
/// intentionally no raw Put); out-of-band writers must call
/// [`LoadState::mark_rescan`].
#[derive(Debug)]
pub struct LoadState {
    cursor: Mutex<Option<Vec<u8>>>,
    rescan: AtomicBool,
    recover: Mutex<BTreeSet<u64>>,
    generation: AtomicU64,
    idle_at: AtomicU64,
    refilling: AtomicBool,
    refill_requested: AtomicBool,
    capped: AtomicBool,
}
impl Default for LoadState {
    fn default() -> Self {
        Self {
            cursor: Mutex::new(None),
            rescan: AtomicBool::new(false),
            recover: Mutex::new(BTreeSet::new()),
            generation: AtomicU64::new(0),
            idle_at: AtomicU64::new(NOT_IDLE),
            refilling: AtomicBool::new(false),
            refill_requested: AtomicBool::new(false),
            capped: AtomicBool::new(false),
        }
    }
}
impl LoadState {
    /// Forces the next refill pass to start over from the prefix start. Only
    /// needed after writing `t:` records outside `submit`.
    pub fn mark_rescan(&self) {
        self.rescan.store(true, Ordering::SeqCst);
        self.generation.fetch_add(1, Ordering::SeqCst);
    }

    /// Hands a still-persisted chunk to the next refill. Call *after* clearing
    /// its dedup entry.
    pub fn recover(&self, chunk_seq: u64) {
        self.recover_set().insert(chunk_seq);
        self.generation.fetch_add(1, Ordering::SeqCst);
    }

    /// Runs `f` while no refill pass can run for this type, so no chunk joins the
    /// dedup set (gets queued) until `f` returns: refill passes are the only
    /// place dedup entries are added, and they hold the same lock. Lets a caller
    /// check dedup membership and delete `t:`/`f:` records as one atomic step.
    pub fn exclusive<R>(&self, f: impl FnOnce() -> R) -> R {
        let _cursor = self
            .cursor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        f()
    }

    /// Records that new pending chunks were written (by `submit`).
    pub fn note_submitted(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
    }

    fn recover_set(&self) -> MutexGuard<'_, BTreeSet<u64>> {
        self.recover
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
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
    // Runs on every insert / complete / typed query: TRACE, so release builds
    // (INFO) don't build and format a span per request.
    #[instrument(level = "trace", skip_all, fields(namespace = %identifier.0, name = %identifier.1))]
    fn get(&self, identifier: &Identifier) -> Option<Box<dyn Task>> {
        self.tasks.get(identifier).map(|obj| obj.clone_box())
    }
}

/// A chunk waiting in a queue (or held by a lease): its payloads and the
/// indexes that still have no result.
#[derive(Debug, Clone)]
pub struct QueuedChunk {
    pub chunk_seq: u64,
    pub payloads: Arc<ChunkPayloads>,
    /// Indexes without a result. Never empty while queued or leased.
    pub open: Bitmap,
    /// Whether a finished (`f:`) record exists, i.e. completing must merge.
    pub has_finished: bool,
}

/// One task type's in-memory queue.
///
/// `dedup` holds the `chunk_seq` of every chunk that is in flight — queued in
/// the channel **or** leased — and nothing else.
#[derive(Debug)]
pub struct TypeQueue {
    pub tx: async_channel::Sender<QueuedChunk>,
    pub rx: async_channel::Receiver<QueuedChunk>,
    pub dedup: DashSet<u64>,
    /// Open tasks across the chunks in the channel.
    queued_tasks: AtomicUsize,
}

impl TypeQueue {
    pub fn new(capacity_chunks: usize) -> Self {
        let (tx, rx) = async_channel::bounded(capacity_chunks.max(1));
        Self {
            tx,
            rx,
            dedup: DashSet::new(),
            queued_tasks: AtomicUsize::new(0),
        }
    }

    /// Non-blocking send; hands the chunk back if the channel is full/closed.
    pub fn try_push(&self, chunk: QueuedChunk) -> Result<(), QueuedChunk> {
        // Count before sending so a concurrent receive never underflows.
        let tasks = chunk.open.count();
        self.queued_tasks.fetch_add(tasks, Ordering::Relaxed);
        self.tx.try_send(chunk).map_err(|err| {
            self.queued_tasks.fetch_sub(tasks, Ordering::Relaxed);
            err.into_inner()
        })
    }

    /// Bookkeeping for a chunk just received from `rx`.
    pub(crate) fn note_received(&self, chunk: &QueuedChunk) {
        self.queued_tasks
            .fetch_sub(chunk.open.count(), Ordering::Relaxed);
    }

    /// Open tasks currently queued.
    pub fn queued_tasks(&self) -> usize {
        self.queued_tasks.load(Ordering::Relaxed)
    }

    /// Free channel slots, in chunks.
    pub fn free_chunks(&self) -> usize {
        self.tx.capacity().map_or(DEFAULT_LOAD_BATCH, |capacity| {
            capacity.saturating_sub(self.tx.len())
        })
    }
}

/// Refill batch size for an unbounded queue (all engine queues are bounded).
const DEFAULT_LOAD_BATCH: usize = 64;

#[derive(Debug, Default, Clone)]
pub struct TaskQueue {
    pub tasks: DashMap<Identifier, Arc<TypeQueue>>,
}
impl TaskQueue {
    /// The queue of `task_type`, cloned out so no map guard is held.
    pub fn get(&self, task_type: &Identifier) -> Option<Arc<TypeQueue>> {
        self.tasks.get(task_type).map(|queue| queue.clone())
    }
}

/// A leased chunk. Shared behind a mutex so complete / renew / cancel / reap
/// of one chunk serialize; `gone` is set (under the mutex) when the lease is
/// dropped from its map, so a holder of a stale `Arc` sees it ended.
///
/// Lock order: never lock a `ChunkLease` while holding a guard of the lease
/// `DashMap` (clone the `Arc` out first). Removing from the map while holding
/// the mutex is fine.
#[derive(Debug)]
pub struct ChunkLease {
    /// The chunk; `open` is the indexes this lease has not completed yet.
    pub chunk: QueuedChunk,
    pub user_id: String,
    /// Fencing token of this lease; see [`Lease::token`].
    pub token: u64,
    pub given_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub gone: bool,
}

impl ChunkLease {
    fn expired(&self, now: DateTime<Utc>) -> bool {
        self.expires_at <= now
    }
}

pub type LeaseCell = Arc<Mutex<ChunkLease>>;

/// Locks a lease cell, ignoring poisoning (every mutation is a plain field
/// write, so the state stays consistent).
pub fn lock_lease(cell: &LeaseCell) -> MutexGuard<'_, ChunkLease> {
    cell.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Leases per task type, keyed by `chunk_seq`. The inner maps are behind an
/// `Arc` so callers clone them out and never hold an outer-map guard.
#[derive(Debug, Default, Clone)]
pub struct LeasedTaskQueue {
    pub tasks: DashMap<Identifier, Arc<DashMap<u64, LeaseCell>>>,
}

/// A chunk handed to a worker by `lease`: every open task of the chunk, all
/// under one fencing token.
#[derive(Debug, Clone)]
pub struct Lease {
    pub task_type: Identifier,
    pub server_id: u16,
    pub chunk_seq: u64,
    pub payloads: Arc<ChunkPayloads>,
    /// Indexes leased (the chunk's open tasks at lease time).
    pub open: Bitmap,
    /// Fencing token: `renew`/`complete`/`cancel` must present it. Unique per
    /// lease, so a worker whose lease was reaped can't act on the chunk after it
    /// is re-leased to someone else. Not a secret — authenticate workers
    /// separately.
    pub token: u64,
    pub expires_at: DateTime<Utc>,
}

impl Lease {
    /// Number of tasks in this lease.
    pub fn len(&self) -> usize {
        self.open.count()
    }

    pub fn is_empty(&self) -> bool {
        self.open.none()
    }

    /// The index range of a derived chunk lease (its one task's payload is the
    /// base); `None` for a regular chunk.
    pub fn range(&self) -> Option<crate::chunk::DerivedRange> {
        self.payloads.range()
    }

    /// `(task_id, payload)` of every leased task, in index order.
    pub fn tasks(&self) -> impl Iterator<Item = (String, &[u8])> + '_ {
        self.open.iter().map(|idx| {
            (
                TaskId::new(self.server_id, self.chunk_seq, idx as u16).to_string(),
                self.payloads.get(idx).unwrap_or_default(),
            )
        })
    }

    /// Task ids of every leased task, in index order.
    pub fn task_ids(&self) -> Vec<String> {
        self.tasks().map(|(task_id, _)| task_id).collect()
    }
}

static NEXT_LEASE_TOKEN: AtomicU64 = AtomicU64::new(1);

/// A process-unique lease token.
pub fn next_lease_token() -> u64 {
    NEXT_LEASE_TOKEN.fetch_add(1, Ordering::Relaxed)
}

impl LeasedTaskQueue {
    /// The lease map of `task_type`, if the type is known.
    pub fn leases(&self, task_type: &Identifier) -> Option<Arc<DashMap<u64, LeaseCell>>> {
        self.tasks.get(task_type).map(|leases| leases.clone())
    }

    /// The lease map of `task_type`, created on first use.
    pub fn leases_or_default(&self, task_type: &Identifier) -> Arc<DashMap<u64, LeaseCell>> {
        if let Some(leases) = self.leases(task_type) {
            return leases;
        }
        self.tasks.entry(task_type.clone()).or_default().clone()
    }

    /// The lease cell of a chunk, cloned out of the map.
    pub fn cell(&self, task_type: &Identifier, chunk_seq: u64) -> Option<LeaseCell> {
        self.leases(task_type)?
            .get(&chunk_seq)
            .map(|cell| cell.clone())
    }

    /// Ends a lease whose mutex the caller holds: marks it gone and drops it
    /// from the map (only if the map still holds this very cell).
    pub fn end(&self, task_type: &Identifier, cell: &LeaseCell, lease: &mut ChunkLease) {
        lease.gone = true;
        if let Some(leases) = self.leases(task_type) {
            leases.remove_if(&lease.chunk.chunk_seq, |_, current| {
                Arc::ptr_eq(current, cell)
            });
        }
    }

    /// Open tasks across every live lease (for stats / load reporting).
    pub fn open_tasks(&self) -> usize {
        // Clone the cells out first: never lock a lease under a map guard.
        let cells: Vec<LeaseCell> = self
            .tasks
            .iter()
            .flat_map(|leases| {
                leases
                    .iter()
                    .map(|cell| cell.value().clone())
                    .collect::<Vec<_>>()
            })
            .collect();
        cells
            .iter()
            .map(|cell| {
                let lease = lock_lease(cell);
                if lease.gone {
                    0
                } else {
                    lease.chunk.open.count()
                }
            })
            .sum()
    }

    /// Drops every expired lease and puts its still-open tasks back into the
    /// queue (or the recovery set if the queue is full).
    pub fn reap_expired(api: &Arc<ServerAPI>) {
        let now = Utc::now();
        // Snapshot the per-type maps and cells so no map guard is held while a
        // lease mutex is taken.
        let types: Vec<_> = api
            .leased_tasks
            .tasks
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        for (task_type, leases) in types {
            let cells: Vec<LeaseCell> = leases.iter().map(|cell| cell.value().clone()).collect();
            for cell in cells {
                let chunk = {
                    let mut lease = lock_lease(&cell);
                    if lease.gone || !lease.expired(now) {
                        continue;
                    }
                    api.leased_tasks.end(&task_type, &cell, &mut lease);
                    lease.chunk.clone()
                };
                ServerAPI::requeue(api, &task_type, chunk);
            }
        }
    }
}
