//! Worker: leases tasks of the types this process has mods for, runs them
//! through the mod's [`Task`] and completes them.
//!
//! [`run`] drives one connection with two roles:
//! - the **updater** (the calling thread) owns the [`Client`] and keeps every
//!   request pipelined: it leases the next batch while the current one runs,
//!   completes finished batches (asking for more with `then_lease`), and
//!   renews every held lease on one timer;
//! - `threads` **compute** threads take slices of leased batches from a
//!   channel and run them. The thread that finishes a batch's last slice wakes
//!   the updater over an inproc socket, so the updater waits on the server
//!   and the pool at once and never blocks on either.
//!
//! A task runs as `from_bytes(payload)` -> `run` -> `to_bytes`; a derived chunk
//! as `from_bytes(encode_derived(base, range))` (see
//! [`crate::chunk::decode_derived`]) and its one result completes the chunk.
//! Tasks that panic or are skipped by a stop are handed back with
//! `CancelLease` once the batch's completion is acknowledged (cancel requeues
//! only the still-open tasks, so completed ones stay done).
//!
//! The client events fire on the updater around every leased batch:
//! `task_id` is the task type (`"namespace:name"`) and `instance_ids` the wire
//! task ids. `before_task_block_acquire` can skip a type,
//! `before_task_block_execute` and `before_task_block_publish` hand the
//! batch's leases back, and mods may rewrite the payloads (before execute) or
//! results (before publish) in place. Event arguments are only built when a
//! handler is registered.

use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use crossbeam::channel::{Receiver, Sender};
use tracing::{debug, info, warn};

use crate::Identifier;
use crate::api::ServerAPI;
use crate::chunk::encode_derived;
use crate::error::{Error, ErrorKind};
use crate::events::Events;
use crate::task::{Runner, Task};
use crate::transport::client::Client;
use crate::transport::{context, sleep_unless_stopped, zmq_err};
use crate::wire::{
    Auth, Caps, Envelope, LeaseRef, LeaseReq, Leased, LeasedItem, Request, Response, RunnerKind,
    TaskResult, TaskTypeInfo,
};

#[derive(Debug, Clone)]
pub struct WorkerOptions {
    /// Task types to work on; empty = every type both the server and the
    /// loaded mods know.
    pub task_types: Vec<Identifier>,
    /// `max_tasks` per lease (whole chunks, so a lease may return more). The
    /// next lease goes out once fewer than `max(max_tasks, threads)` leased
    /// tasks are still waiting for a compute thread.
    pub max_tasks: u32,
    /// How long one lease waits for work, split across the task types.
    pub wait: Duration,
    pub runner: Option<Runner>,
    /// Compute threads.
    pub threads: usize,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            task_types: Vec::new(),
            max_tasks: 1024,
            wait: Duration::from_secs(1),
            runner: None,
            threads: std::thread::available_parallelism().map_or(1, |n| n.get()),
        }
    }
}

/// Capabilities announced in `Hello` for `runner`.
pub fn caps(runner: Option<Runner>) -> Caps {
    let mut runners = vec![RunnerKind::Cpu];
    if matches!(runner, Some(Runner::HIP)) {
        runners.push(RunnerKind::Hip);
    }
    Caps {
        runners,
        max_inflight: 1,
    }
}

/// `auth` with the `uid` / `challenge` that `client:auth_prepare` handlers put
/// in the headers map (mods that sign challenges, issue tickets, ...).
pub fn auth(api: &ServerAPI, mut auth: Auth) -> Auth {
    let headers = Arc::new(RwLock::new(Default::default()));
    Events::ClientAuthPrepare(api, headers.clone());
    let headers = headers.read().unwrap_or_else(|err| err.into_inner());
    if let Some(uid) = headers.get("uid") {
        auth.uid = uid.clone();
    }
    if let Some(challenge) = headers.get("challenge") {
        auth.challenge = challenge.clone();
    }
    auth
}

/// The task types this worker runs on the connected server.
pub fn runnable_types(api: &ServerAPI, client: &Client, opts: &WorkerOptions) -> Vec<TaskTypeInfo> {
    client
        .server
        .task_types
        .iter()
        .filter(|info| api.task_registry.tasks.contains_key(&info.task_type))
        .filter(|info| opts.task_types.is_empty() || opts.task_types.contains(&info.task_type))
        .cloned()
        .collect()
}

/// Works until `stop` is set, (re)connecting with `connect` whenever the
/// connection fails; batches finished meanwhile are completed on the new
/// connection (leases belong to the uid, not the connection). On stop,
/// unstarted tasks are handed back and finished ones completed before it
/// returns. Errors only when the server offers no task type this worker can
/// run, or the compute pool can't be set up.
pub fn run(
    api: &ServerAPI,
    mut connect: impl FnMut() -> Result<Client, Error>,
    opts: &WorkerOptions,
    stop: &AtomicBool,
) -> Result<(), Error> {
    static NEXT_POOL: AtomicU64 = AtomicU64::new(0);
    let threads = opts.threads.max(1);
    let endpoint = format!(
        "inproc://enginelib-worker-{}",
        NEXT_POOL.fetch_add(1, Ordering::Relaxed)
    );
    let wake = context()
        .socket(zmq::PULL)
        .map_err(|err| zmq_err("Failed to create PULL", err))?;
    wake.bind(&endpoint)
        .map_err(|err| zmq_err("Failed to bind the wake socket", err))?;
    let mut pushes = Vec::with_capacity(threads);
    for _ in 0..threads {
        let push = context()
            .socket(zmq::PUSH)
            .map_err(|err| zmq_err("Failed to create PUSH", err))?;
        push.connect(&endpoint)
            .map_err(|err| zmq_err("Failed to connect the wake socket", err))?;
        pushes.push(push);
    }

    let unstarted = AtomicUsize::new(0);
    let (jobs, job_rx) = crossbeam::channel::unbounded::<Job>();
    std::thread::scope(|scope| {
        for (id, push) in pushes.into_iter().enumerate() {
            let (job_rx, unstarted) = (job_rx.clone(), &unstarted);
            std::thread::Builder::new()
                .name(format!("compute-{id}"))
                .spawn_scoped(scope, move || {
                    compute(job_rx, push, unstarted, stop, opts.runner)
                })
                .expect("Failed to spawn compute thread");
        }
        drop(job_rx);

        let mut updater = Updater {
            api,
            opts,
            stop,
            unstarted: &unstarted,
            low_water: (opts.max_tasks as usize).max(threads),
            slice: threads * 4,
            jobs,
            wake,
            hooks: Hooks::new(api),
            types: Vec::new(),
            turn: 0,
            batches: HashMap::new(),
            next_batch: 0,
            pending: HashMap::new(),
            leases_inflight: 0,
        };
        // Dropping the updater closes the job channel, which ends the compute
        // threads once they drain it.
        updater.run(&mut connect)
    })
}

/// A leased batch on its way through the compute pool.
struct Batch {
    id: u64,
    task_type: Identifier,
    task: Arc<dyn Task>,
    items: Vec<LeasedItem>,
    /// One slot per item; `None` = didn't run (panicked, or skipped by stop).
    outputs: Mutex<Vec<Option<Vec<u8>>>>,
    /// Items not run yet; whoever takes it to 0 wakes the updater.
    remaining: AtomicUsize,
}

struct Job {
    batch: Arc<Batch>,
    range: Range<usize>,
}

fn compute(
    jobs: Receiver<Job>,
    wake: zmq::Socket,
    unstarted: &AtomicUsize,
    stop: &AtomicBool,
    runner: Option<Runner>,
) {
    for job in jobs.iter() {
        let len = job.range.len();
        unstarted.fetch_sub(len, Ordering::Relaxed);
        let batch = &job.batch;
        let outputs: Vec<Option<Vec<u8>>> = batch.items[job.range.clone()]
            .iter()
            .map(|item| {
                if stop.load(Ordering::Relaxed) {
                    return None;
                }
                run_task(batch.task.as_ref(), item, runner)
            })
            .collect();
        {
            let mut slots = batch.outputs.lock().unwrap_or_else(|err| err.into_inner());
            for (slot, output) in slots[job.range.clone()].iter_mut().zip(outputs) {
                *slot = output;
            }
        }
        if batch.remaining.fetch_sub(len, Ordering::AcqRel) == len
            && let Err(err) = wake.send(&batch.id.to_le_bytes()[..], 0)
        {
            warn!("Worker: failed to wake the updater: {err}");
        }
    }
}

/// Runs one leased task through the mod; `None` if it panicked.
fn run_task(task: &dyn Task, item: &LeasedItem, runner: Option<Runner>) -> Option<Vec<u8>> {
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let mut instance = match item.range {
            Some(range) => task.from_bytes(&encode_derived(&item.bytes, range)),
            None => task.from_bytes(&item.bytes),
        };
        instance.run(runner);
        instance.to_bytes()
    }));
    match result {
        Ok(bytes) => Some(bytes),
        Err(_) => {
            warn!("Worker: task {} panicked", item.task_id);
            None
        }
    }
}

/// Which client events have handlers, checked once: building their arguments
/// costs allocations per task.
struct Hooks {
    acquire: bool,
    acquired: bool,
    execute: bool,
    publish: bool,
}

impl Hooks {
    fn new(api: &ServerAPI) -> Self {
        let has = |name: &str| {
            api.event_bus
                .has_handlers(&("client".to_string(), name.to_string()))
        };
        Self {
            acquire: has("before_task_block_acquire"),
            acquired: has("task_block_acquired"),
            execute: has("before_task_block_execute"),
            publish: has("before_task_block_publish"),
        }
    }
}

struct Held {
    batch: Arc<Batch>,
    /// Set once the batch ran: the `Complete` to (re)send until acknowledged.
    complete: Option<Request>,
    /// Leases of tasks that didn't run, cancelled after the completion lands
    /// (cancelling first would requeue the tasks being completed).
    failed: Vec<LeaseRef>,
}

enum Pending {
    Lease,
    Complete { batch: u64, leases: bool },
    Other,
}

/// Leases requested ahead at once: with one, the pool can run dry for a
/// round trip whenever a batch takes less time to run than a lease to arrive.
const MAX_LEASES_INFLIGHT: usize = 2;

struct Updater<'a> {
    api: &'a ServerAPI,
    opts: &'a WorkerOptions,
    stop: &'a AtomicBool,
    unstarted: &'a AtomicUsize,
    /// Lease again once fewer leased tasks than this wait for a thread.
    low_water: usize,
    /// Slices per batch handed to the pool.
    slice: usize,
    jobs: Sender<Job>,
    wake: zmq::Socket,
    hooks: Hooks,
    types: Vec<TaskTypeInfo>,
    turn: usize,
    batches: HashMap<u64, Held>,
    next_batch: u64,
    /// Outstanding requests on the current connection, by `req_id`.
    pending: HashMap<u64, (Pending, Instant)>,
    /// Outstanding `Lease`s (and `Complete`s with `then_lease`).
    leases_inflight: usize,
}

impl Updater<'_> {
    fn run(&mut self, connect: &mut impl FnMut() -> Result<Client, Error>) -> Result<(), Error> {
        let mut backoff = Duration::from_millis(100);
        loop {
            if self.stopping() {
                return Ok(());
            }
            let mut client = match connect() {
                Ok(client) => client,
                Err(err) => {
                    warn!("Worker: failed to connect: {err}");
                    sleep_unless_stopped(self.stop, backoff);
                    backoff = (backoff * 2).min(Duration::from_secs(10));
                    continue;
                }
            };
            backoff = Duration::from_millis(100);
            self.types = runnable_types(self.api, &client, self.opts);
            if self.types.is_empty() {
                return Err(Error::not_found("NoRunnableTaskTypes"));
            }
            info!(
                "Worker: connected to {} (server {}), {} task type(s), {} compute thread(s)",
                client.endpoint(),
                client.server.server_id,
                self.types.len(),
                self.opts.threads.max(1)
            );
            match self.serve(&mut client) {
                Ok(()) => return Ok(()),
                Err(err) => {
                    warn!("Worker: connection to {} failed: {err}", client.endpoint());
                    self.pending.clear();
                    self.leases_inflight = 0;
                }
            }
        }
    }

    fn stopping(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
    }

    /// Runs one connection until stopped and drained (`Ok`) or the connection
    /// fails (`Err`).
    fn serve(&mut self, client: &mut Client) -> Result<(), Error> {
        // Completions that were in flight on a previous connection.
        let unacked: Vec<u64> = self
            .batches
            .iter()
            .filter(|(_, held)| held.complete.is_some())
            .map(|(id, _)| *id)
            .collect();
        for id in unacked {
            self.send_complete(client, id)?;
        }

        let renew_every =
            Duration::from_secs(client.server.lease_ttl_secs / 3).max(Duration::from_secs(1));
        let mut next_renew = Instant::now() + renew_every;
        loop {
            let stopping = self.stopping();
            if stopping
                && self.batches.is_empty()
                && !self
                    .pending
                    .values()
                    .any(|(kind, _)| !matches!(kind, Pending::Other))
            {
                return Ok(());
            }
            if !stopping {
                self.maybe_lease(client)?;
            }
            let now = Instant::now();
            if now >= next_renew {
                self.renew(client)?;
                next_renew = now + renew_every;
            }
            if let Some((_, deadline)) = self.pending.values().min_by_key(|(_, at)| *at)
                && *deadline <= now
            {
                return Err(Error::io_error("Timed out waiting for the server"));
            }

            let mut wait = next_renew.saturating_duration_since(now);
            if let Some((_, deadline)) = self.pending.values().min_by_key(|(_, at)| *at) {
                wait = wait.min(deadline.saturating_duration_since(now));
            }
            // Wake now and then to notice `stop`.
            wait = wait.min(Duration::from_millis(100));
            if !client.has_buffered() {
                let mut items = [client.poll_item(), self.wake.as_poll_item(zmq::POLLIN)];
                zmq::poll(&mut items, wait.as_millis().max(1) as i64)
                    .map_err(|err| zmq_err("Failed to poll", err))?;
            }

            loop {
                match self.wake.recv_bytes(zmq::DONTWAIT) {
                    Ok(bytes) => {
                        let id = u64::from_le_bytes(bytes.try_into().unwrap_or_default());
                        self.finish(client, id)?;
                    }
                    Err(zmq::Error::EAGAIN) => break,
                    Err(err) => return Err(zmq_err("Failed to receive a wake-up", err)),
                }
            }
            while let Some(envelope) = client.try_recv()? {
                self.on_reply(client, envelope)?;
            }
        }
    }

    fn track(&mut self, client: &Client, req_id: u64, kind: Pending, extra: Duration) {
        let deadline = Instant::now() + client.timeout + extra;
        self.pending.insert(req_id, (kind, deadline));
    }

    /// The next task type to lease, round robin, skipping types a
    /// `before_task_block_acquire` handler cancels.
    fn next_type(&mut self) -> Option<Identifier> {
        for _ in 0..self.types.len() {
            let task_type = self.types[self.turn % self.types.len()].task_type.clone();
            self.turn += 1;
            if self.hooks.acquire
                && Events::BeforeTaskBlockAcquire(self.api, vec![type_name(&task_type)])
            {
                continue;
            }
            return Some(task_type);
        }
        None
    }

    fn wants_work(&self) -> bool {
        self.leases_inflight < MAX_LEASES_INFLIGHT
            && !self.stopping()
            && self.unstarted.load(Ordering::Relaxed) < self.low_water
    }

    fn lease_wait(&self) -> Duration {
        self.opts.wait / self.types.len().max(1) as u32
    }

    fn maybe_lease(&mut self, client: &mut Client) -> Result<(), Error> {
        if !self.wants_work() {
            return Ok(());
        }
        let Some(task_type) = self.next_type() else {
            return Ok(());
        };
        let wait = self.lease_wait();
        let request = Request::Lease {
            task_type,
            max_tasks: self.opts.max_tasks.max(1),
            wait_ms: wait.as_millis().min(u32::MAX as u128) as u32,
        };
        let req_id = client.send(&request)?;
        self.track(client, req_id, Pending::Lease, wait);
        self.leases_inflight += 1;
        Ok(())
    }

    fn on_reply(&mut self, client: &mut Client, envelope: Envelope<Response>) -> Result<(), Error> {
        let Some((kind, _)) = self.pending.remove(&envelope.req_id) else {
            return Ok(());
        };
        if let Response::Error(err) = &envelope.body
            && err.message == "NotAuthenticated"
        {
            // The server forgot the session (restart, idle sweep): reconnect.
            return Err(Error::io_error("NotAuthenticated"));
        }
        match (kind, envelope.body) {
            (Pending::Lease, body) => {
                self.leases_inflight -= 1;
                match body {
                    Response::Leased(leased) => self.dispatch(client, leased)?,
                    Response::Error(err) if err.message == "QueueEmpty" => {}
                    Response::Error(err) => warn!("Worker: lease failed: {}", err.message),
                    other => warn!("Worker: unexpected lease reply: {other:?}"),
                }
            }
            (Pending::Complete { batch, leases }, body) => {
                if leases {
                    self.leases_inflight -= 1;
                }
                let held = self.batches.remove(&batch);
                match body {
                    Response::Completed { results, next } => {
                        if let Some(held) = &held {
                            for (item, result) in held.batch.items.iter().zip(&results) {
                                if let Err(err) = result {
                                    warn!(
                                        "Worker: completing {} failed: {}",
                                        item.task_id, err.message
                                    );
                                }
                            }
                        }
                        if let Some(next) = next {
                            self.dispatch(client, next)?;
                        }
                    }
                    Response::Error(err) => warn!("Worker: complete failed: {}", err.message),
                    other => warn!("Worker: unexpected complete reply: {other:?}"),
                }
                if let Some(held) = held {
                    self.cancel(client, &held.failed)?;
                }
            }
            (Pending::Other, Response::Renewed { results }) => {
                for result in results {
                    if let Err(err) = result {
                        warn!("Worker: lease renewal failed: {}", err.message);
                    }
                }
            }
            (Pending::Other, _) => {}
        }
        Ok(())
    }

    /// Hands a freshly leased batch to the compute pool.
    fn dispatch(&mut self, client: &mut Client, leased: Leased) -> Result<(), Error> {
        if leased.tasks.is_empty() {
            return Ok(());
        }
        let task = self
            .api
            .task_registry
            .tasks
            .get(&leased.task_type)
            .map(|entry| entry.value().clone());
        let Some(task) = task.filter(|_| !self.stopping()) else {
            return self.cancel(client, &lease_refs(&leased.task_type, &leased.tasks));
        };
        let Leased {
            task_type,
            mut tasks,
            ..
        } = leased;
        debug!(
            "Worker: leased {} task(s) of {}",
            tasks.len(),
            type_name(&task_type)
        );

        if self.hooks.acquired || self.hooks.execute {
            let name = type_name(&task_type);
            let ids: Vec<String> = tasks.iter().map(|task| task.task_id.clone()).collect();
            let payloads: Vec<Arc<RwLock<Vec<u8>>>> = tasks
                .iter_mut()
                .map(|task| Arc::new(RwLock::new(std::mem::take(&mut task.bytes))))
                .collect();
            if self.hooks.acquired {
                Events::TaskBlockAcquired(self.api, name.clone(), ids.clone(), payloads.clone());
            }
            let cancelled = self.hooks.execute
                && Events::BeforeTaskBlockExecute(self.api, name, ids, payloads.clone());
            for (task, payload) in tasks.iter_mut().zip(&payloads) {
                task.bytes = take(payload);
            }
            if cancelled {
                return self.cancel(client, &lease_refs(&task_type, &tasks));
            }
        }

        let n = tasks.len();
        let id = self.next_batch;
        self.next_batch += 1;
        let batch = Arc::new(Batch {
            id,
            task_type,
            task,
            items: tasks,
            outputs: Mutex::new(vec![None; n]),
            remaining: AtomicUsize::new(n),
        });
        self.batches.insert(
            id,
            Held {
                batch: batch.clone(),
                complete: None,
                failed: Vec::new(),
            },
        );
        self.unstarted.fetch_add(n, Ordering::Relaxed);
        let per = n.div_ceil(self.slice).max(1);
        for start in (0..n).step_by(per) {
            let job = Job {
                batch: batch.clone(),
                range: start..(start + per).min(n),
            };
            // The receivers live as long as the updater (scoped threads).
            let _ = self.jobs.send(job);
        }
        Ok(())
    }

    /// A batch ran: complete what ran, remember what didn't.
    fn finish(&mut self, client: &mut Client, id: u64) -> Result<(), Error> {
        let Some(batch) = self.batches.get(&id).map(|held| held.batch.clone()) else {
            return Ok(());
        };
        let outputs =
            std::mem::take(&mut *batch.outputs.lock().unwrap_or_else(|err| err.into_inner()));
        let mut results = Vec::with_capacity(outputs.len());
        let mut failed = Vec::new();
        for (item, output) in batch.items.iter().zip(outputs) {
            let lease = LeaseRef {
                task_type: batch.task_type.clone(),
                task_id: item.task_id.clone(),
                token: item.token,
            };
            match output {
                Some(bytes) => results.push(TaskResult { lease, bytes }),
                None => failed.push(lease),
            }
        }

        if self.hooks.publish && !results.is_empty() {
            let ids = results
                .iter()
                .map(|result| result.lease.task_id.clone())
                .collect();
            let outputs: Vec<Arc<RwLock<Vec<u8>>>> = results
                .iter_mut()
                .map(|result| Arc::new(RwLock::new(std::mem::take(&mut result.bytes))))
                .collect();
            let cancelled = Events::BeforeTaskBlockPublish(
                self.api,
                type_name(&batch.task_type),
                ids,
                outputs.clone(),
            );
            if cancelled {
                self.batches.remove(&id);
                return self.cancel(client, &lease_refs(&batch.task_type, &batch.items));
            }
            for (result, output) in results.iter_mut().zip(&outputs) {
                result.bytes = take(output);
            }
        }

        if results.is_empty() {
            self.batches.remove(&id);
            return self.cancel(client, &failed);
        }
        if let Some(held) = self.batches.get_mut(&id) {
            held.failed = failed;
            held.complete = Some(Request::Complete {
                results,
                then_lease: None,
            });
        }
        self.send_complete(client, id)
    }

    /// Sends (or re-sends) a finished batch's `Complete`, leasing the next
    /// batch with it when the pool is running low.
    fn send_complete(&mut self, client: &mut Client, id: u64) -> Result<(), Error> {
        let then_lease = if self.wants_work() {
            self.next_type().map(|task_type| LeaseReq {
                task_type,
                max_tasks: self.opts.max_tasks.max(1),
            })
        } else {
            None
        };
        let leases = then_lease.is_some();
        let Some(request) = self
            .batches
            .get_mut(&id)
            .and_then(|held| held.complete.as_mut())
        else {
            return Ok(());
        };
        if let Request::Complete {
            then_lease: slot, ..
        } = request
        {
            *slot = then_lease;
        }
        let req_id = client.send(request)?;
        self.track(
            client,
            req_id,
            Pending::Complete { batch: id, leases },
            Duration::ZERO,
        );
        if leases {
            self.leases_inflight += 1;
        }
        Ok(())
    }

    /// Renews every lease this worker holds (one ref per chunk).
    fn renew(&mut self, client: &mut Client) -> Result<(), Error> {
        let mut seen = HashSet::new();
        let leases: Vec<LeaseRef> = self
            .batches
            .values()
            .flat_map(|held| {
                held.batch
                    .items
                    .iter()
                    .map(|item| (&held.batch.task_type, item))
            })
            .filter(|(_, item)| seen.insert(item.token))
            .map(|(task_type, item)| LeaseRef {
                task_type: task_type.clone(),
                task_id: item.task_id.clone(),
                token: item.token,
            })
            .collect();
        if leases.is_empty() {
            return Ok(());
        }
        let req_id = client.send(&Request::Renew { leases })?;
        self.track(client, req_id, Pending::Other, Duration::ZERO);
        Ok(())
    }

    /// Hands leases back so their open tasks are requeued at once instead of
    /// after the TTL (the reaper would requeue them anyway).
    fn cancel(&mut self, client: &mut Client, refs: &[LeaseRef]) -> Result<(), Error> {
        let leases = chunk_leases(refs);
        if leases.is_empty() {
            return Ok(());
        }
        match client.send(&Request::CancelLease { leases }) {
            Ok(req_id) => {
                self.track(client, req_id, Pending::Other, Duration::ZERO);
                Ok(())
            }
            Err(err) if err.kind() == ErrorKind::IOError => Err(err),
            Err(err) => {
                debug!("Worker: cancelling leases failed: {err}");
                Ok(())
            }
        }
    }
}

/// One ref per chunk lease: renew/cancel act on the whole chunk.
fn chunk_leases(refs: &[LeaseRef]) -> Vec<LeaseRef> {
    let mut seen = HashSet::new();
    refs.iter()
        .filter(|lease| seen.insert(lease.token))
        .cloned()
        .collect()
}

fn lease_refs(task_type: &Identifier, items: &[LeasedItem]) -> Vec<LeaseRef> {
    items
        .iter()
        .map(|item| LeaseRef {
            task_type: task_type.clone(),
            task_id: item.task_id.clone(),
            token: item.token,
        })
        .collect()
}

fn take(bytes: &RwLock<Vec<u8>>) -> Vec<u8> {
    std::mem::take(&mut *bytes.write().unwrap_or_else(|err| err.into_inner()))
}

fn type_name(task_type: &Identifier) -> String {
    format!("{}:{}", task_type.0, task_type.1)
}
