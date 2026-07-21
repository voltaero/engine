//! Bounded, single-owner ZeroMQ transport pipeline.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use enginelib::api::ServerAPI;
use enginelib::config::TransportConfig;
use enginelib::error::Error;
use enginelib::nucleus::query::Query;
use enginelib::protocol::{Envelope, Request, Response, decode, encode};
use futures::{SinkExt, StreamExt};
use tmq::{Context, Multipart, router};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{error, warn};

#[derive(Debug, Clone, Copy)]
enum OperationClass {
    LeasePoll,
    Mutation,
    Query,
}

impl OperationClass {
    fn of(request: &Request) -> Self {
        match request {
            Request::LeaseBatch { .. } => Self::LeasePoll,
            Request::Query(_) => Self::Query,
            _ => Self::Mutation,
        }
    }
}

/// In-process counters are always available to tests and operators even when no
/// external metrics recorder is installed. Labels are intentionally bounded.
#[derive(Debug, Default)]
pub struct TransportStats {
    admitted: AtomicUsize,
    overloaded: AtomicUsize,
    dropped_replies: AtomicUsize,
    send_failures: AtomicUsize,
    active: AtomicUsize,
    max_active: AtomicUsize,
    active_long_polls: AtomicUsize,
    max_active_long_polls: AtomicUsize,
    reply_count: AtomicUsize,
    reply_bytes: AtomicUsize,
    max_reply_count: AtomicUsize,
    max_reply_bytes: AtomicUsize,
    shutdown_drops: AtomicUsize,
}

impl TransportStats {
    pub fn admitted(&self) -> usize {
        self.admitted.load(Ordering::Relaxed)
    }
    pub fn overloaded(&self) -> usize {
        self.overloaded.load(Ordering::Relaxed)
    }
    pub fn dropped_replies(&self) -> usize {
        self.dropped_replies.load(Ordering::Relaxed)
    }
    pub fn active(&self) -> usize {
        self.active.load(Ordering::Relaxed)
    }
    pub fn max_active(&self) -> usize {
        self.max_active.load(Ordering::Relaxed)
    }
    pub fn max_active_long_polls(&self) -> usize {
        self.max_active_long_polls.load(Ordering::Relaxed)
    }
    pub fn reply_count(&self) -> usize {
        self.reply_count.load(Ordering::Relaxed)
    }
    pub fn reply_bytes(&self) -> usize {
        self.reply_bytes.load(Ordering::Relaxed)
    }
    pub fn max_reply_count(&self) -> usize {
        self.max_reply_count.load(Ordering::Relaxed)
    }
    pub fn max_reply_bytes(&self) -> usize {
        self.max_reply_bytes.load(Ordering::Relaxed)
    }
}

fn observe_max(target: &AtomicUsize, value: usize) {
    target.fetch_max(value, Ordering::Relaxed);
}

#[derive(Clone)]
struct AdmissionController {
    global: Arc<Semaphore>,
    long_polls: Arc<Semaphore>,
    mutations: Arc<Semaphore>,
    queries: Arc<Semaphore>,
    stats: Arc<TransportStats>,
}

impl AdmissionController {
    fn new(config: &TransportConfig, stats: Arc<TransportStats>) -> Self {
        Self {
            global: Arc::new(Semaphore::new(config.max_active_requests)),
            long_polls: Arc::new(Semaphore::new(config.max_active_long_polls)),
            mutations: Arc::new(Semaphore::new(config.max_active_mutations)),
            queries: Arc::new(Semaphore::new(config.max_active_queries)),
            stats,
        }
    }

    fn try_admit(&self, class: OperationClass) -> Option<AdmissionGuard> {
        let global = self.global.clone().try_acquire_owned().ok()?;
        let class_permit = match class {
            OperationClass::LeasePoll => self.long_polls.clone().try_acquire_owned(),
            OperationClass::Mutation => self.mutations.clone().try_acquire_owned(),
            OperationClass::Query => self.queries.clone().try_acquire_owned(),
        }
        .ok()?;

        self.stats.admitted.fetch_add(1, Ordering::Relaxed);
        let active = self.stats.active.fetch_add(1, Ordering::Relaxed) + 1;
        observe_max(&self.stats.max_active, active);
        if matches!(class, OperationClass::LeasePoll) {
            let active = self.stats.active_long_polls.fetch_add(1, Ordering::Relaxed) + 1;
            observe_max(&self.stats.max_active_long_polls, active);
        }

        Some(AdmissionGuard {
            _global: global,
            _class: class_permit,
            class,
            stats: self.stats.clone(),
        })
    }
}

struct AdmissionGuard {
    _global: OwnedSemaphorePermit,
    _class: OwnedSemaphorePermit,
    class: OperationClass,
    stats: Arc<TransportStats>,
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        self.stats.active.fetch_sub(1, Ordering::Relaxed);
        if matches!(self.class, OperationClass::LeasePoll) {
            self.stats.active_long_polls.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

#[derive(Clone)]
struct ReplyBudget {
    slots: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
}

struct ReplyBudgetGuard {
    _slot: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
    bytes: usize,
    stats: Arc<TransportStats>,
}

impl Drop for ReplyBudgetGuard {
    fn drop(&mut self) {
        self.stats.reply_count.fetch_sub(1, Ordering::Relaxed);
        self.stats
            .reply_bytes
            .fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

struct ReplyEnvelope {
    identity: Vec<u8>,
    payload: Vec<u8>,
    _admission: Option<AdmissionGuard>,
    _budget: ReplyBudgetGuard,
}

/// Serve indefinitely using the configuration stored on `ServerAPI`.
pub async fn serve(api: Arc<ServerAPI>, endpoint: &str) -> Result<(), Error> {
    let config = api.cfg.config_toml.transport.clone();
    let stats = Arc::new(TransportStats::default());
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    serve_with_shutdown(api, endpoint, config, shutdown_rx, stats).await
}

/// Serve one transport shard until `shutdown` becomes true.
pub async fn serve_with_shutdown(
    api: Arc<ServerAPI>,
    endpoint: &str,
    config: TransportConfig,
    mut shutdown: watch::Receiver<bool>,
    stats: Arc<TransportStats>,
) -> Result<(), Error> {
    config.validate().map_err(Error::invalid_argument)?;

    let context = Context::new();
    let mut socket = router(&context)
        .set_maxmsgsize(config.max_wire_bytes as i64)
        .set_sndhwm(config.zmq_sndhwm)
        .set_rcvhwm(config.zmq_rcvhwm)
        .set_linger(config.zmq_linger_ms)
        .bind(endpoint)
        .map_err(|err| Error::io_error(format!("Failed to bind ROUTER on {endpoint}: {err}")))?;

    let admission = AdmissionController::new(&config, stats.clone());
    let reply_budget = ReplyBudget {
        slots: Arc::new(Semaphore::new(config.reply_queue_count)),
        bytes: Arc::new(Semaphore::new(config.reply_queue_bytes)),
    };
    let (reply_tx, mut reply_rx) = mpsc::channel::<ReplyEnvelope>(config.reply_queue_count);
    let mut handlers = JoinSet::new();

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            Some(joined) = handlers.join_next(), if !handlers.is_empty() => {
                if let Err(err) = joined {
                    error!("transport handler failed: {err}");
                }
            }
            Some(reply) = reply_rx.recv() => {
                send_reply(&mut socket, reply, &stats).await;
            }
            incoming = socket.next() => {
                let Some(incoming) = incoming else { break; };
                let multipart = match incoming {
                    Ok(multipart) => multipart,
                    Err(err) => {
                        error!("ROUTER receive failed: {err}");
                        continue;
                    }
                };
                admit_message(
                    multipart,
                    api.clone(),
                    &config,
                    &admission,
                    &reply_tx,
                    &reply_budget,
                    &stats,
                    &mut handlers,
                );
            }
        }
    }

    // No new work is admitted. Existing handlers may still enqueue through
    // their sender clones; drain them until the configured deadline.
    drop(reply_tx);
    let deadline = Instant::now() + Duration::from_millis(config.shutdown_timeout_ms);
    while !handlers.is_empty() || !reply_rx.is_empty() {
        tokio::select! {
            Some(joined) = handlers.join_next(), if !handlers.is_empty() => {
                if let Err(err) = joined {
                    error!("transport handler failed while draining: {err}");
                }
            }
            Some(reply) = reply_rx.recv() => {
                let ReplyEnvelope {
                    identity,
                    payload,
                    _admission,
                    _budget,
                } = reply;
                let multipart: Multipart = vec![identity, payload].into();
                let send = socket.send(multipart);
                tokio::pin!(send);
                tokio::select! {
                    result = &mut send => {
                        if let Err(err) = result {
                            stats.send_failures.fetch_add(1, Ordering::Relaxed);
                            error!("ROUTER send failed while draining: {err}");
                        }
                    }
                    _ = tokio::time::sleep_until(deadline) => {
                        // Cancellation is safe only because the socket is dropped
                        // immediately and will never flush tmq's retained multipart.
                        stats.shutdown_drops.fetch_add(1, Ordering::Relaxed);
                        handlers.abort_all();
                        return Ok(());
                    }
                }
            }
            _ = tokio::time::sleep_until(deadline) => {
                break;
            }
        }
    }

    if !handlers.is_empty() {
        handlers.abort_all();
    }
    while let Ok(reply) = reply_rx.try_recv() {
        stats.shutdown_drops.fetch_add(1, Ordering::Relaxed);
        drop(reply);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn admit_message(
    mut multipart: Multipart,
    api: Arc<ServerAPI>,
    config: &TransportConfig,
    admission: &AdmissionController,
    reply_tx: &mpsc::Sender<ReplyEnvelope>,
    reply_budget: &ReplyBudget,
    stats: &Arc<TransportStats>,
    handlers: &mut JoinSet<()>,
) {
    if multipart.len() != 2 {
        warn!("dropping malformed message with {} frames", multipart.len());
        return;
    }
    let identity = multipart.pop_front().expect("two frames").to_vec();
    let payload = multipart.pop_front().expect("two frames").to_vec();
    if payload.len() > config.max_wire_bytes {
        try_enqueue_immediate(
            identity,
            Response::Err(Error::invalid_argument("request exceeds wire size limit")),
            reply_tx,
            reply_budget,
            stats,
            config,
        );
        return;
    }

    let envelope: Envelope = match decode(&payload) {
        Ok(envelope) => envelope,
        Err(err) => {
            try_enqueue_immediate(
                identity,
                Response::Err(err),
                reply_tx,
                reply_budget,
                stats,
                config,
            );
            return;
        }
    };
    if let Err(err) = validate_request(&envelope.req, config) {
        try_enqueue_immediate(
            identity,
            Response::Err(err),
            reply_tx,
            reply_budget,
            stats,
            config,
        );
        return;
    }

    let class = OperationClass::of(&envelope.req);
    let Some(guard) = admission.try_admit(class) else {
        stats.overloaded.fetch_add(1, Ordering::Relaxed);
        try_enqueue_immediate(
            identity,
            Response::Overloaded { retry_after_ms: 50 },
            reply_tx,
            reply_budget,
            stats,
            config,
        );
        return;
    };

    let reply_tx = reply_tx.clone();
    let reply_budget = reply_budget.clone();
    let stats = stats.clone();
    let limits = config.clone();
    handlers.spawn(async move {
        let response = crate::server::dispatch(api, envelope, limits.clone()).await;
        let payload = encode_limited(response, limits.max_reply_bytes);
        let Ok(payload) = payload else {
            stats.dropped_replies.fetch_add(1, Ordering::Relaxed);
            return;
        };
        enqueue_reply(
            identity,
            payload,
            Some(guard),
            &reply_tx,
            reply_budget,
            stats,
        )
        .await;
    });
}

fn validate_request(request: &Request, config: &TransportConfig) -> Result<(), Error> {
    match request {
        Request::SubmitBatch { tasks, .. } => {
            validate_batch(tasks.iter().map(Vec::len), tasks.len(), config)
        }
        Request::CompleteBatch { results, .. } => validate_batch(
            results
                .iter()
                .map(|(task_id, bytes)| task_id.len().saturating_add(bytes.len())),
            results.len(),
            config,
        ),
        Request::LeaseBatch { max, .. } if *max as usize > config.max_batch_items => Err(
            Error::invalid_argument("lease batch exceeds configured item limit"),
        ),
        Request::Query(Query::Scan { limit, .. }) if *limit > config.max_batch_items => Err(
            Error::invalid_argument("query page exceeds configured item limit"),
        ),
        _ => Ok(()),
    }
}

fn validate_batch(
    sizes: impl Iterator<Item = usize>,
    count: usize,
    config: &TransportConfig,
) -> Result<(), Error> {
    if count > config.max_batch_items {
        return Err(Error::invalid_argument(
            "batch exceeds configured item limit",
        ));
    }
    let mut bytes = 0usize;
    for size in sizes {
        bytes = bytes
            .checked_add(size)
            .ok_or_else(|| Error::invalid_argument("batch byte size overflow"))?;
        if bytes > config.max_batch_bytes {
            return Err(Error::invalid_argument(
                "batch exceeds configured byte limit",
            ));
        }
    }
    Ok(())
}

fn encode_limited(response: Response, max_reply_bytes: usize) -> Result<Vec<u8>, Error> {
    let payload = encode(&response)?;
    if payload.len() <= max_reply_bytes {
        return Ok(payload);
    }
    let fallback = Response::Err(Error::invalid_argument(
        "response exceeds configured byte limit",
    ));
    let payload = encode(&fallback)?;
    if payload.len() <= max_reply_bytes {
        Ok(payload)
    } else {
        Err(Error::invalid_argument("reply limit is too small"))
    }
}

fn try_enqueue_immediate(
    identity: Vec<u8>,
    response: Response,
    reply_tx: &mpsc::Sender<ReplyEnvelope>,
    reply_budget: &ReplyBudget,
    stats: &Arc<TransportStats>,
    config: &TransportConfig,
) {
    let Ok(payload) = encode_limited(response, config.max_reply_bytes) else {
        stats.dropped_replies.fetch_add(1, Ordering::Relaxed);
        return;
    };
    let Ok(slot) = reply_budget.slots.clone().try_acquire_owned() else {
        stats.dropped_replies.fetch_add(1, Ordering::Relaxed);
        return;
    };
    let Ok(bytes) = reply_budget
        .bytes
        .clone()
        .try_acquire_many_owned(payload.len() as u32)
    else {
        stats.dropped_replies.fetch_add(1, Ordering::Relaxed);
        return;
    };
    let budget = track_reply_budget(slot, bytes, payload.len(), stats.clone());
    let reply = ReplyEnvelope {
        identity,
        payload,
        _admission: None,
        _budget: budget,
    };
    if reply_tx.try_send(reply).is_err() {
        stats.dropped_replies.fetch_add(1, Ordering::Relaxed);
    }
}

async fn enqueue_reply(
    identity: Vec<u8>,
    payload: Vec<u8>,
    admission: Option<AdmissionGuard>,
    reply_tx: &mpsc::Sender<ReplyEnvelope>,
    reply_budget: ReplyBudget,
    stats: Arc<TransportStats>,
) {
    let byte_count = payload.len();
    let Ok(slot) = reply_budget.slots.acquire_owned().await else {
        stats.dropped_replies.fetch_add(1, Ordering::Relaxed);
        return;
    };
    let Ok(bytes) = reply_budget
        .bytes
        .acquire_many_owned(byte_count as u32)
        .await
    else {
        stats.dropped_replies.fetch_add(1, Ordering::Relaxed);
        return;
    };
    let budget = track_reply_budget(slot, bytes, byte_count, stats.clone());
    let reply = ReplyEnvelope {
        identity,
        payload,
        _admission: admission,
        _budget: budget,
    };
    if reply_tx.send(reply).await.is_err() {
        stats.dropped_replies.fetch_add(1, Ordering::Relaxed);
    }
}

fn track_reply_budget(
    slot: OwnedSemaphorePermit,
    byte_permit: OwnedSemaphorePermit,
    bytes: usize,
    stats: Arc<TransportStats>,
) -> ReplyBudgetGuard {
    let count = stats.reply_count.fetch_add(1, Ordering::Relaxed) + 1;
    let total_bytes = stats.reply_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
    observe_max(&stats.max_reply_count, count);
    observe_max(&stats.max_reply_bytes, total_bytes);
    ReplyBudgetGuard {
        _slot: slot,
        _bytes: byte_permit,
        bytes,
        stats,
    }
}

async fn send_reply(
    socket: &mut tmq::router::Router,
    reply: ReplyEnvelope,
    stats: &Arc<TransportStats>,
) {
    let ReplyEnvelope {
        identity,
        payload,
        _admission,
        _budget,
    } = reply;
    let multipart: Multipart = vec![identity, payload].into();
    if let Err(err) = socket.send(multipart).await {
        stats.send_failures.fetch_add(1, Ordering::Relaxed);
        error!("ROUTER send failed: {err}");
    }
}
