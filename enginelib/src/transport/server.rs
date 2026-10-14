//! ZeroMQ server: a ROUTER socket in front of [`ServerAPI`].
//!
//! One I/O thread owns the ROUTER socket and does no work beyond moving bytes:
//! every inbound message is handed to the tokio runtime, decoded and handled
//! there (DB-touching requests on the blocking pool), and the encoded reply comes
//! back to the I/O thread over an inproc PUSH/PULL pair, which forwards it to the
//! peer. ZeroMQ sockets are not thread-safe, so handlers never touch the ROUTER
//! directly.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use rust_rocksdb::WriteBatch;
use tokio::runtime::Handle;
use tracing::{debug, error, info, warn};

use super::{Background, POLL_MS, context, sleep_unless_stopped, zmq_err};
use crate::api::{
    ServerAPI, finished_task_key, finished_task_key_prefix, task_id_from_key, task_key,
    task_key_prefix,
};
use crate::config::ConfigTomlServer;
use crate::error::{Error, ErrorKind};
use crate::events::Events;
use crate::nucleus::query::{self as db_query, Query as DbQuery, QueryResult};
use crate::nucleus::{
    cancel::cancel, complete::complete, lease::lease_timeout, renew::renew, submit::submit_batch,
};
use crate::wire::{
    self, Auth, LeaseRef, Leased, LeasedItem, MAX_LEASE_BATCH, MAX_SCAN_LIMIT, PROTO_VERSION,
    PageRecord, ProxyRequest, ProxyResponse, Query, QueryResponse, Request, Response, Scope,
    ServerInfo, Stats, TaskRecord, TaskResult, TaskStatus, TaskTypeInfo, WireError,
};
use crate::{Identifier, Registry};

/// Socket high-water marks, in messages. Generous so bursts of small messages
/// queue instead of being dropped.
const HWM: i32 = 100_000;
/// Inbound messages handled per poll wakeup before replies get a turn.
const RECV_BURST: usize = 1024;
/// Default cap on requests being handled at once (see
/// [`ServerOptions::max_inflight`]).
pub const DEFAULT_MAX_INFLIGHT: usize = 65_536;
/// Pending records deleted per refill-excluding step of a `Purge`.
const PURGE_PAGE: usize = 10_000;

#[derive(Debug, Clone)]
pub struct ServerOptions {
    /// ZeroMQ endpoint to bind, e.g. `tcp://[::1]:50051` or `tcp://127.0.0.1:*`.
    pub endpoint: String,
    pub server_id: u16,
    /// Endpoint announced to the proxy; defaults to the bound endpoint.
    pub advertise: Option<String>,
    /// Proxy endpoint to register with, if any.
    pub proxy: Option<String>,
    /// Shared secret sent with every proxy registration (`proxy_token`).
    pub proxy_token: Option<String>,
    pub heartbeat: Duration,
    pub max_message_bytes: i64,
    pub session_idle: Duration,
    /// Most requests handled at once. When reached, the I/O thread stops reading
    /// the ROUTER until a handler finishes, so ZeroMQ's high-water marks push
    /// back on clients instead of memory growing. Parked `Lease` waits count.
    pub max_inflight: usize,
}

impl ServerOptions {
    pub fn from_config(cfg: &ConfigTomlServer) -> Self {
        Self {
            endpoint: wire::endpoint(&cfg.host),
            server_id: cfg.server_id,
            advertise: cfg.advertise.as_deref().map(wire::endpoint),
            proxy: cfg.proxy.as_deref().map(wire::endpoint),
            proxy_token: cfg.proxy_token.clone(),
            heartbeat: Duration::from_secs(cfg.heartbeat_secs.max(1)),
            max_message_bytes: cfg.max_message_bytes,
            session_idle: Duration::from_secs(cfg.session_idle_secs.max(1)),
            max_inflight: DEFAULT_MAX_INFLIGHT,
        }
    }
}

pub struct Server;

impl Server {
    /// Binds the ROUTER socket and starts serving `api` in the background. Must
    /// be called from inside a tokio runtime; handlers run on it.
    pub fn start(api: Arc<ServerAPI>, opts: ServerOptions) -> Result<Background, Error> {
        let runtime = Handle::try_current()
            .map_err(|_| Error::new("Server::start must run inside a tokio runtime".into()))?;
        let ctx = context();

        let router = ctx
            .socket(zmq::ROUTER)
            .map_err(|err| zmq_err("Failed to create ROUTER", err))?;
        router
            .set_ipv6(true)
            .map_err(|err| zmq_err("set_ipv6", err))?;
        router
            .set_linger(0)
            .map_err(|err| zmq_err("set_linger", err))?;
        router
            .set_rcvhwm(HWM)
            .map_err(|err| zmq_err("set_rcvhwm", err))?;
        router
            .set_sndhwm(HWM)
            .map_err(|err| zmq_err("set_sndhwm", err))?;
        router
            .set_maxmsgsize(opts.max_message_bytes)
            .map_err(|err| zmq_err("set_maxmsgsize", err))?;
        router
            .bind(&opts.endpoint)
            .map_err(|err| zmq_err(&format!("Failed to bind {}", opts.endpoint), err))?;
        let bound = match router.get_last_endpoint() {
            Ok(Ok(endpoint)) => endpoint,
            _ => opts.endpoint.clone(),
        };

        // Replies come back from handler threads over inproc. The name is unique
        // per server so several servers can share the process-wide context.
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let reply_endpoint = format!(
            "inproc://engine-replies-{}",
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let replies_in = ctx
            .socket(zmq::PULL)
            .map_err(|err| zmq_err("Failed to create PULL", err))?;
        replies_in
            .set_linger(0)
            .map_err(|err| zmq_err("set_linger", err))?;
        replies_in
            .set_rcvhwm(HWM)
            .map_err(|err| zmq_err("set_rcvhwm", err))?;
        replies_in
            .bind(&reply_endpoint)
            .map_err(|err| zmq_err("Failed to bind reply socket", err))?;

        let shared = Arc::new(Shared {
            api,
            server_id: opts.server_id,
            sessions: DashMap::new(),
            counters: Counters::default(),
            started: Instant::now(),
            inflight: AtomicUsize::new(0),
            max_inflight: opts.max_inflight.max(1),
            replies: ReplyPool {
                ctx,
                endpoint: reply_endpoint,
                sockets: Mutex::new(Vec::new()),
            },
        });

        let stop = Arc::new(AtomicBool::new(false));
        // Owns the threads from the start: if a later step fails, dropping it
        // stops and joins whatever already runs, releasing the bound port.
        let mut background = Background::new(bound.clone(), stop.clone(), Vec::new());
        {
            let shared = shared.clone();
            let stop = stop.clone();
            let session_idle = opts.session_idle;
            background.threads.push(
                std::thread::Builder::new()
                    .name("engine-zmq-io".into())
                    .spawn(move || {
                        io_loop(&shared, &runtime, &router, &replies_in, &stop, session_idle)
                    })
                    .map_err(|err| Error::io_error(format!("Failed to spawn I/O thread: {err}")))?,
            );
        }
        if let Some(proxy) = opts.proxy.clone() {
            let shared = shared.clone();
            let stop = stop.clone();
            let advertise = opts.advertise.clone().unwrap_or_else(|| bound.clone());
            if ["[::]", "0.0.0.0", "*"]
                .iter()
                .any(|any| advertise.contains(any))
            {
                warn!("Announcing wildcard endpoint {advertise} to the proxy; set `advertise`");
            }
            let heartbeat = opts.heartbeat;
            let token = opts.proxy_token.clone().unwrap_or_default();
            if token.is_empty() {
                warn!("`proxy_token` is unset; the proxy will refuse this server's registration");
            }
            background.threads.push(
                std::thread::Builder::new()
                    .name("engine-zmq-heartbeat".into())
                    .spawn(move || {
                        heartbeat_loop(&shared, &proxy, &advertise, &token, heartbeat, &stop)
                    })
                    .map_err(|err| {
                        Error::io_error(format!("Failed to spawn heartbeat thread: {err}"))
                    })?,
            );
        }

        info!("Server {} listening on {bound}", opts.server_id);
        Ok(background)
    }
}

fn io_loop(
    shared: &Arc<Shared>,
    runtime: &Handle,
    router: &zmq::Socket,
    replies_in: &zmq::Socket,
    stop: &AtomicBool,
    session_idle: Duration,
) {
    let mut last_sweep = Instant::now();
    while !stop.load(Ordering::SeqCst) {
        // At the in-flight cap, only wait for replies: inbound messages stay in
        // ZeroMQ's queues (and, past the HWM, in TCP) until a handler finishes.
        let saturated = shared.saturated();
        let mut items = [
            replies_in.as_poll_item(zmq::POLLIN),
            router.as_poll_item(if saturated {
                zmq::PollEvents::empty()
            } else {
                zmq::POLLIN
            }),
        ];
        if let Err(err) = zmq::poll(&mut items, POLL_MS) {
            error!("zmq poll failed: {err}");
            return;
        }
        let (outbound, inbound) = (items[0].is_readable(), items[1].is_readable());

        if outbound {
            // Forward every pending reply. A peer that has gone away (or whose
            // queue is full) just loses the reply; the ROUTER drops it.
            while let Ok(frames) = replies_in.recv_multipart(zmq::DONTWAIT) {
                if let Err(err) = router.send_multipart(frames, zmq::DONTWAIT) {
                    debug!("Dropped reply: {err}");
                }
            }
        }

        if inbound {
            for _ in 0..RECV_BURST {
                if shared.saturated() {
                    break;
                }
                let frames = match router.recv_multipart(zmq::DONTWAIT) {
                    Ok(frames) => frames,
                    Err(zmq::Error::EAGAIN) => break,
                    Err(err) => {
                        warn!("ROUTER recv failed: {err}");
                        break;
                    }
                };
                // DEALER clients send a single frame: [identity, payload].
                let mut frames = frames.into_iter();
                let (Some(identity), Some(payload)) = (frames.next(), frames.next()) else {
                    continue;
                };
                dispatch(shared, runtime, identity, payload);
            }
        }

        if last_sweep.elapsed() >= Duration::from_secs(30) {
            last_sweep = Instant::now();
            shared
                .sessions
                .retain(|_, session| session.last_seen.elapsed() < session_idle);
        }
    }
}

fn dispatch(shared: &Arc<Shared>, runtime: &Handle, identity: Vec<u8>, payload: Vec<u8>) {
    let slot = InflightSlot::acquire(shared);
    let shared = shared.clone();
    runtime.spawn(async move {
        let (req_id, response) = match wire::decode::<Request>(&payload) {
            Ok(envelope) => (
                envelope.req_id,
                shared.handle(&identity, envelope.body).await,
            ),
            Err(err) => (0, Response::Error(err.into())),
        };
        // Free the slot before handing over the reply, so the I/O thread sees
        // the capacity as soon as it wakes for the reply.
        drop(slot);
        match wire::encode(req_id, &response) {
            Ok(bytes) => shared.replies.send(identity, bytes),
            Err(err) => error!("Failed to encode response: {err}"),
        }
    });
}

/// One handler's share of [`ServerOptions::max_inflight`]; released on drop,
/// including when the handler panics.
struct InflightSlot(Arc<Shared>);
impl InflightSlot {
    fn acquire(shared: &Arc<Shared>) -> Self {
        shared.inflight.fetch_add(1, Ordering::SeqCst);
        Self(shared.clone())
    }
}
impl Drop for InflightSlot {
    fn drop(&mut self) {
        self.0.inflight.fetch_sub(1, Ordering::SeqCst);
    }
}

fn heartbeat_loop(
    shared: &Arc<Shared>,
    proxy: &str,
    advertise: &str,
    token: &str,
    interval: Duration,
    stop: &AtomicBool,
) {
    let socket = match context().socket(zmq::DEALER) {
        Ok(socket) => socket,
        Err(err) => {
            error!("Failed to create proxy socket: {err}");
            return;
        }
    };
    let _ = socket.set_ipv6(true);
    let _ = socket.set_linger(0);
    if let Err(err) = socket.connect(proxy) {
        error!("Failed to connect to proxy {proxy}: {err}");
        return;
    }
    let mut req_id = 0u64;
    let mut refused = false;
    loop {
        let info = ServerInfo {
            server_id: shared.server_id,
            endpoint: advertise.to_string(),
            task_types: shared.task_types(),
            load: shared.load(),
        };
        req_id += 1;
        let request = ProxyRequest::Register {
            info,
            token: token.to_string(),
        };
        match wire::encode(req_id, &request) {
            // DONTWAIT: while the proxy is unreachable heartbeats are simply
            // skipped; ZeroMQ reconnects on its own.
            Ok(bytes) => {
                let _ = socket.send(bytes, zmq::DONTWAIT);
            }
            Err(err) => error!("Failed to encode heartbeat: {err}"),
        }
        // Drain acknowledgements; only a refusal (bad `proxy_token`) matters,
        // logged once per change so a misconfiguration is visible but not noisy.
        while let Ok(bytes) = socket.recv_bytes(zmq::DONTWAIT) {
            match wire::decode::<ProxyResponse>(&bytes).map(|envelope| envelope.body) {
                Ok(ProxyResponse::Error(err)) if !refused => {
                    refused = true;
                    error!("Proxy {proxy} refused registration: {}", err.message);
                }
                Ok(ProxyResponse::Registered) if refused => {
                    refused = false;
                    info!("Registered with proxy {proxy}");
                }
                _ => {}
            }
        }
        if !sleep_unless_stopped(stop, interval) {
            return;
        }
    }
}

/// Pool of PUSH sockets that handlers use to hand replies to the I/O thread.
/// A socket is only ever used by one thread at a time.
struct ReplyPool {
    ctx: zmq::Context,
    endpoint: String,
    sockets: Mutex<Vec<zmq::Socket>>,
}

impl ReplyPool {
    fn send(&self, identity: Vec<u8>, bytes: Vec<u8>) {
        let socket = self
            .sockets
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .pop();
        let socket = match socket {
            Some(socket) => socket,
            None => match self.connect() {
                Ok(socket) => socket,
                Err(err) => {
                    error!("Failed to open reply socket: {err}");
                    return;
                }
            },
        };
        if let Err(err) = socket.send_multipart([identity, bytes], 0) {
            // Only times out if the I/O thread is gone (shutting down).
            debug!("Dropped reply: {err}");
        }
        self.sockets
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(socket);
    }

    fn connect(&self) -> Result<zmq::Socket, zmq::Error> {
        let socket = self.ctx.socket(zmq::PUSH)?;
        socket.set_linger(0)?;
        socket.set_sndhwm(HWM)?;
        socket.set_sndtimeo(1000)?;
        socket.connect(&self.endpoint)?;
        Ok(socket)
    }
}

struct Session {
    uid: String,
    admin: bool,
    last_seen: Instant,
}

#[derive(Default)]
struct Counters {
    inserted: AtomicU64,
    leased: AtomicU64,
    completed: AtomicU64,
    cancelled: AtomicU64,
}

struct Shared {
    api: Arc<ServerAPI>,
    server_id: u16,
    sessions: DashMap<Vec<u8>, Session>,
    counters: Counters,
    started: Instant,
    replies: ReplyPool,
    inflight: AtomicUsize,
    max_inflight: usize,
}

fn not_authenticated() -> Response {
    Response::Error(WireError {
        kind: ErrorKind::InvalidArgument,
        message: "NotAuthenticated".into(),
    })
}

fn add(counter: &AtomicU64, n: usize) {
    counter.fetch_add(n as u64, Ordering::Relaxed);
}

impl Shared {
    fn saturated(&self) -> bool {
        self.inflight.load(Ordering::SeqCst) >= self.max_inflight
    }

    /// Refuses to act on a lease held by another uid. Tokens come from a
    /// counter, so they fence stale holders but can be guessed; ownership is
    /// what stops one worker from renewing, completing or cancelling another's
    /// lease. A (task_id, token) pair names one lease whose holder never
    /// changes, so this check can't be raced by reaping or re-leasing. Leases
    /// that are gone or held under another token pass through: the nucleus op
    /// rejects them (or treats a `complete` as an idempotent retry).
    fn check_owner(&self, lease: &LeaseRef, uid: &str) -> Result<(), Error> {
        let not_owned = self
            .api
            .leased_tasks
            .leases(&lease.task_type)
            .and_then(|leases| {
                leases
                    .get(&lease.task_id)
                    .map(|held| held.token == lease.token && held.user_id != uid)
            })
            .unwrap_or(false);
        if not_owned {
            return Err(Error::invalid_argument("LeaseNotOwned"));
        }
        Ok(())
    }

    async fn handle(self: &Arc<Self>, identity: &[u8], request: Request) -> Response {
        if let Request::Hello {
            proto_ver, auth, ..
        } = request
        {
            return self.hello(identity, proto_ver, auth);
        }

        // Every other request needs a session; refresh its idle timer.
        let (uid, admin) = match self.sessions.get_mut(identity) {
            Some(mut session) => {
                session.last_seen = Instant::now();
                (session.uid.clone(), session.admin)
            }
            None => return not_authenticated(),
        };

        match request {
            Request::Hello { .. } => unreachable!("handled above"),
            Request::Lease {
                task_type,
                max_tasks,
                wait_ms,
            } => match self
                .lease(
                    task_type,
                    uid,
                    max_tasks,
                    Duration::from_millis(wait_ms.into()),
                )
                .await
            {
                Ok(leased) => Response::Leased(leased),
                Err(err) => Response::Error(err.into()),
            },
            Request::Renew { leases } => Response::Renewed {
                results: leases
                    .into_iter()
                    .map(|lease| {
                        self.check_owner(&lease, &uid)?;
                        renew(
                            self.api.clone(),
                            lease.task_type,
                            &lease.task_id,
                            lease.token,
                        )
                        .map(wire::unix_ms)
                    })
                    .map(|result| result.map_err(Into::into))
                    .collect(),
            },
            Request::Complete {
                results,
                then_lease,
            } => {
                let this = self.clone();
                let owner = uid.clone();
                let results =
                    match tokio::task::spawn_blocking(move || this.complete(results, &owner)).await
                    {
                        Ok(results) => results,
                        Err(err) => return Response::Error(Error::new(err.to_string()).into()),
                    };
                // The next lease never waits: an empty queue just means no `next`.
                let next = match then_lease {
                    Some(req) => self
                        .lease(req.task_type, uid, req.max_tasks, Duration::ZERO)
                        .await
                        .ok(),
                    None => None,
                };
                Response::Completed { results, next }
            }
            Request::CancelLease { leases } => {
                let results: Vec<Result<(), WireError>> = leases
                    .into_iter()
                    .map(|lease| {
                        self.check_owner(&lease, &uid)?;
                        let LeaseRef {
                            task_type,
                            task_id,
                            token,
                        } = lease;
                        cancel(self.api.clone(), task_type, task_id, token)
                    })
                    .map(|result| result.map_err(Into::into))
                    .collect();
                add(
                    &self.counters.cancelled,
                    results.iter().filter(|result| result.is_ok()).count(),
                );
                Response::Cancelled { results }
            }
            Request::Query(query) => {
                let this = self.clone();
                match tokio::task::spawn_blocking(move || this.query(query, admin)).await {
                    Ok(Ok(response)) => Response::Query(response),
                    Ok(Err(err)) => Response::Error(err.into()),
                    Err(err) => Response::Error(Error::new(err.to_string()).into()),
                }
            }
        }
    }

    fn hello(&self, identity: &[u8], proto_ver: u16, auth: Auth) -> Response {
        if proto_ver != PROTO_VERSION {
            return Response::Error(WireError {
                kind: ErrorKind::NotSupported,
                message: format!("ProtocolVersion: server speaks {PROTO_VERSION}"),
            });
        }
        let api = &self.api;
        if !Events::CheckAuth(api, auth.uid.clone(), auth.challenge, api.db.clone()) {
            return Response::Error(WireError {
                kind: ErrorKind::InvalidArgument,
                message: "AuthFailed".into(),
            });
        }
        // Only a client that presents a token is ever considered for admin, even
        // though an unset `auth_token` makes the admin check itself permissive.
        let admin = auth.admin_token.is_some_and(|token| {
            Events::CheckAdminAuth(
                api,
                token,
                ("core".to_string(), "admin".to_string()),
                api.db.clone(),
            )
        });
        self.sessions.insert(
            identity.to_vec(),
            Session {
                uid: auth.uid,
                admin,
                last_seen: Instant::now(),
            },
        );
        Response::Welcome {
            proto_ver: PROTO_VERSION,
            server_id: self.server_id,
            lease_ttl_secs: api.cfg.config_toml.lease_ttl_secs,
            task_types: self
                .task_types()
                .into_iter()
                .map(|task_type| TaskTypeInfo { task_type })
                .collect(),
        }
    }

    /// Leases up to `max_tasks`, waiting at most `wait` for the first one only.
    async fn lease(
        &self,
        task_type: Identifier,
        uid: String,
        max_tasks: u32,
        wait: Duration,
    ) -> Result<Leased, Error> {
        let max_tasks = max_tasks.clamp(1, MAX_LEASE_BATCH) as usize;
        let first = lease_timeout(self.api.clone(), task_type.clone(), uid.clone(), wait).await?;
        let mut expires_at = first.expires_at;
        let mut tasks = Vec::with_capacity(max_tasks.min(1024));
        let mut push = |lease: crate::api::Lease| {
            expires_at = expires_at.min(lease.expires_at);
            tasks.push(LeasedItem {
                task_id: lease.task.task_id.clone(),
                token: lease.token,
                bytes: lease.task.bytes.clone(),
            });
        };
        push(first);
        // The rest only take what is already queued. A zero timeout still polls
        // the queue once before giving up.
        for _ in 1..max_tasks {
            match lease_timeout(
                self.api.clone(),
                task_type.clone(),
                uid.clone(),
                Duration::ZERO,
            )
            .await
            {
                Ok(lease) => push(lease),
                Err(_) => break,
            }
        }
        add(&self.counters.leased, tasks.len());
        Ok(Leased {
            task_type,
            expires_at_ms: wire::unix_ms(expires_at),
            tasks,
        })
    }

    fn complete(&self, results: Vec<TaskResult>, uid: &str) -> Vec<Result<(), WireError>> {
        let results: Vec<Result<(), WireError>> = results
            .into_iter()
            .map(|TaskResult { lease, bytes }| {
                self.check_owner(&lease, uid)?;
                complete(
                    self.api.clone(),
                    lease.task_type,
                    lease.task_id,
                    lease.token,
                    &bytes,
                )
            })
            .map(|result| result.map_err(Into::into))
            .collect();
        add(
            &self.counters.completed,
            results.iter().filter(|result| result.is_ok()).count(),
        );
        results
    }

    fn query(&self, query: Query, admin: bool) -> Result<QueryResponse, Error> {
        let api = &self.api;
        match query {
            Query::Insert {
                task_type,
                payloads,
            } => {
                let results: Vec<Result<String, WireError>> =
                    submit_batch(api.clone(), &payloads, task_type)?
                        .into_iter()
                        .map(|result| result.map_err(Into::into))
                        .collect();
                add(
                    &self.counters.inserted,
                    results.iter().filter(|result| result.is_ok()).count(),
                );
                Ok(QueryResponse::Ids(results))
            }
            Query::Get { task_type, ids } => {
                self.require_type(&task_type)?;
                let leases = api.leased_tasks.leases(&task_type);
                let records = ids
                    .into_iter()
                    .map(|task_id| {
                        if let Some(result) = self.get(finished_task_key(&task_type, &task_id))? {
                            return Ok(TaskRecord {
                                task_id,
                                status: TaskStatus::Finished,
                                bytes: Some(result),
                            });
                        }
                        if let Some(lease) = leases.as_ref().and_then(|leases| {
                            leases
                                .get(&task_id)
                                .map(|lease| lease.stored_task.bytes.clone())
                        }) {
                            return Ok(TaskRecord {
                                task_id,
                                status: TaskStatus::Leased,
                                bytes: Some(lease),
                            });
                        }
                        if let Some(pending) = self.get(task_key(&task_type, &task_id))? {
                            return Ok(TaskRecord {
                                task_id,
                                status: TaskStatus::Pending,
                                bytes: Some(pending),
                            });
                        }
                        // `complete` drops the lease, then swaps `t:` for `f:` in
                        // one batch. A task that finished after the first check
                        // has no lease or pending record any more but already has
                        // its finished record, so look once more before Unknown.
                        let finished = self.get(finished_task_key(&task_type, &task_id))?;
                        Ok(TaskRecord {
                            task_id,
                            status: if finished.is_some() {
                                TaskStatus::Finished
                            } else {
                                TaskStatus::Unknown
                            },
                            bytes: finished,
                        })
                    })
                    .collect::<Result<Vec<_>, Error>>()?;
                Ok(QueryResponse::Records(records))
            }
            Query::Scan {
                task_type,
                scope,
                after,
                limit,
            } => {
                self.require_type(&task_type)?;
                let prefix = scope_prefix(&task_type, scope);
                let after = after.map(|task_id| format!("{prefix}{task_id}"));
                let limit = limit.clamp(1, MAX_SCAN_LIMIT) as usize;
                let QueryResult::Page(page) = db_query::query(
                    api.clone(),
                    DbQuery::Scan {
                        prefix,
                        after,
                        limit,
                    },
                )?
                else {
                    return Err(Error::new("Unexpected scan result".into()));
                };
                let id_of = |key: &str| task_id_from_key(key).unwrap_or_default().to_string();
                Ok(QueryResponse::Page {
                    records: page
                        .records
                        .into_iter()
                        .map(|record| PageRecord {
                            task_id: id_of(&record.key),
                            bytes: record.value,
                        })
                        .collect(),
                    next: page.next.as_deref().map(id_of),
                })
            }
            Query::EstimateKeys => match db_query::query(api.clone(), DbQuery::EstimateKeys)? {
                QueryResult::EstimateKeys(keys) => Ok(QueryResponse::Estimate(keys)),
                _ => Err(Error::new("Unexpected estimate result".into())),
            },
            Query::EstimateSize { task_type, scope } => {
                self.require_type(&task_type)?;
                let prefix = scope_prefix(&task_type, scope);
                match db_query::query(api.clone(), DbQuery::EstimateSize { prefix })? {
                    QueryResult::EstimateSize(size) => Ok(QueryResponse::Estimate(size)),
                    _ => Err(Error::new("Unexpected estimate result".into())),
                }
            }
            Query::Delete {
                task_type,
                scope,
                ids,
            } => {
                require_admin(admin)?;
                self.require_type(&task_type)?;
                match scope {
                    Scope::Pending => self.delete_pending(&task_type, &ids)?,
                    Scope::Finished => {
                        let mut batch = WriteBatch::default();
                        for task_id in &ids {
                            batch.delete(finished_task_key(&task_type, task_id));
                        }
                        write(api, batch)?;
                    }
                }
                Ok(QueryResponse::Done)
            }
            Query::Purge { task_type, scope } => {
                require_admin(admin)?;
                self.require_type(&task_type)?;
                match scope {
                    Scope::Pending => self.purge_pending(&task_type)?,
                    Scope::Finished => {
                        let prefix = scope_prefix(&task_type, scope);
                        db_query::query(api.clone(), DbQuery::PurgePrefix { prefix })?;
                    }
                }
                Ok(QueryResponse::Done)
            }
            Query::Stats => Ok(QueryResponse::Stats(self.stats())),
        }
    }

    /// Deletes the pending records of `ids`, refusing the whole request if any
    /// is queued or leased: those must go through cancel/complete, and deleting
    /// their record underneath would lose the task on reap or restart. Runs with
    /// refill excluded, so no id can be queued between the check and the delete.
    fn delete_pending(&self, task_type: &Identifier, ids: &[String]) -> Result<(), Error> {
        let api = &self.api;
        // Same lock order as `refill`: queue map guard, then the refill lock.
        let queue = api.task_queue.tasks.get(task_type);
        let dedup = queue.as_ref().map(|queue| &queue.2);
        api.load_state.get(task_type).exclusive(|| {
            if let Some(task_id) = ids
                .iter()
                .find(|task_id| dedup.is_some_and(|dedup| dedup.contains(*task_id)))
            {
                return Err(Error::invalid_argument(format!("TaskInFlight: {task_id}")));
            }
            let mut batch = WriteBatch::default();
            for task_id in ids {
                batch.delete(task_key(task_type, task_id));
            }
            write(api, batch)
        })
    }

    /// Deletes every pending record of `task_type` that isn't queued or leased;
    /// in-flight tasks keep their records and finish normally. Walks the prefix
    /// a page at a time, each page with refill excluded (see `delete_pending`).
    fn purge_pending(&self, task_type: &Identifier) -> Result<(), Error> {
        let api = &self.api;
        let state = api.load_state.get(task_type);
        let prefix = task_key_prefix(task_type);
        let mut after = None;
        loop {
            let queue = api.task_queue.tasks.get(task_type);
            let dedup = queue.as_ref().map(|queue| &queue.2);
            let next = state.exclusive(|| {
                let QueryResult::Page(page) = db_query::query(
                    api.clone(),
                    DbQuery::Scan {
                        prefix: prefix.clone(),
                        after: after.clone(),
                        limit: PURGE_PAGE,
                    },
                )?
                else {
                    return Err(Error::new("Unexpected scan result".into()));
                };
                let mut batch = WriteBatch::default();
                for record in &page.records {
                    let in_flight = task_id_from_key(&record.key)
                        .is_some_and(|id| dedup.is_some_and(|dedup| dedup.contains(id)));
                    if !in_flight {
                        batch.delete(record.key.as_bytes());
                    }
                }
                write(api, batch)?;
                Ok(page.next)
            })?;
            match next {
                Some(next) => after = Some(next),
                None => return Ok(()),
            }
        }
    }

    fn get(&self, key: String) -> Result<Option<Vec<u8>>, Error> {
        match db_query::query(self.api.clone(), DbQuery::Get { key })? {
            QueryResult::Record(record) => Ok(record.map(|record| record.value)),
            _ => Err(Error::new("Unexpected get result".into())),
        }
    }

    /// Typed queries only accept registered task types, so a crafted namespace
    /// or name can't build a key prefix that reaches another type's records.
    fn require_type(&self, task_type: &Identifier) -> Result<(), Error> {
        self.api
            .task_registry
            .get(task_type)
            .map(|_| ())
            .ok_or(Error::not_found("TaskTypeNotFound"))
    }

    fn task_types(&self) -> Vec<Identifier> {
        let mut types: Vec<Identifier> = self
            .api
            .task_registry
            .tasks
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        types.sort();
        types
    }

    fn queued(&self) -> u64 {
        self.api
            .task_queue
            .tasks
            .iter()
            .map(|queue| queue.1.len() as u64)
            .sum()
    }

    fn active_leases(&self) -> u64 {
        self.api
            .leased_tasks
            .tasks
            .iter()
            .map(|leases| leases.len() as u64)
            .sum()
    }

    fn load(&self) -> u64 {
        self.queued() + self.active_leases()
    }

    fn stats(&self) -> Stats {
        let counters = &self.counters;
        Stats {
            server_id: self.server_id,
            uptime_ms: self.started.elapsed().as_millis() as u64,
            inserted: counters.inserted.load(Ordering::Relaxed),
            leased: counters.leased.load(Ordering::Relaxed),
            completed: counters.completed.load(Ordering::Relaxed),
            cancelled: counters.cancelled.load(Ordering::Relaxed),
            queued: self.queued(),
            active_leases: self.active_leases(),
            sessions: self.sessions.len() as u64,
        }
    }
}

fn scope_prefix(task_type: &Identifier, scope: Scope) -> String {
    match scope {
        Scope::Pending => task_key_prefix(task_type),
        Scope::Finished => finished_task_key_prefix(task_type),
    }
}

fn write(api: &ServerAPI, batch: WriteBatch) -> Result<(), Error> {
    api.db
        .write(&batch)
        .map_err(|err| Error::io_error(format!("Failed to delete records: {err}")))
}

fn require_admin(admin: bool) -> Result<(), Error> {
    if admin {
        Ok(())
    } else {
        Err(Error::invalid_argument("AdminRequired"))
    }
}
