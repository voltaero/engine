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
use tokio::runtime::Handle;
use tracing::{debug, error, info, warn};

use super::{Background, POLL_MS, constant_time_eq, context, sleep_unless_stopped, zmq_err};
use crate::api::{ServerAPI, lock_lease};
use crate::config::ConfigTomlServer;
use crate::error::{Error, ErrorKind};
use crate::events::Events;
use crate::nucleus::query::{
    self as db_query, Query as DbQuery, QueryResult, delete_tasks, get_tasks, purge_tasks,
    scan_tasks, scope_prefix,
};
use crate::nucleus::{
    cancel::cancel_batch,
    complete::{CompleteItem, complete_batch},
    lease::lease_many,
    renew::renew_batch,
    submit::submit_batch,
};
use crate::wire::{
    self, Auth, LeaseRef, Leased, LeasedItem, MAX_LEASE_BATCH, MAX_LEASE_WAIT_MS, MAX_SCAN_LIMIT,
    PROTO_VERSION, ProxyRequest, ProxyResponse, Query, QueryResponse, Request, Response,
    ServerInfo, Stats, TaskResult, TaskTypeInfo, WireError,
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
/// Default cap on one session's requests being handled at once (see
/// [`ServerOptions::max_inflight_per_session`]).
pub const DEFAULT_MAX_INFLIGHT_PER_SESSION: usize = 1024;

#[derive(Debug, Clone)]
pub struct ServerOptions {
    /// ZeroMQ endpoint to bind, e.g. `tcp://[::1]:50051` or `tcp://127.0.0.1:*`.
    pub endpoint: String,
    /// Announced in `Welcome`/to the proxy. Keep it equal to the API config's
    /// `server_id`, which task ids embed (`from_config` does).
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
    /// Most requests one session may have in flight; beyond it requests are
    /// refused with `TooManyRequests` instead of parking, so one client can't
    /// take the whole `max_inflight` budget (e.g. with waiting `Lease`s).
    pub max_inflight_per_session: usize,
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
            max_inflight_per_session: DEFAULT_MAX_INFLIGHT_PER_SESSION,
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
            max_inflight_per_session: opts.max_inflight_per_session.max(1),
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
                shared
                    .handle(&identity, envelope.session, envelope.body)
                    .await,
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
    /// Random key handed out in `Welcome`; every later request must carry it.
    key: u128,
    uid: String,
    admin: bool,
    last_seen: Instant,
    /// This session's requests currently being handled.
    inflight: Arc<AtomicUsize>,
}

/// One request's share of a session's in-flight budget; released on drop.
struct SessionSlot(Arc<AtomicUsize>);
impl SessionSlot {
    fn acquire(counter: &Arc<AtomicUsize>, max: usize) -> Option<Self> {
        if counter.fetch_add(1, Ordering::SeqCst) >= max {
            counter.fetch_sub(1, Ordering::SeqCst);
            return None;
        }
        Some(Self(counter.clone()))
    }
}
impl Drop for SessionSlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
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
    max_inflight_per_session: usize,
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
        // Leases are per chunk: every task of a chunk lease shares its holder.
        // Malformed / foreign ids pass through for the nucleus op to reject.
        let Ok(id) = self.api.parse_task_id(&lease.task_id) else {
            return Ok(());
        };
        let not_owned = self
            .api
            .leased_tasks
            .cell(&lease.task_type, id.chunk_seq)
            .is_some_and(|cell| {
                let held = lock_lease(&cell);
                !held.gone && held.token == lease.token && held.user_id != uid
            });
        if not_owned {
            return Err(Error::invalid_argument("LeaseNotOwned"));
        }
        Ok(())
    }

    /// Runs the batch op `op` on the items whose lease `uid` may act on and
    /// answers the rest with `LeaseNotOwned`; one result per item, in order.
    fn owned_batch<I, T>(
        &self,
        uid: &str,
        items: Vec<I>,
        lease_of: impl Fn(&I) -> &LeaseRef,
        op: impl FnOnce(Vec<I>) -> Vec<Result<T, Error>>,
    ) -> Vec<Result<T, Error>> {
        let checks: Vec<Result<(), Error>> = items
            .iter()
            .map(|item| self.check_owner(lease_of(item), uid))
            .collect();
        let allowed: Vec<I> = items
            .into_iter()
            .zip(&checks)
            .filter(|(_, check)| check.is_ok())
            .map(|(item, _)| item)
            .collect();
        let mut results = op(allowed).into_iter();
        checks
            .into_iter()
            .map(|check| match check {
                Ok(()) => results
                    .next()
                    .unwrap_or_else(|| Err(Error::new("batch op returned too few results".into()))),
                Err(err) => Err(err),
            })
            .collect()
    }

    async fn handle(
        self: &Arc<Self>,
        identity: &[u8],
        session_key: u128,
        request: Request,
    ) -> Response {
        if let Request::Hello {
            proto_ver, auth, ..
        } = request
        {
            return self.hello(identity, proto_ver, auth);
        }

        // Every other request needs a session; refresh its idle timer. The
        // routing identity is peer-chosen (a peer can reuse a disconnected
        // client's), so the session key from `Welcome` must match too.
        let (uid, admin, inflight) = match self.sessions.get_mut(identity) {
            Some(mut session)
                if constant_time_eq(&session.key.to_le_bytes(), &session_key.to_le_bytes()) =>
            {
                session.last_seen = Instant::now();
                (session.uid.clone(), session.admin, session.inflight.clone())
            }
            _ => return not_authenticated(),
        };
        let Some(_slot) = SessionSlot::acquire(&inflight, self.max_inflight_per_session) else {
            return Response::Error(WireError {
                kind: ErrorKind::InvalidArgument,
                message: "TooManyRequests".into(),
            });
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
                    Duration::from_millis(wait_ms.min(MAX_LEASE_WAIT_MS).into()),
                )
                .await
            {
                Ok(leased) => Response::Leased(leased),
                Err(err) => Response::Error(err.into()),
            },
            Request::Renew { leases } => Response::Renewed {
                results: self
                    .owned_batch(
                        &uid,
                        leases,
                        |lease| lease,
                        |leases| renew_batch(&self.api, leases.into_iter().map(lease_ref_parts)),
                    )
                    .into_iter()
                    .map(|result| result.map(wire::unix_ms).map_err(Into::into))
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
                let results: Vec<Result<(), WireError>> = self
                    .owned_batch(
                        &uid,
                        leases,
                        |lease| lease,
                        |leases| cancel_batch(&self.api, leases.into_iter().map(lease_ref_parts)),
                    )
                    .into_iter()
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
        // Only a client that presents a token is ever considered for admin. The
        // core handler grants it only for a configured, matching `auth_token`.
        let admin = auth.admin_token.is_some_and(|token| {
            Events::CheckAdminAuth(
                api,
                token,
                ("core".to_string(), "admin".to_string()),
                api.db.clone(),
            )
        });
        // Re-handshaking under an identity replaces its session, so a peer
        // reusing someone's identity can at worst force that client to `Hello`
        // again (it does so automatically); it never inherits the session.
        let key: u128 = rand::random();
        self.sessions.insert(
            identity.to_vec(),
            Session {
                key,
                uid: auth.uid,
                admin,
                last_seen: Instant::now(),
                inflight: Arc::new(AtomicUsize::new(0)),
            },
        );
        Response::Welcome {
            proto_ver: PROTO_VERSION,
            server_id: self.server_id,
            session: key,
            lease_ttl_secs: api.cfg.config_toml.lease_ttl_secs,
            task_types: self
                .task_types()
                .into_iter()
                .map(|task_type| {
                    let settings = api.settings(&task_type);
                    TaskTypeInfo {
                        task_type,
                        chunk_size: settings.chunk_size,
                        max_payload: settings.max_payload,
                        max_result: settings.max_result,
                    }
                })
                .collect(),
        }
    }

    /// Leases whole chunks until at least `max_tasks` tasks, waiting at most
    /// `wait` for the first chunk only.
    async fn lease(
        &self,
        task_type: Identifier,
        uid: String,
        max_tasks: u32,
        wait: Duration,
    ) -> Result<Leased, Error> {
        let max_tasks = max_tasks.clamp(1, MAX_LEASE_BATCH) as usize;
        let leases = lease_many(self.api.clone(), task_type.clone(), uid, max_tasks, wait).await?;
        let mut expires_at = chrono::DateTime::<chrono::Utc>::MAX_UTC;
        let mut tasks = Vec::with_capacity(leases.iter().map(|lease| lease.len()).sum());
        for lease in &leases {
            expires_at = expires_at.min(lease.expires_at);
            tasks.extend(lease.tasks().map(|(task_id, bytes)| LeasedItem {
                task_id,
                token: lease.token,
                bytes: bytes.to_vec(),
            }));
        }
        add(&self.counters.leased, tasks.len());
        Ok(Leased {
            task_type,
            expires_at_ms: wire::unix_ms(expires_at),
            tasks,
        })
    }

    fn complete(&self, results: Vec<TaskResult>, uid: &str) -> Vec<Result<(), WireError>> {
        let results: Vec<Result<(), WireError>> = self
            .owned_batch(
                uid,
                results,
                |result| &result.lease,
                |results| {
                    let items = results
                        .into_iter()
                        .map(|TaskResult { lease, bytes }| CompleteItem {
                            task_type: lease.task_type,
                            task_id: lease.task_id,
                            lease_token: lease.token,
                            bytes,
                        })
                        .collect();
                    complete_batch(&self.api, items)
                },
            )
            .into_iter()
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
                Ok(QueryResponse::Records(get_tasks(api, &task_type, ids)?))
            }
            Query::Scan {
                task_type,
                scope,
                after,
                limit,
            } => {
                self.require_type(&task_type)?;
                let limit = limit.clamp(1, MAX_SCAN_LIMIT) as usize;
                let (records, next) = scan_tasks(api, &task_type, scope, after.as_deref(), limit)?;
                Ok(QueryResponse::Page { records, next })
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
                delete_tasks(api, &task_type, scope, &ids)?;
                Ok(QueryResponse::Done)
            }
            Query::Purge { task_type, scope } => {
                require_admin(admin)?;
                self.require_type(&task_type)?;
                purge_tasks(api, &task_type, scope)?;
                Ok(QueryResponse::Done)
            }
            Query::Stats => Ok(QueryResponse::Stats(self.stats())),
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
            .map(|queue| queue.queued_tasks() as u64)
            .sum()
    }

    /// Open tasks across live leases (leases are per chunk; this counts tasks).
    fn active_leases(&self) -> u64 {
        self.api.leased_tasks.open_tasks() as u64
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

fn lease_ref_parts(lease: LeaseRef) -> (Identifier, String, u64) {
    (lease.task_type, lease.task_id, lease.token)
}

fn require_admin(admin: bool) -> Result<(), Error> {
    if admin {
        Ok(())
    } else {
        Err(Error::invalid_argument("AdminRequired"))
    }
}
