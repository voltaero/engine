//! Discovery proxy: tells clients which server to use. Servers register with a
//! heartbeat authenticated by the shared `proxy_token`; clients `Resolve` when
//! they connect (or fail over), so the proxy is never on the task hot path.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tracing::{error, info, warn};

use super::{Background, POLL_MS, constant_time_eq, context, zmq_err};
use crate::config::ConfigTomlServer;
use crate::error::{Error, ErrorKind};
use crate::wire::{self, ProxyRequest, ProxyResponse, ServerInfo, WireError};

#[derive(Debug, Clone)]
pub struct ProxyOptions {
    pub endpoint: String,
    /// How long clients may cache a `Resolved` answer.
    pub ttl_secs: u32,
    /// A server that hasn't heartbeated for this long is no longer handed out.
    pub server_expiry: Duration,
    /// Shared secret a server must present in `Register`. `None` (or empty) refuses every
    /// registration: otherwise anyone who can reach the proxy could advertise
    /// their own endpoint and collect clients' `Hello` credentials.
    pub token: Option<String>,
}

impl ProxyOptions {
    pub fn from_config(cfg: &ConfigTomlServer) -> Self {
        let heartbeat = cfg.heartbeat_secs.max(1);
        Self {
            endpoint: wire::endpoint(&cfg.proxy_host),
            ttl_secs: 30,
            server_expiry: Duration::from_secs(heartbeat * 3),
            token: cfg.proxy_token.clone(),
        }
    }
}

pub struct Proxy;

impl Proxy {
    pub fn start(opts: ProxyOptions) -> Result<Background, Error> {
        let router = context()
            .socket(zmq::ROUTER)
            .map_err(|err| zmq_err("Failed to create ROUTER", err))?;
        router
            .set_ipv6(true)
            .map_err(|err| zmq_err("set_ipv6", err))?;
        router
            .set_linger(0)
            .map_err(|err| zmq_err("set_linger", err))?;
        // Discovery messages are tiny; refuse anything that isn't.
        router
            .set_maxmsgsize(64 * 1024)
            .map_err(|err| zmq_err("set_maxmsgsize", err))?;
        router
            .bind(&opts.endpoint)
            .map_err(|err| zmq_err(&format!("Failed to bind {}", opts.endpoint), err))?;
        let bound = match router.get_last_endpoint() {
            Ok(Ok(endpoint)) => endpoint,
            _ => opts.endpoint.clone(),
        };

        if opts.token.as_deref().is_none_or(str::is_empty) {
            error!("`proxy_token` is unset: every server registration will be refused");
        }
        let stop = Arc::new(AtomicBool::new(false));
        let thread = {
            let stop = stop.clone();
            std::thread::Builder::new()
                .name("engine-proxy".into())
                .spawn(move || proxy_loop(&router, &opts, &stop))
                .map_err(|err| Error::io_error(format!("Failed to spawn proxy thread: {err}")))?
        };
        info!("Proxy listening on {bound}");
        Ok(Background::new(bound, stop, vec![thread]))
    }
}

fn proxy_loop(router: &zmq::Socket, opts: &ProxyOptions, stop: &AtomicBool) {
    let mut servers: HashMap<u16, (ServerInfo, Instant)> = HashMap::new();
    while !stop.load(Ordering::SeqCst) {
        let mut items = [router.as_poll_item(zmq::POLLIN)];
        if let Err(err) = zmq::poll(&mut items, POLL_MS) {
            error!("zmq poll failed: {err}");
            return;
        }
        if !items[0].is_readable() {
            continue;
        }
        while let Ok(frames) = router.recv_multipart(zmq::DONTWAIT) {
            let mut frames = frames.into_iter();
            let (Some(identity), Some(payload)) = (frames.next(), frames.next()) else {
                continue;
            };
            let (req_id, response) = match wire::decode::<ProxyRequest>(&payload) {
                Ok(envelope) => (envelope.req_id, handle(&mut servers, opts, envelope.body)),
                Err(err) => (0, ProxyResponse::Error(err.into())),
            };
            match wire::encode(req_id, &response) {
                Ok(bytes) => {
                    if let Err(err) = router.send_multipart([identity, bytes], zmq::DONTWAIT) {
                        warn!("Dropped proxy reply: {err}");
                    }
                }
                Err(err) => error!("Failed to encode proxy reply: {err}"),
            }
        }
    }
}

fn handle(
    servers: &mut HashMap<u16, (ServerInfo, Instant)>,
    opts: &ProxyOptions,
    request: ProxyRequest,
) -> ProxyResponse {
    match request {
        ProxyRequest::Register { info, token } => {
            let authorized = opts
                .token
                .as_deref()
                .filter(|expected| !expected.is_empty())
                .is_some_and(|expected| constant_time_eq(expected.as_bytes(), token.as_bytes()));
            if !authorized {
                warn!(
                    "Refused registration of server {} ({})",
                    info.server_id, info.endpoint
                );
                return ProxyResponse::Error(WireError {
                    kind: ErrorKind::InvalidArgument,
                    message: "RegistrationRefused".into(),
                });
            }
            servers.insert(info.server_id, (info, Instant::now()));
            ProxyResponse::Registered
        }
        // Forwarded auth (issuing `ticket`) is left to a mod; the core proxy
        // only routes and issues none.
        ProxyRequest::Resolve { task_type, .. } => {
            servers.retain(|_, (_, seen)| seen.elapsed() < opts.server_expiry);
            let mut candidates: Vec<ServerInfo> = servers
                .values()
                .filter(|(info, _)| {
                    task_type
                        .as_ref()
                        .is_none_or(|task_type| info.task_types.contains(task_type))
                })
                .map(|(info, _)| info.clone())
                .collect();
            candidates.sort_by_key(|info| (info.load, info.server_id));
            ProxyResponse::Resolved {
                servers: candidates,
                ticket: None,
                ttl_secs: opts.ttl_secs,
            }
        }
    }
}
