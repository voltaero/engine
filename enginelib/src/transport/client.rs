//! Blocking ZeroMQ client (DEALER socket) for workers, submitters and operators.
//!
//! One `Client` per thread: ZeroMQ sockets are not thread-safe. [`Client::call`]
//! is request/response; [`Client::send`] + [`Client::recv`] let a caller keep
//! several requests in flight on one connection (responses carry the `req_id`).

use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};

use super::{context, zmq_err};
use crate::Identifier;
use crate::error::{Error, ErrorKind};
use crate::wire::{
    self, Auth, Caps, Envelope, LeaseRef, LeaseReq, Leased, PROTO_VERSION, ProxyRequest,
    ProxyResponse, Query, QueryResponse, Request, Response, Results, ServerInfo, Stats, TaskResult,
    TaskTypeInfo,
};

/// Default time to wait for a response, on top of any server-side `wait_ms`.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// What the server said in `Welcome`.
#[derive(Debug, Clone)]
pub struct ServerHello {
    pub server_id: u16,
    pub lease_ttl_secs: u64,
    pub task_types: Vec<TaskTypeInfo>,
}

pub struct Client {
    socket: zmq::Socket,
    endpoint: String,
    auth: Auth,
    caps: Caps,
    next_req_id: u64,
    /// Session key from the last `Welcome`; sent with every request.
    session: u128,
    /// `req_id`s issued by [`Client::send`] whose reply `recv` hasn't returned.
    outstanding: HashSet<u64>,
    /// Replies to `outstanding` requests that arrived while `call` waited for
    /// its own; `recv` hands them out first.
    buffered: VecDeque<Envelope<Response>>,
    pub timeout: Duration,
    pub server: ServerHello,
}

impl Client {
    /// Connects to a server and performs the `Hello` handshake.
    pub fn connect(endpoint: &str, auth: Auth, caps: Caps) -> Result<Self, Error> {
        Self::connect_timeout(endpoint, auth, caps, DEFAULT_TIMEOUT)
    }

    /// Like [`Client::connect`], with `timeout` as the handshake deadline and as
    /// the default response timeout afterwards.
    pub fn connect_timeout(
        endpoint: &str,
        auth: Auth,
        caps: Caps,
        timeout: Duration,
    ) -> Result<Self, Error> {
        let endpoint = wire::endpoint(endpoint);
        let socket = dealer(&endpoint)?;
        let mut client = Self {
            socket,
            endpoint,
            auth,
            caps,
            next_req_id: 0,
            session: 0,
            outstanding: HashSet::new(),
            buffered: VecDeque::new(),
            timeout,
            server: ServerHello {
                server_id: 0,
                lease_ttl_secs: 0,
                task_types: Vec::new(),
            },
        };
        client.hello()?;
        Ok(client)
    }

    /// Asks the proxy which server to use, then connects to the first candidate
    /// that accepts the handshake. A proxy-issued ticket, if any, replaces
    /// `auth.challenge`. Resolves once: the client keeps no discovery cache, so
    /// to fail over, call this again when the server stops answering.
    pub fn connect_via_proxy(
        proxy: &str,
        task_type: Option<Identifier>,
        mut auth: Auth,
        caps: Caps,
    ) -> Result<Self, Error> {
        let (servers, ticket) = resolve(proxy, task_type, &auth, DEFAULT_TIMEOUT)?;
        if let Some(ticket) = ticket {
            auth.challenge = String::from_utf8_lossy(&ticket).into_owned();
        }
        let mut last = Error::not_found("NoServerAvailable");
        for server in servers {
            match Self::connect(&server.endpoint, auth.clone(), caps.clone()) {
                Ok(client) => return Ok(client),
                Err(err) => last = err,
            }
        }
        Err(last)
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn hello(&mut self) -> Result<(), Error> {
        let request = Request::Hello {
            proto_ver: PROTO_VERSION,
            auth: self.auth.clone(),
            caps: self.caps.clone(),
        };
        // A stale key must not ride along on the handshake itself.
        self.session = 0;
        match self.call_once(&request, self.timeout)? {
            Response::Welcome {
                server_id,
                session,
                lease_ttl_secs,
                task_types,
                ..
            } => {
                self.session = session;
                self.server = ServerHello {
                    server_id,
                    lease_ttl_secs,
                    task_types,
                };
                Ok(())
            }
            other => Err(unexpected(other)),
        }
    }

    /// Sends a request without waiting; returns its `req_id`. Collect the reply
    /// with [`Client::recv`]; `call`s made in between keep it for `recv`.
    pub fn send(&mut self, request: &Request) -> Result<u64, Error> {
        let req_id = self.send_raw(request, Instant::now() + self.timeout)?;
        self.outstanding.insert(req_id);
        Ok(req_id)
    }

    /// Queues one request, giving up at `deadline` if the socket stays full
    /// (an unreachable or stalled server) instead of blocking forever.
    fn send_raw(&mut self, request: &Request, deadline: Instant) -> Result<u64, Error> {
        self.next_req_id += 1;
        let req_id = self.next_req_id;
        let bytes = wire::encode_in_session(req_id, self.session, request)?;
        loop {
            match self.socket.send(&bytes, zmq::DONTWAIT) {
                Ok(()) => return Ok(req_id),
                Err(zmq::Error::EAGAIN) => {}
                Err(err) => return Err(zmq_err("Failed to send request", err)),
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(Error::io_error("Timed out sending to the server"));
            }
            let mut items = [self.socket.as_poll_item(zmq::POLLOUT)];
            zmq::poll(&mut items, poll_ms(left)).map_err(|err| zmq_err("Failed to poll", err))?;
        }
    }

    /// Receives the next response to a request issued by [`Client::send`],
    /// whichever one it answers. Late replies to `call`s that timed out are
    /// skipped.
    pub fn recv(&mut self, timeout: Duration) -> Result<Envelope<Response>, Error> {
        if let Some(envelope) = self.buffered.pop_front() {
            self.outstanding.remove(&envelope.req_id);
            return Ok(envelope);
        }
        let deadline = Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let envelope = self.recv_raw(left)?;
            if self.outstanding.remove(&envelope.req_id) {
                return Ok(envelope);
            }
        }
    }

    fn recv_raw(&mut self, timeout: Duration) -> Result<Envelope<Response>, Error> {
        let mut items = [self.socket.as_poll_item(zmq::POLLIN)];
        let ready = zmq::poll(&mut items, timeout.as_millis().min(i64::MAX as u128) as i64)
            .map_err(|err| zmq_err("Failed to poll", err))?;
        if ready == 0 {
            return Err(Error::io_error("Timed out waiting for the server"));
        }
        let bytes = self
            .socket
            .recv_bytes(0)
            .map_err(|err| zmq_err("Failed to receive response", err))?;
        wire::decode(&bytes)
    }

    /// Sends `request` and waits for its response. If the server forgot this
    /// connection's session (restart, idle sweep), re-handshakes once and retries.
    pub fn call(&mut self, request: &Request, timeout: Duration) -> Result<Response, Error> {
        let response = self.call_once(request, timeout)?;
        if is_not_authenticated(&response) {
            self.hello()?;
            return self.call_once(request, timeout);
        }
        Ok(response)
    }

    fn call_once(&mut self, request: &Request, timeout: Duration) -> Result<Response, Error> {
        // One deadline covers both queueing the request and the reply.
        let deadline = Instant::now() + timeout;
        let req_id = self.send_raw(request, deadline)?;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let envelope = self.recv_raw(left)?;
            if envelope.req_id == req_id {
                return Ok(envelope.body);
            }
            // Keep replies to live `send` requests for `recv`; anything else is
            // a late reply to an earlier `call` that timed out.
            if self.outstanding.contains(&envelope.req_id) {
                self.buffered.push_back(envelope);
            }
        }
    }

    /// Leases up to `max_tasks`, letting the server wait up to `wait` for the
    /// first. An empty queue is `Err` with kind NotFound ("QueueEmpty").
    pub fn lease(
        &mut self,
        task_type: Identifier,
        max_tasks: u32,
        wait: Duration,
    ) -> Result<Leased, Error> {
        let request = Request::Lease {
            task_type,
            max_tasks,
            wait_ms: wait.as_millis().min(u32::MAX as u128) as u32,
        };
        match self.call(&request, self.timeout + wait)? {
            Response::Leased(leased) => Ok(leased),
            other => Err(into_error(other)),
        }
    }

    /// Renews leases; per-lease new expiry (unix ms) or error.
    pub fn renew(&mut self, leases: Vec<LeaseRef>) -> Result<Results<i64>, Error> {
        match self.call(&Request::Renew { leases }, self.timeout)? {
            Response::Renewed { results } => Ok(results),
            other => Err(into_error(other)),
        }
    }

    /// Completes tasks; with `then_lease`, also returns the next batch.
    pub fn complete(
        &mut self,
        results: Vec<TaskResult>,
        then_lease: Option<LeaseReq>,
    ) -> Result<(Results<()>, Option<Leased>), Error> {
        let request = Request::Complete {
            results,
            then_lease,
        };
        match self.call(&request, self.timeout)? {
            Response::Completed { results, next } => Ok((results, next)),
            other => Err(into_error(other)),
        }
    }

    pub fn cancel_lease(&mut self, leases: Vec<LeaseRef>) -> Result<Results<()>, Error> {
        match self.call(&Request::CancelLease { leases }, self.timeout)? {
            Response::Cancelled { results } => Ok(results),
            other => Err(into_error(other)),
        }
    }

    pub fn query(&mut self, query: Query) -> Result<QueryResponse, Error> {
        match self.call(&Request::Query(query), self.timeout)? {
            Response::Query(response) => Ok(response),
            other => Err(into_error(other)),
        }
    }

    /// Submits tasks; per-payload task id or error, in order.
    pub fn insert(
        &mut self,
        task_type: Identifier,
        payloads: Vec<Vec<u8>>,
    ) -> Result<Results<String>, Error> {
        match self.query(Query::Insert {
            task_type,
            payloads,
        })? {
            QueryResponse::Ids(ids) => Ok(ids),
            other => Err(Error::new(format!("Unexpected query response: {other:?}"))),
        }
    }

    /// Submits the indexes `start..start + count` of a derived type; one id per
    /// chunk, in range order.
    pub fn insert_derived(
        &mut self,
        task_type: Identifier,
        base: Vec<u8>,
        start: u64,
        count: u64,
    ) -> Result<Results<String>, Error> {
        match self.query(Query::InsertDerived {
            task_type,
            base,
            start,
            count,
        })? {
            QueryResponse::Ids(ids) => Ok(ids),
            other => Err(Error::new(format!("Unexpected query response: {other:?}"))),
        }
    }

    pub fn stats(&mut self) -> Result<Stats, Error> {
        match self.query(Query::Stats)? {
            QueryResponse::Stats(stats) => Ok(stats),
            other => Err(Error::new(format!("Unexpected query response: {other:?}"))),
        }
    }
}

/// Asks a proxy for candidate servers (least loaded first) and an optional
/// forwarded-auth ticket.
pub fn resolve(
    proxy: &str,
    task_type: Option<Identifier>,
    auth: &Auth,
    timeout: Duration,
) -> Result<(Vec<ServerInfo>, Option<Vec<u8>>), Error> {
    let socket = dealer(&wire::endpoint(proxy))?;
    let request = ProxyRequest::Resolve {
        task_type,
        auth: auth.clone(),
    };
    socket
        .send(wire::encode(1, &request)?, 0)
        .map_err(|err| zmq_err("Failed to send to proxy", err))?;
    let mut items = [socket.as_poll_item(zmq::POLLIN)];
    let ready = zmq::poll(&mut items, timeout.as_millis().min(i64::MAX as u128) as i64)
        .map_err(|err| zmq_err("Failed to poll", err))?;
    if ready == 0 {
        return Err(Error::io_error("Timed out waiting for the proxy"));
    }
    let bytes = socket
        .recv_bytes(0)
        .map_err(|err| zmq_err("Failed to receive from proxy", err))?;
    match wire::decode::<ProxyResponse>(&bytes)?.body {
        ProxyResponse::Resolved {
            servers, ticket, ..
        } => Ok((servers, ticket)),
        ProxyResponse::Error(err) => Err(err.into()),
        ProxyResponse::Registered => Err(Error::new("Unexpected proxy response".into())),
    }
}

fn dealer(endpoint: &str) -> Result<zmq::Socket, Error> {
    let socket = context()
        .socket(zmq::DEALER)
        .map_err(|err| zmq_err("Failed to create DEALER", err))?;
    socket
        .set_ipv6(true)
        .map_err(|err| zmq_err("set_ipv6", err))?;
    socket
        .set_linger(0)
        .map_err(|err| zmq_err("set_linger", err))?;
    socket
        .connect(endpoint)
        .map_err(|err| zmq_err(&format!("Failed to connect to {endpoint}"), err))?;
    Ok(socket)
}

fn is_not_authenticated(response: &Response) -> bool {
    matches!(response, Response::Error(err) if err.message == "NotAuthenticated")
}

fn into_error(response: Response) -> Error {
    match response {
        Response::Error(err) => err.into(),
        other => unexpected(other),
    }
}

fn unexpected(response: Response) -> Error {
    match response {
        Response::Error(err) => err.into(),
        other => Error::with_kind(
            ErrorKind::Unknown,
            format!("Unexpected response: {other:?}"),
        ),
    }
}

/// A poll timeout in milliseconds, rounded up so a sub-millisecond wait still
/// polls rather than returning immediately forever.
fn poll_ms(left: Duration) -> i64 {
    left.as_micros().div_ceil(1000).min(i64::MAX as u128) as i64
}
