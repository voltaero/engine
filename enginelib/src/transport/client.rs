//! Blocking ZeroMQ client (DEALER socket) for workers, submitters and operators.
//!
//! One `Client` per thread: ZeroMQ sockets are not thread-safe. [`Client::call`]
//! is request/response; [`Client::send`] + [`Client::recv`] let a caller keep
//! several requests in flight on one connection (responses carry the `req_id`).

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
    pub timeout: Duration,
    pub server: ServerHello,
}

impl Client {
    /// Connects to a server and performs the `Hello` handshake.
    pub fn connect(endpoint: &str, auth: Auth, caps: Caps) -> Result<Self, Error> {
        let endpoint = wire::endpoint(endpoint);
        let socket = dealer(&endpoint)?;
        let mut client = Self {
            socket,
            endpoint,
            auth,
            caps,
            next_req_id: 0,
            timeout: DEFAULT_TIMEOUT,
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
    /// `auth.challenge`.
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
        match self.call_once(&request, self.timeout)? {
            Response::Welcome {
                server_id,
                lease_ttl_secs,
                task_types,
                ..
            } => {
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

    /// Sends a request without waiting; returns its `req_id`.
    pub fn send(&mut self, request: &Request) -> Result<u64, Error> {
        self.next_req_id += 1;
        let req_id = self.next_req_id;
        let bytes = wire::encode(req_id, request)?;
        self.socket
            .send(bytes, 0)
            .map_err(|err| zmq_err("Failed to send request", err))?;
        Ok(req_id)
    }

    /// Receives the next response, whichever request it answers.
    pub fn recv(&mut self, timeout: Duration) -> Result<Envelope<Response>, Error> {
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
        let req_id = self.send(request)?;
        let deadline = Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let envelope = self.recv(left)?;
            // Skip late replies to earlier requests that timed out.
            if envelope.req_id == req_id {
                return Ok(envelope.body);
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
