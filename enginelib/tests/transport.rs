//! End-to-end tests of the ZeroMQ transport: a real server on a loopback TCP
//! port, driven by the blocking client. Each test gets its own RocksDB store and
//! tokio runtime; client calls run on the test thread, outside the runtime.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use enginelib::api::ServerAPI;
use enginelib::config::Config;
use enginelib::error::ErrorKind;
use enginelib::task::{Task, Verifiable};
use enginelib::transport::Background;
use enginelib::transport::client::{Client, resolve};
use enginelib::transport::proxy::{Proxy, ProxyOptions};
use enginelib::transport::server::{DEFAULT_MAX_INFLIGHT, Server, ServerOptions};
use enginelib::wire::{
    Auth, Caps, LeaseRef, LeaseReq, Leased, Query, QueryResponse, Request, Response, Scope,
    TaskRecord, TaskResult, TaskStatus,
};
use enginelib::{Identifier, Registry};

#[derive(Debug, Clone)]
struct TestTask;
impl Verifiable for TestTask {
    fn verify(&self, b: &[u8]) -> bool {
        !b.is_empty()
    }
}
impl Task for TestTask {
    fn get_id(&self) -> Identifier {
        task_type()
    }
    fn clone_box(&self) -> Box<dyn Task> {
        Box::new(self.clone())
    }
    fn to_bytes(&self) -> Vec<u8> {
        vec![1]
    }
    fn from_bytes(&self, _bytes: &[u8]) -> Box<dyn Task> {
        Box::new(self.clone())
    }
    fn from_toml(&self, _d: String) -> Box<dyn Task> {
        Box::new(self.clone())
    }
    fn to_toml(&self) -> String {
        String::new()
    }
}

fn task_type() -> Identifier {
    ("test".to_string(), "task".to_string())
}

struct TempDir(PathBuf);
impl TempDir {
    fn new(name: &str) -> Self {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "enginelib-transport-{}-{}-{}",
            std::process::id(),
            name,
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&path);
        Self(path)
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A running server. Field order matters: the server stops before the runtime
/// and store go away.
struct Harness {
    server: Background,
    _rt: tokio::runtime::Runtime,
    _dir: TempDir,
}

fn start(name: &str, auth_token: Option<&str>, proxy: Option<(&str, u16)>) -> Harness {
    start_with(
        name,
        |cfg| cfg.config_toml.auth_token = auth_token.map(str::to_string),
        |opts| {
            if let Some((endpoint, id)) = proxy {
                opts.server_id = id;
                opts.proxy = Some(endpoint.to_string());
                opts.proxy_token = Some(PROXY_TOKEN.into());
            }
        },
    )
}

const PROXY_TOKEN: &str = "proxy-secret";

fn start_with(
    name: &str,
    configure: impl FnOnce(&mut Config),
    options: impl FnOnce(&mut ServerOptions),
) -> Harness {
    let dir = TempDir::new(name);
    let mut cfg = Config::default();
    configure(&mut cfg);
    let mut api = ServerAPI::open(&dir.0, cfg);
    api.task_registry.register(Arc::new(TestTask), task_type());
    let api = Arc::new(api);
    ServerAPI::populate(&api);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut opts = ServerOptions {
        endpoint: "tcp://127.0.0.1:*".into(),
        server_id: 0,
        advertise: None,
        proxy: None,
        proxy_token: None,
        heartbeat: Duration::from_millis(100),
        max_message_bytes: 16 * 1024 * 1024,
        session_idle: Duration::from_secs(60),
        max_inflight: DEFAULT_MAX_INFLIGHT,
    };
    options(&mut opts);
    let server = {
        let _guard = rt.enter();
        Server::start(api, opts).unwrap()
    };
    Harness {
        server,
        _rt: rt,
        _dir: dir,
    }
}

fn auth(admin_token: Option<&str>) -> Auth {
    auth_as("worker", admin_token)
}

fn auth_as(uid: &str, admin_token: Option<&str>) -> Auth {
    Auth {
        uid: uid.into(),
        challenge: String::new(),
        admin_token: admin_token.map(str::to_string),
    }
}

fn connect(harness: &Harness) -> Client {
    Client::connect(harness.server.endpoint(), auth(None), Caps::default()).unwrap()
}

fn lease_refs(leased: &Leased) -> Vec<LeaseRef> {
    leased
        .tasks
        .iter()
        .map(|task| LeaseRef {
            task_type: leased.task_type.clone(),
            task_id: task.task_id.clone(),
            token: task.token,
        })
        .collect()
}

/// Result of a task in these tests: its payload reversed.
fn results_for(leased: &Leased) -> Vec<TaskResult> {
    leased
        .tasks
        .iter()
        .zip(lease_refs(leased))
        .map(|(task, lease)| TaskResult {
            lease,
            bytes: task.bytes.iter().rev().copied().collect(),
        })
        .collect()
}

const WAIT: Duration = Duration::from_secs(5);

#[test]
fn full_lifecycle_over_tcp() {
    let harness = start("lifecycle", None, None);
    let mut client = connect(&harness);
    assert_eq!(client.server.task_types.len(), 1);

    // Insert 100 valid payloads plus one that fails verification.
    let mut payloads: Vec<Vec<u8>> = (0..100u32).map(|i| i.to_be_bytes().to_vec()).collect();
    payloads.push(Vec::new());
    let ids = client.insert(task_type(), payloads).unwrap();
    assert_eq!(ids.len(), 101);
    assert!(ids[..100].iter().all(Result::is_ok));
    assert_eq!(
        ids[100].as_ref().unwrap_err().kind,
        ErrorKind::InvalidArgument
    );
    let ids: Vec<String> = ids.into_iter().take(100).map(Result::unwrap).collect();

    // Lease a batch of 60, renew it, then complete it while asking for the rest.
    let first = client.lease(task_type(), 60, WAIT).unwrap();
    assert_eq!(first.tasks.len(), 60);
    let renewed = client.renew(lease_refs(&first)).unwrap();
    assert!(renewed.iter().all(Result::is_ok));

    let (results, next) = client
        .complete(
            results_for(&first),
            Some(LeaseReq {
                task_type: task_type(),
                max_tasks: 100,
            }),
        )
        .unwrap();
    assert!(results.iter().all(Result::is_ok));
    let next = next.expect("piggybacked lease");
    assert_eq!(next.tasks.len(), 40);

    // A retried completion is idempotent; finish the rest without a next lease.
    let (retry, _) = client
        .complete(results_for(&first)[..1].to_vec(), None)
        .unwrap();
    assert!(retry[0].is_ok());
    let (results, none) = client.complete(results_for(&next), None).unwrap();
    assert!(results.iter().all(Result::is_ok));
    assert!(none.is_none());

    // Every task is finished, with its reversed payload as the result.
    let QueryResponse::Records(records) = client
        .query(Query::Get {
            task_type: task_type(),
            ids: ids.clone(),
        })
        .unwrap()
    else {
        panic!("expected records");
    };
    for (i, record) in records.iter().enumerate() {
        assert_eq!(record.status, TaskStatus::Finished);
        let mut expected = (i as u32).to_be_bytes().to_vec();
        expected.reverse();
        assert_eq!(record.bytes.as_deref(), Some(expected.as_slice()));
    }

    let stats = client.stats().unwrap();
    assert_eq!(stats.inserted, 100);
    assert_eq!(stats.leased, 100);
    // Counts successful Complete results, so the idempotent retry adds one.
    assert_eq!(stats.completed, 101);
    assert_eq!(stats.active_leases, 0);
    assert_eq!(stats.queued, 0);
}

#[test]
fn empty_queue_lease_times_out() {
    let harness = start("empty", None, None);
    let mut client = connect(&harness);
    let started = Instant::now();
    let err = client
        .lease(task_type(), 1, Duration::from_millis(200))
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    assert_eq!(err.as_ref(), "QueueEmpty");
    assert!(started.elapsed() >= Duration::from_millis(200));
}

#[test]
fn cancelled_lease_is_released_and_stale_token_rejected() {
    let harness = start("cancel", None, None);
    let mut client = connect(&harness);
    client.insert(task_type(), vec![b"a".to_vec()]).unwrap();

    let leased = client.lease(task_type(), 1, WAIT).unwrap();
    let cancelled = client.cancel_lease(lease_refs(&leased)).unwrap();
    assert!(cancelled[0].is_ok());

    // Re-leased under a new token; the old one can't complete it.
    let again = client.lease(task_type(), 1, WAIT).unwrap();
    assert_eq!(again.tasks[0].task_id, leased.tasks[0].task_id);
    assert_ne!(again.tasks[0].token, leased.tasks[0].token);
    let (stale, _) = client.complete(results_for(&leased), None).unwrap();
    assert_eq!(stale[0].as_ref().unwrap_err().message, "LeaseTokenMismatch");
    let (fresh, _) = client.complete(results_for(&again), None).unwrap();
    assert!(fresh[0].is_ok());
}

#[test]
fn admin_queries_require_admin_token() {
    let harness = start("admin", Some("secret"), None);
    let mut user = connect(&harness);
    let ids = user.insert(task_type(), vec![b"x".to_vec()]).unwrap();
    let id = ids[0].clone().unwrap();
    wait_until("the refill", || user.stats().unwrap().queued == 1);

    let purge = Query::Purge {
        task_type: task_type(),
        scope: Scope::Pending,
    };
    let err = user.query(purge.clone()).unwrap_err();
    assert_eq!(err.as_ref(), "AdminRequired");

    let wrong = Client::connect(
        harness.server.endpoint(),
        auth(Some("nope")),
        Caps::default(),
    )
    .unwrap()
    .query(purge.clone())
    .unwrap_err();
    assert_eq!(wrong.as_ref(), "AdminRequired");

    let mut admin = Client::connect(
        harness.server.endpoint(),
        auth(Some("secret")),
        Caps::default(),
    )
    .unwrap();
    assert!(matches!(admin.query(purge).unwrap(), QueryResponse::Done));
    // The task is already queued, so the purge leaves it alone.
    assert_eq!(get(&mut admin, &[id])[0].status, TaskStatus::Pending);
}

fn get(client: &mut Client, ids: &[String]) -> Vec<TaskRecord> {
    let QueryResponse::Records(records) = client
        .query(Query::Get {
            task_type: task_type(),
            ids: ids.to_vec(),
        })
        .unwrap()
    else {
        panic!("expected records");
    };
    records
}

fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn leases_are_bound_to_the_leasing_uid() {
    let harness = start("owner", None, None);
    let mut owner = connect(&harness);
    let mut intruder = Client::connect(
        harness.server.endpoint(),
        auth_as("intruder", None),
        Caps::default(),
    )
    .unwrap();
    owner.insert(task_type(), vec![b"a".to_vec()]).unwrap();
    let leased = owner.lease(task_type(), 1, WAIT).unwrap();

    // The right token alone isn't enough: the lease belongs to "worker".
    let renewed = intruder.renew(lease_refs(&leased)).unwrap();
    assert_eq!(renewed[0].as_ref().unwrap_err().message, "LeaseNotOwned");
    let cancelled = intruder.cancel_lease(lease_refs(&leased)).unwrap();
    assert_eq!(cancelled[0].as_ref().unwrap_err().message, "LeaseNotOwned");
    let (completed, _) = intruder.complete(results_for(&leased), None).unwrap();
    assert_eq!(completed[0].as_ref().unwrap_err().message, "LeaseNotOwned");

    let (completed, _) = owner.complete(results_for(&leased), None).unwrap();
    assert!(completed[0].is_ok());
}

#[test]
fn delete_and_purge_keep_in_flight_work() {
    // Capacity 2: of 5 pending tasks, 2 are queued (in flight) and 3 aren't.
    let harness = start_with(
        "purge",
        |cfg| {
            cfg.config_toml.auth_token = Some("secret".into());
            cfg.config_toml.queue_capacity = 2;
        },
        |_| {},
    );
    let mut admin = Client::connect(
        harness.server.endpoint(),
        auth(Some("secret")),
        Caps::default(),
    )
    .unwrap();
    let payloads: Vec<Vec<u8>> = (1..=5u8).map(|i| vec![i]).collect();
    let ids: Vec<String> = admin
        .insert(task_type(), payloads)
        .unwrap()
        .into_iter()
        .map(Result::unwrap)
        .collect();
    wait_until("the refill", || admin.stats().unwrap().queued == 2);

    let purge = Query::Purge {
        task_type: task_type(),
        scope: Scope::Pending,
    };
    assert!(matches!(admin.query(purge).unwrap(), QueryResponse::Done));
    let records = get(&mut admin, &ids);
    let count = |status| records.iter().filter(|r| r.status == status).count();
    assert_eq!(count(TaskStatus::Pending), 2);
    assert_eq!(count(TaskStatus::Unknown), 3);

    // Both survivors can still be leased; deleting one while leased is refused.
    let leased = admin.lease(task_type(), 2, WAIT).unwrap();
    assert_eq!(leased.tasks.len(), 2);
    let err = admin
        .query(Query::Delete {
            task_type: task_type(),
            scope: Scope::Pending,
            ids: vec![leased.tasks[0].task_id.clone()],
        })
        .unwrap_err();
    assert!(err.as_ref().starts_with("TaskInFlight"), "{err:?}");
    let (results, _) = admin.complete(results_for(&leased), None).unwrap();
    assert!(results.iter().all(Result::is_ok));
}

#[test]
fn call_keeps_replies_to_pipelined_requests() {
    let harness = start("pipeline", None, None);
    let mut client = connect(&harness);
    client.insert(task_type(), vec![b"a".to_vec()]).unwrap();

    // The lease reply lands while `stats` waits for its own; `recv` gets it.
    let lease_id = client
        .send(&Request::Lease {
            task_type: task_type(),
            max_tasks: 1,
            wait_ms: 5_000,
        })
        .unwrap();
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(client.stats().unwrap().active_leases, 1);
    let envelope = client.recv(WAIT).unwrap();
    assert_eq!(envelope.req_id, lease_id);
    let Response::Leased(leased) = envelope.body else {
        panic!("expected a lease, got {:?}", envelope.body);
    };
    assert_eq!(leased.tasks.len(), 1);
}

#[test]
fn inflight_cap_holds_back_requests() {
    let harness = start_with("inflight", |_| {}, |opts| opts.max_inflight = 1);
    let mut waiter = connect(&harness);
    let mut other = connect(&harness);

    // The parked lease takes the only slot; `stats` is served once it gives up.
    waiter
        .send(&Request::Lease {
            task_type: task_type(),
            max_tasks: 1,
            wait_ms: 1_000,
        })
        .unwrap();
    std::thread::sleep(Duration::from_millis(100));
    let started = Instant::now();
    other.stats().unwrap();
    assert!(started.elapsed() >= Duration::from_millis(500));
    let envelope = waiter.recv(WAIT).unwrap();
    assert!(matches!(envelope.body, Response::Error(err) if err.message == "QueueEmpty"));
}

#[test]
fn scan_pages_through_finished_records() {
    let harness = start("scan", None, None);
    let mut client = connect(&harness);
    let payloads: Vec<Vec<u8>> = (0..25u8).map(|i| vec![i + 1]).collect();
    client.insert(task_type(), payloads).unwrap();
    let leased = client.lease(task_type(), 25, WAIT).unwrap();
    client.complete(results_for(&leased), None).unwrap();

    let mut seen = 0;
    let mut after = None;
    loop {
        let QueryResponse::Page { records, next } = client
            .query(Query::Scan {
                task_type: task_type(),
                scope: Scope::Finished,
                after: after.clone(),
                limit: 10,
            })
            .unwrap()
        else {
            panic!("expected page");
        };
        seen += records.len();
        match next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    assert_eq!(seen, 25);
}

#[test]
fn proxy_resolves_registered_server() {
    let proxy = Proxy::start(ProxyOptions {
        endpoint: "tcp://127.0.0.1:*".into(),
        ttl_secs: 30,
        server_expiry: Duration::from_secs(5),
        token: Some(PROXY_TOKEN.into()),
    })
    .unwrap();
    let harness = start("proxy", None, Some((proxy.endpoint(), 7)));

    // Wait for the first heartbeat to land.
    let deadline = Instant::now() + WAIT;
    let servers = loop {
        let (servers, ticket) =
            resolve(proxy.endpoint(), Some(task_type()), &auth(None), WAIT).unwrap();
        assert!(ticket.is_none());
        if !servers.is_empty() {
            break servers;
        }
        assert!(Instant::now() < deadline, "server never registered");
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(servers[0].server_id, 7);
    assert_eq!(servers[0].endpoint, harness.server.endpoint());

    // Unknown task types resolve to nobody.
    let (none, _) = resolve(
        proxy.endpoint(),
        Some(("no".into(), "such".into())),
        &auth(None),
        WAIT,
    )
    .unwrap();
    assert!(none.is_empty());

    let mut client = Client::connect_via_proxy(
        proxy.endpoint(),
        Some(task_type()),
        auth(None),
        Caps::default(),
    )
    .unwrap();
    assert_eq!(client.server.server_id, 7);
    assert_eq!(client.stats().unwrap().server_id, 7);
}

#[test]
fn proxy_refuses_registration_without_the_token() {
    let proxy = Proxy::start(ProxyOptions {
        endpoint: "tcp://127.0.0.1:*".into(),
        ttl_secs: 30,
        server_expiry: Duration::from_secs(5),
        token: Some(PROXY_TOKEN.into()),
    })
    .unwrap();
    let register = |id: u16, token: Option<&str>| {
        let endpoint = proxy.endpoint().to_string();
        let token = token.map(str::to_string);
        start_with(
            &format!("proxy-auth-{id}"),
            |_| {},
            move |opts| {
                opts.server_id = id;
                opts.proxy = Some(endpoint);
                opts.proxy_token = token;
            },
        )
    };
    let _missing = register(1, None);
    let _wrong = register(2, Some("guess"));
    let _right = register(3, Some(PROXY_TOKEN));

    let resolve_ids = || {
        let (servers, _) = resolve(proxy.endpoint(), None, &auth(None), WAIT).unwrap();
        servers.iter().map(|s| s.server_id).collect::<Vec<_>>()
    };
    wait_until("the registration", || !resolve_ids().is_empty());
    // Give the refused servers several more heartbeats to (not) show up.
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(resolve_ids(), vec![3]);

    // A proxy without a token refuses everyone.
    let closed = Proxy::start(ProxyOptions {
        endpoint: "tcp://127.0.0.1:*".into(),
        ttl_secs: 30,
        server_expiry: Duration::from_secs(5),
        token: None,
    })
    .unwrap();
    let endpoint = closed.endpoint().to_string();
    let _server = start_with(
        "proxy-closed",
        |_| {},
        move |opts| {
            opts.proxy = Some(endpoint);
            opts.proxy_token = Some(PROXY_TOKEN.into());
        },
    );
    std::thread::sleep(Duration::from_millis(500));
    let (servers, _) = resolve(closed.endpoint(), None, &auth(None), WAIT).unwrap();
    assert!(servers.is_empty());
}
