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
use enginelib::transport::server::{Server, ServerOptions};
use enginelib::wire::{
    Auth, Caps, LeaseRef, LeaseReq, Leased, Query, QueryResponse, Scope, TaskResult, TaskStatus,
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
    let dir = TempDir::new(name);
    let mut cfg = Config::default();
    cfg.config_toml.auth_token = auth_token.map(str::to_string);
    let mut api = ServerAPI::open(&dir.0, cfg);
    api.task_registry.register(Arc::new(TestTask), task_type());
    let api = Arc::new(api);
    ServerAPI::populate(&api);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = {
        let _guard = rt.enter();
        Server::start(
            api,
            ServerOptions {
                endpoint: "tcp://127.0.0.1:*".into(),
                server_id: proxy.map_or(0, |(_, id)| id),
                advertise: None,
                proxy: proxy.map(|(endpoint, _)| endpoint.to_string()),
                heartbeat: Duration::from_millis(100),
                max_message_bytes: 16 * 1024 * 1024,
                session_idle: Duration::from_secs(60),
            },
        )
        .unwrap()
    };
    Harness {
        server,
        _rt: rt,
        _dir: dir,
    }
}

fn auth(admin_token: Option<&str>) -> Auth {
    Auth {
        uid: "worker".into(),
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
    let QueryResponse::Records(records) = admin
        .query(Query::Get {
            task_type: task_type(),
            ids: vec![id],
        })
        .unwrap()
    else {
        panic!("expected records");
    };
    assert_eq!(records[0].status, TaskStatus::Unknown);
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
