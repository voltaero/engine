//! Task lifecycle tests: lease fencing, idempotent complete, reaping + refill,
//! cursor-based loading and lease timeouts. Each test opens its own RocksDB
//! store in a temp dir so tests can run in parallel.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use enginelib::api::{LeasedTaskQueue, ServerAPI, finished_task_key, task_key};
use enginelib::config::Config;
use enginelib::error::ErrorKind;
use enginelib::nucleus::submit::submit;
use enginelib::nucleus::{cancel::cancel, complete::complete, lease::lease_timeout, renew::renew};
use enginelib::task::{Task, Verifiable};
use enginelib::{Identifier, Registry};

const WAIT: Duration = Duration::from_secs(5);

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

/// A unique temp dir, removed on drop.
struct TempDir(PathBuf);
impl TempDir {
    fn new(name: &str) -> Self {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "enginelib-test-{}-{}-{}",
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

fn setup(name: &str, queue_capacity: usize, lease_ttl_secs: u64) -> (Arc<ServerAPI>, TempDir) {
    let dir = TempDir::new(name);
    let mut cfg = Config::default();
    cfg.config_toml.queue_capacity = queue_capacity;
    cfg.config_toml.lease_ttl_secs = lease_ttl_secs;
    let mut api = ServerAPI::open(&dir.0, cfg);
    api.task_registry.register(Arc::new(TestTask), task_type());
    let api = Arc::new(api);
    ServerAPI::populate(&api);
    (api, dir)
}

fn in_dedup(api: &ServerAPI, task_id: &str) -> bool {
    api.task_queue
        .tasks
        .get(&task_type())
        .unwrap()
        .2
        .contains(task_id)
}

fn is_leased(api: &ServerAPI, task_id: &str) -> bool {
    api.leased_tasks
        .leases(&task_type())
        .unwrap()
        .contains_key(task_id)
}

#[tokio::test]
async fn lease_token_mismatch_is_rejected() {
    let (api, _dir) = setup("mismatch", 64, 300);
    let task_id = submit(api.clone(), b"payload", task_type()).unwrap();
    let lease = lease_timeout(api.clone(), task_type(), "w".into(), WAIT)
        .await
        .unwrap();
    assert_eq!(lease.task.task_id, task_id);
    let wrong = lease.token + 1000;

    let err = renew(api.clone(), task_type(), &task_id, wrong).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidArgument);
    let err = cancel(api.clone(), task_type(), task_id.clone(), wrong).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidArgument);
    let err = complete(api.clone(), task_type(), task_id.clone(), wrong, b"r").unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidArgument);

    // The rejected calls left the lease intact for its real holder.
    assert!(is_leased(&api, &task_id));
    let expires_at = renew(api.clone(), task_type(), &task_id, lease.token).unwrap();
    assert!(expires_at >= lease.expires_at);
    complete(api.clone(), task_type(), task_id.clone(), lease.token, b"r").unwrap();
    assert!(!is_leased(&api, &task_id));
    assert!(!in_dedup(&api, &task_id));
}

#[tokio::test]
async fn reaped_task_is_releasable_via_refill() {
    // TTL 0: every lease is expired as soon as it is granted.
    let (api, _dir) = setup("reap", 64, 0);
    let task_id = submit(api.clone(), b"payload", task_type()).unwrap();
    let first = lease_timeout(api.clone(), task_type(), "a".into(), WAIT)
        .await
        .unwrap();
    assert_eq!(first.task.task_id, task_id);

    LeasedTaskQueue::reap_expired(&api);
    assert!(!is_leased(&api, &task_id));

    // No manual load(): the reaper's refill request puts the task back.
    let second = lease_timeout(api.clone(), task_type(), "b".into(), WAIT)
        .await
        .unwrap();
    assert_eq!(second.task.task_id, task_id);
    assert_ne!(second.token, first.token);
    assert!(is_leased(&api, &task_id));
    assert!(in_dedup(&api, &task_id));
}

#[tokio::test]
async fn stale_worker_cannot_act_after_reap_and_release() {
    let (api, _dir) = setup("stale", 64, 0);
    let task_id = submit(api.clone(), b"payload", task_type()).unwrap();
    let stale = lease_timeout(api.clone(), task_type(), "a".into(), WAIT)
        .await
        .unwrap();
    LeasedTaskQueue::reap_expired(&api);
    let current = lease_timeout(api.clone(), task_type(), "b".into(), WAIT)
        .await
        .unwrap();
    assert_eq!(current.task.task_id, task_id);

    // Worker A's lease was reaped; it must not touch B's lease.
    assert!(complete(api.clone(), task_type(), task_id.clone(), stale.token, b"a").is_err());
    assert!(cancel(api.clone(), task_type(), task_id.clone(), stale.token).is_err());
    assert!(renew(api.clone(), task_type(), &task_id, stale.token).is_err());
    assert!(is_leased(&api, &task_id));
    assert!(
        api.db
            .get(finished_task_key(&task_type(), &task_id))
            .unwrap()
            .is_none()
    );

    complete(
        api.clone(),
        task_type(),
        task_id.clone(),
        current.token,
        b"b",
    )
    .unwrap();
    assert_eq!(
        api.db
            .get(finished_task_key(&task_type(), &task_id))
            .unwrap()
            .as_deref(),
        Some(&b"b"[..])
    );
}

#[tokio::test]
async fn complete_retry_is_idempotent() {
    let (api, _dir) = setup("idempotent", 64, 300);
    let task_id = submit(api.clone(), b"payload", task_type()).unwrap();
    let lease = lease_timeout(api.clone(), task_type(), "w".into(), WAIT)
        .await
        .unwrap();

    complete(
        api.clone(),
        task_type(),
        task_id.clone(),
        lease.token,
        b"result",
    )
    .unwrap();
    assert!(
        api.db
            .get(task_key(&task_type(), &task_id))
            .unwrap()
            .is_none()
    );

    // Retry after a lost response: same bytes succeed, different bytes don't.
    complete(
        api.clone(),
        task_type(),
        task_id.clone(),
        lease.token,
        b"result",
    )
    .unwrap();
    let err = complete(
        api.clone(),
        task_type(),
        task_id.clone(),
        lease.token,
        b"other",
    )
    .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    assert_eq!(
        api.db
            .get(finished_task_key(&task_type(), &task_id))
            .unwrap()
            .as_deref(),
        Some(&b"result"[..])
    );
    assert!(!in_dedup(&api, &task_id));
}

#[tokio::test]
async fn cancel_requeues_under_a_new_token() {
    let (api, _dir) = setup("cancel", 64, 300);
    let task_id = submit(api.clone(), b"payload", task_type()).unwrap();
    let first = lease_timeout(api.clone(), task_type(), "a".into(), WAIT)
        .await
        .unwrap();
    cancel(api.clone(), task_type(), task_id.clone(), first.token).unwrap();
    assert!(in_dedup(&api, &task_id));

    let second = lease_timeout(api.clone(), task_type(), "b".into(), WAIT)
        .await
        .unwrap();
    assert_eq!(second.task.task_id, task_id);
    assert_ne!(second.token, first.token);
    assert!(complete(api.clone(), task_type(), task_id.clone(), first.token, b"r").is_err());
    complete(
        api.clone(),
        task_type(),
        task_id.clone(),
        second.token,
        b"r",
    )
    .unwrap();
}

#[tokio::test]
async fn cursor_load_picks_up_key_before_cursor_after_full_rescan() {
    // Capacity 2 so a refill can't load everything at once and leaves the cursor
    // mid-prefix. With capacity 2 the low watermark is an empty queue, and every
    // lease below happens on a non-empty one, so no background refill races the
    // explicit loads.
    let (api, _dir) = setup("cursor", 2, 300);
    let tt = task_type();
    for id in ["b1", "b2", "b3"] {
        api.db.put(task_key(&tt, id), b"payload").unwrap();
    }

    ServerAPI::load(&api, tt.clone()).await.unwrap();
    let a = lease_timeout(api.clone(), tt.clone(), "w".into(), WAIT)
        .await
        .unwrap();
    let b = lease_timeout(api.clone(), tt.clone(), "w".into(), WAIT)
        .await
        .unwrap();
    assert_eq!(
        (a.task.task_id.as_str(), b.task.task_id.as_str()),
        ("b1", "b2")
    );

    // A pending record whose key sorts before the cursor (as a druid id from a
    // skewed clock would). Written raw, so only the cursor logic can find it.
    api.db.put(task_key(&tt, "a0"), b"payload").unwrap();

    // The cursor pass resumes after b2: it finds b3, reaches the end of the
    // prefix, and does not see a0.
    assert_eq!(api.refill(&tt).unwrap(), 1);
    let c = lease_timeout(api.clone(), tt.clone(), "w".into(), WAIT)
        .await
        .unwrap();
    assert_eq!(c.task.task_id, "b3");
    assert!(!in_dedup(&api, "a0"));

    // Having reached the end, the next pass is a full rescan from the prefix
    // start, which picks a0 up (b1..b3 are skipped as in-flight).
    assert_eq!(api.refill(&tt).unwrap(), 1);
    let d = lease_timeout(api.clone(), tt.clone(), "w".into(), WAIT)
        .await
        .unwrap();
    assert_eq!(d.task.task_id, "a0");

    // That full pass found everything: further refills are no-ops.
    assert_eq!(api.refill(&tt).unwrap(), 0);
    for lease in [a, b, c, d] {
        complete(
            api.clone(),
            tt.clone(),
            lease.task.task_id.clone(),
            lease.token,
            b"r",
        )
        .unwrap();
    }
    assert!(api.task_queue.tasks.get(&tt).unwrap().2.is_empty());
}

#[tokio::test]
async fn submit_wakes_parked_lease() {
    let (api, _dir) = setup("wake", 64, 300);
    let waiter = tokio::spawn(lease_timeout(api.clone(), task_type(), "w".into(), WAIT));
    tokio::time::sleep(Duration::from_millis(50)).await;
    let task_id = submit(api.clone(), b"payload", task_type()).unwrap();
    let lease = waiter.await.unwrap().unwrap();
    assert_eq!(lease.task.task_id, task_id);
}

#[tokio::test]
async fn lease_times_out_on_empty_queue() {
    let (api, _dir) = setup("timeout", 64, 300);
    let started = std::time::Instant::now();
    let err = lease_timeout(
        api.clone(),
        task_type(),
        "w".into(),
        Duration::from_millis(100),
    )
    .await
    .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    assert_eq!(err.as_ref(), "QueueEmpty");
    assert!(started.elapsed() >= Duration::from_millis(100));
}
