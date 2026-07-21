//! In-process tests of the lease-recovery paths: reaping requeues expired
//! leases, the loader's rescan flag un-strands tasks dropped back to
//! "persisted only", complete_batch counts duplicate ids once, and the
//! registry refuses identifiers that would collide key prefixes.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::Utc;
use dashmap::DashSet;
use enginelib::Identifier;
use enginelib::Registry;
use enginelib::api::{LeasedTaskQueue, ServerAPI};
use enginelib::nucleus::{complete, lease, submit};
use enginelib::task::{Task, Verifiable};

#[derive(Debug, Clone)]
struct EchoTask;

impl Verifiable for EchoTask {
    fn verify(&self, _bytes: &[u8]) -> bool {
        true
    }
}

impl Task for EchoTask {
    fn get_id(&self) -> Identifier {
        ("test".to_string(), "echo".to_string())
    }
    fn clone_box(&self) -> Box<dyn Task> {
        Box::new(self.clone())
    }
    fn to_bytes(&self) -> Vec<u8> {
        Vec::new()
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

fn unique_tmp(tag: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir()
        .join(format!("engine_recovery_{tag}_{nanos}"))
        .to_string_lossy()
        .into_owned()
}

fn test_api(tag: &str, task_type: &Identifier) -> Arc<ServerAPI> {
    let api = ServerAPI::with_path(&unique_tmp(tag));
    api.task_registry
        .tasks
        .insert(task_type.clone(), Arc::new(EchoTask));
    let api = Arc::new(api);
    ServerAPI::populate(&api);
    api
}

/// Backdate every lease for `task_type` far enough that the reaper sees it as
/// expired.
fn expire_leases(api: &Arc<ServerAPI>, task_type: &Identifier) {
    let mut leases = api.leased_tasks.tasks.get_mut(task_type).expect("leases");
    for lease in leases.iter_mut() {
        lease.given_at = Utc::now() - chrono::Duration::seconds(7200);
    }
}

/// An expired lease is reaped and its task handed straight back to the queue,
/// so the next lease call gets it again.
#[tokio::test]
async fn reaped_lease_is_requeued() {
    let task_type: Identifier = ("test".to_string(), "echo".to_string());
    let api = test_api("reap", &task_type);

    submit::submit(api.clone(), b"payload", task_type.clone()).expect("submit");
    ServerAPI::load_all(&api, task_type.clone()).await.expect("load");

    let leased = lease::lease_batch(api.clone(), task_type.clone(), "w1".into(), 1)
        .await
        .expect("lease");
    assert_eq!(leased.len(), 1);
    let task_id = leased[0].task_id.clone();

    expire_leases(&api, &task_type);
    LeasedTaskQueue::reap_expired(&api);

    // The lease is gone and the task is back in the channel for the next worker.
    assert!(api.leased_tasks.tasks.get(&task_type).is_none_or(|l| l.is_empty()));
    let released = lease::lease_batch(api.clone(), task_type.clone(), "w2".into(), 1)
        .await
        .expect("re-lease");
    assert_eq!(released.len(), 1);
    assert_eq!(released[0].task_id, task_id);
}

/// When the reaper can't requeue (channel full), the task drops back to
/// "persisted only" and the rescan flag makes the running loader rewind its
/// cursor and re-enqueue it — the stranded-task case.
#[tokio::test]
async fn full_channel_reap_recovers_via_loader_rescan() {
    let task_type: Identifier = ("test".to_string(), "echo".to_string());
    let api = ServerAPI::with_path(&unique_tmp("rescan"));
    api.task_registry
        .tasks
        .insert(task_type.clone(), Arc::new(EchoTask));
    let api = Arc::new(api);
    // Capacity-1 channel (inserted before populate, whose or_insert keeps it)
    // so a single queued task makes the reaper's try_send fail.
    let (tx, rx) = async_channel::bounded(1);
    api.task_queue.tasks.insert(
        task_type.clone(),
        (tx, rx, DashSet::default(), Arc::new(AtomicBool::new(false))),
    );
    ServerAPI::populate(&api);
    tokio::spawn(ServerAPI::run_loader(api.clone(), task_type.clone()));

    let ids = submit::submit_batch(
        api.clone(),
        task_type.clone(),
        vec![b"a".to_vec(), b"b".to_vec()],
    )
    .expect("submit");

    // Lease the first task; the loader then fills the channel with the second.
    let first = tokio::time::timeout(
        Duration::from_secs(5),
        lease::lease_batch(api.clone(), task_type.clone(), "w1".into(), 1),
    )
    .await
    .expect("lease timed out")
    .expect("lease");
    assert_eq!(first.len(), 1);

    // Wait until the channel is full so the reaper's try_send must fail.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while api.task_queue.tasks.get(&task_type).expect("queue").0.len() < 1 {
        assert!(tokio::time::Instant::now() < deadline, "loader never enqueued");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    expire_leases(&api, &task_type);
    LeasedTaskQueue::reap_expired(&api);

    // Both tasks must come back out: the queued one, and the reaped one that
    // only the rescan can recover.
    let mut seen = Vec::new();
    while seen.len() < 2 {
        let batch = tokio::time::timeout(
            Duration::from_secs(5),
            lease::lease_batch(api.clone(), task_type.clone(), "w2".into(), 2),
        )
        .await
        .expect("recovery lease timed out")
        .expect("lease");
        seen.extend(batch.iter().map(|t| t.task_id.clone()));
    }
    seen.sort();
    let mut expected = ids.clone();
    expected.sort();
    assert_eq!(seen, expected);
}

/// A duplicate task id inside one complete_batch request is completed and
/// counted once, not twice.
#[tokio::test]
async fn complete_batch_counts_duplicate_ids_once() {
    let task_type: Identifier = ("test".to_string(), "echo".to_string());
    let api = test_api("dup", &task_type);

    submit::submit(api.clone(), b"payload", task_type.clone()).expect("submit");
    ServerAPI::load_all(&api, task_type.clone()).await.expect("load");
    let leased = lease::lease_batch(api.clone(), task_type.clone(), "w".into(), 1)
        .await
        .expect("lease");
    let task_id = leased[0].task_id.clone();

    let ok = complete::complete_batch(
        api.clone(),
        task_type.clone(),
        vec![
            (task_id.clone(), b"done".to_vec()),
            (task_id.clone(), b"done again".to_vec()),
        ],
    )
    .expect("complete");
    assert_eq!(ok, 1);
}

/// Identifiers containing ':' would collide RocksDB key prefixes across task
/// types; the registry must refuse them.
#[test]
fn registry_refuses_colliding_identifiers() {
    let mut registry = enginelib::api::EngineTaskRegistry::default();
    let bad: Identifier = ("ns:x".to_string(), "name".to_string());
    registry.register(Arc::new(EchoTask), bad.clone());
    assert!(registry.get(&bad).is_none());

    let good: Identifier = ("ns".to_string(), "name".to_string());
    registry.register(Arc::new(EchoTask), good.clone());
    assert!(registry.get(&good).is_some());
}
