//! Task lifecycle tests over chunked storage: lease fencing, (partial and
//! idempotent) complete, cancel / reaping returning only open tasks, refill
//! recovery, restart recovery, task-level Get/Scan and per-type settings. Each
//! test opens its own RocksDB store in a temp dir so tests can run in parallel.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use enginelib::api::rust_rocksdb::IteratorMode;
use enginelib::api::{
    Lease, LeasedTaskQueue, ServerAPI, TaskSettings, finished_task_key, task_key, task_key_prefix,
};
use enginelib::chunk::{FinishedChunk, TaskId};
use enginelib::config::{Config, TaskTypeConfig};
use enginelib::error::ErrorKind;
use enginelib::nucleus::complete::{CompleteItem, complete_batch};
use enginelib::nucleus::lease::{lease_many, try_lease};
use enginelib::nucleus::query::{get_tasks, scan_tasks};
use enginelib::nucleus::submit::{submit, submit_batch};
use enginelib::nucleus::{cancel::cancel, complete::complete, lease::lease_timeout, renew::renew};
use enginelib::task::{Task, TaskLimits, Verifiable};
use enginelib::wire::{Scope, TaskStatus};
use enginelib::{Identifier, Registry};

const WAIT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
struct TestTask {
    id: Identifier,
    limits: TaskLimits,
}
impl Verifiable for TestTask {
    fn verify(&self, b: &[u8]) -> bool {
        !b.is_empty()
    }
}
impl Task for TestTask {
    fn get_id(&self) -> Identifier {
        self.id.clone()
    }
    fn clone_box(&self) -> Box<dyn Task> {
        Box::new(self.clone())
    }
    fn limits(&self) -> TaskLimits {
        self.limits
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

struct Opts {
    /// Queue capacity in tasks (the queue holds `ceil(this / chunk_size)` chunks).
    queue_capacity: usize,
    lease_ttl_secs: u64,
    /// Set through `config.toml`'s `[tasks."test:task"]`.
    chunk_size: Option<u32>,
}
impl Default for Opts {
    fn default() -> Self {
        Self {
            queue_capacity: 1024,
            lease_ttl_secs: 300,
            chunk_size: None,
        }
    }
}

fn open(dir: &TempDir, opts: &Opts) -> Arc<ServerAPI> {
    let mut cfg = Config::default();
    cfg.config_toml.queue_capacity = opts.queue_capacity;
    cfg.config_toml.lease_ttl_secs = opts.lease_ttl_secs;
    cfg.config_toml.server_id = 3;
    if let Some(chunk_size) = opts.chunk_size {
        cfg.config_toml.tasks.insert(
            "test:task".into(),
            TaskTypeConfig {
                chunk_size: Some(chunk_size),
                ..Default::default()
            },
        );
    }
    let mut api = ServerAPI::open(&dir.0, cfg);
    api.task_registry.register(
        Arc::new(TestTask {
            id: task_type(),
            limits: TaskLimits::default(),
        }),
        task_type(),
    );
    let api = Arc::new(api);
    ServerAPI::populate(&api);
    api
}

fn setup(name: &str, opts: Opts) -> (Arc<ServerAPI>, TempDir) {
    let dir = TempDir::new(name);
    let api = open(&dir, &opts);
    (api, dir)
}

/// Waits for background refills to release their references, then drops the
/// API so the RocksDB lock is released.
async fn close(api: Arc<ServerAPI>) {
    let mut api = api;
    for _ in 0..500 {
        match Arc::try_unwrap(api) {
            Ok(api) => return drop(api),
            Err(back) => api = back,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("ServerAPI still referenced");
}

fn seq_of(task_id: &str) -> u64 {
    TaskId::parse(task_id).unwrap().chunk_seq
}

fn idx_of(task_id: &str) -> u16 {
    TaskId::parse(task_id).unwrap().idx
}

fn in_dedup(api: &ServerAPI, chunk_seq: u64) -> bool {
    api.task_queue
        .get(&task_type())
        .unwrap()
        .dedup
        .contains(&chunk_seq)
}

fn is_leased(api: &ServerAPI, chunk_seq: u64) -> bool {
    api.leased_tasks.cell(&task_type(), chunk_seq).is_some()
}

fn pending_exists(api: &ServerAPI, chunk_seq: u64) -> bool {
    api.db
        .get_cf(api.cf_pending(), task_key(&task_type(), chunk_seq))
        .unwrap()
        .is_some()
}

fn finished(api: &ServerAPI, chunk_seq: u64) -> Option<FinishedChunk> {
    api.read_finished(&task_type(), chunk_seq).unwrap()
}

fn ids(results: Vec<Result<String, enginelib::error::Error>>) -> Vec<String> {
    results.into_iter().map(Result::unwrap).collect()
}

/// Waits until `n` chunks sit in the queue (a background refill may still be
/// pushing what an explicit one raced it to).
async fn wait_queued(api: &ServerAPI, n: usize) {
    let queue = api.task_queue.get(&task_type()).unwrap();
    for _ in 0..500 {
        if queue.rx.len() == n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("queue never reached {n} chunks (has {})", queue.rx.len());
}

async fn lease_one(api: &Arc<ServerAPI>, worker: &str) -> Lease {
    lease_timeout(api.clone(), task_type(), worker.into(), WAIT)
        .await
        .unwrap()
}

fn complete_ids(
    api: &Arc<ServerAPI>,
    task_ids: &[String],
    token: u64,
    bytes: &[u8],
) -> Vec<Result<(), enginelib::error::Error>> {
    complete_batch(
        api,
        task_ids
            .iter()
            .map(|task_id| CompleteItem {
                task_type: task_type(),
                task_id: task_id.clone(),
                lease_token: token,
                bytes: bytes.to_vec(),
            })
            .collect(),
    )
}

#[tokio::test]
async fn lease_token_mismatch_is_rejected() {
    let (api, _dir) = setup("mismatch", Opts::default());
    let task_id = submit(api.clone(), b"payload", task_type()).unwrap();
    let seq = seq_of(&task_id);
    let lease = lease_one(&api, "w").await;
    assert_eq!(lease.task_ids(), vec![task_id.clone()]);
    let wrong = lease.token + 1000;

    let err = renew(api.clone(), task_type(), &task_id, wrong).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidArgument);
    let err = cancel(api.clone(), task_type(), task_id.clone(), wrong).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidArgument);
    let err = complete(api.clone(), task_type(), task_id.clone(), wrong, b"r").unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidArgument);

    // The rejected calls left the lease intact for its real holder.
    assert!(is_leased(&api, seq));
    let expires_at = renew(api.clone(), task_type(), &task_id, lease.token).unwrap();
    assert!(expires_at >= lease.expires_at);
    complete(api.clone(), task_type(), task_id.clone(), lease.token, b"r").unwrap();
    assert!(!is_leased(&api, seq));
    assert!(!in_dedup(&api, seq));
    assert!(!pending_exists(&api, seq));
}

#[tokio::test]
async fn reaped_chunk_is_releasable() {
    // TTL 0: every lease is expired as soon as it is granted.
    let (api, _dir) = setup(
        "reap",
        Opts {
            lease_ttl_secs: 0,
            ..Default::default()
        },
    );
    let task_id = submit(api.clone(), b"payload", task_type()).unwrap();
    let first = lease_one(&api, "a").await;
    assert_eq!(first.task_ids(), vec![task_id.clone()]);

    LeasedTaskQueue::reap_expired(&api);
    assert!(!is_leased(&api, seq_of(&task_id)));
    // Requeued directly: still in flight, no refill needed.
    assert!(in_dedup(&api, seq_of(&task_id)));

    let second = lease_one(&api, "b").await;
    assert_eq!(second.task_ids(), vec![task_id.clone()]);
    assert_ne!(second.token, first.token);
    assert!(is_leased(&api, seq_of(&task_id)));
}

#[tokio::test]
async fn stale_worker_cannot_act_after_reap_and_release() {
    let (api, _dir) = setup(
        "stale",
        Opts {
            lease_ttl_secs: 0,
            ..Default::default()
        },
    );
    let task_id = submit(api.clone(), b"payload", task_type()).unwrap();
    let seq = seq_of(&task_id);
    let stale = lease_one(&api, "a").await;
    LeasedTaskQueue::reap_expired(&api);
    let current = lease_one(&api, "b").await;
    assert_eq!(current.task_ids(), vec![task_id.clone()]);

    // Worker A's lease was reaped; it must not touch B's lease.
    assert!(complete(api.clone(), task_type(), task_id.clone(), stale.token, b"a").is_err());
    assert!(cancel(api.clone(), task_type(), task_id.clone(), stale.token).is_err());
    assert!(renew(api.clone(), task_type(), &task_id, stale.token).is_err());
    assert!(is_leased(&api, seq));
    assert!(finished(&api, seq).is_none());

    complete(
        api.clone(),
        task_type(),
        task_id.clone(),
        current.token,
        b"b",
    )
    .unwrap();
    assert_eq!(
        finished(&api, seq).unwrap().results,
        vec![Some(b"b".to_vec())]
    );
}

#[tokio::test]
async fn complete_retry_is_idempotent() {
    let (api, _dir) = setup("idempotent", Opts::default());
    let task_id = submit(api.clone(), b"payload", task_type()).unwrap();
    let seq = seq_of(&task_id);
    let lease = lease_one(&api, "w").await;

    complete(
        api.clone(),
        task_type(),
        task_id.clone(),
        lease.token,
        b"result",
    )
    .unwrap();
    assert!(!pending_exists(&api, seq));

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
        finished(&api, seq).unwrap().results,
        vec![Some(b"result".to_vec())]
    );
    assert!(!in_dedup(&api, seq));
}

#[tokio::test]
async fn cancel_requeues_under_a_new_token() {
    let (api, _dir) = setup("cancel", Opts::default());
    let task_id = submit(api.clone(), b"payload", task_type()).unwrap();
    let first = lease_one(&api, "a").await;
    cancel(api.clone(), task_type(), task_id.clone(), first.token).unwrap();
    assert!(in_dedup(&api, seq_of(&task_id)));

    let second = lease_one(&api, "b").await;
    assert_eq!(second.task_ids(), vec![task_id.clone()]);
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
async fn batch_insert_packs_chunks() {
    let (api, _dir) = setup(
        "pack",
        Opts {
            chunk_size: Some(4),
            ..Default::default()
        },
    );
    let mut payloads: Vec<Vec<u8>> = (1..=10u8).map(|i| vec![i]).collect();
    payloads.insert(5, Vec::new()); // fails verification, takes no slot
    let results = submit_batch(api.clone(), &payloads, task_type()).unwrap();
    assert_eq!(results.len(), 11);
    assert_eq!(
        results[5].as_ref().unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );
    let ids: Vec<String> = results.into_iter().filter_map(Result::ok).collect();
    assert!(ids.iter().all(|id| id.len() == 24));
    assert!(
        ids.iter()
            .all(|id| TaskId::parse(id).unwrap().server_id == 3)
    );

    // 10 valid payloads -> chunks of 4, 4, 2 with consecutive seqs.
    let seqs: Vec<u64> = ids.iter().map(|id| seq_of(id)).collect();
    let base = seqs[0];
    let expected: Vec<(u64, u16)> = (0..10)
        .map(|i| (base + i as u64 / 4, (i % 4) as u16))
        .collect();
    let actual: Vec<(u64, u16)> = ids.iter().map(|id| (seq_of(id), idx_of(id))).collect();
    assert_eq!(actual, expected);
    for offset in 0..3 {
        assert!(pending_exists(&api, base + offset));
    }
    assert!(!pending_exists(&api, base + 3));

    // A lease is one whole chunk, payloads in index order.
    let lease = lease_one(&api, "w").await;
    assert_eq!(lease.chunk_seq, base);
    let payloads: Vec<Vec<u8>> = lease.tasks().map(|(_, bytes)| bytes.to_vec()).collect();
    assert_eq!(payloads, vec![vec![1], vec![2], vec![3], vec![4]]);
}

#[tokio::test]
async fn partial_complete_and_idempotent_partial_retry() {
    let (api, _dir) = setup(
        "partial",
        Opts {
            chunk_size: Some(4),
            ..Default::default()
        },
    );
    let payloads: Vec<Vec<u8>> = (1..=4u8).map(|i| vec![i]).collect();
    let ids = ids(submit_batch(api.clone(), &payloads, task_type()).unwrap());
    let seq = seq_of(&ids[0]);
    let lease = lease_one(&api, "w").await;
    assert_eq!(lease.len(), 4);

    // Complete two of four: results stored, chunk still pending and leased.
    let results = complete_ids(&api, &ids[..2], lease.token, b"r");
    assert!(results.iter().all(Result::is_ok));
    assert!(pending_exists(&api, seq));
    assert!(is_leased(&api, seq) && in_dedup(&api, seq));
    let stored = finished(&api, seq).unwrap();
    assert_eq!(stored.done().iter().collect::<Vec<_>>(), vec![0, 1]);

    // Retry of a done task while the lease lives: same bytes ok, others not.
    assert!(complete_ids(&api, &ids[..1], lease.token, b"r")[0].is_ok());
    let err = complete_ids(&api, &ids[..1], lease.token, b"x")[0]
        .clone()
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);

    // Get sees both states inside one chunk.
    let records = get_tasks(&api, &task_type(), ids.clone()).unwrap();
    let statuses: Vec<TaskStatus> = records.iter().map(|record| record.status).collect();
    assert_eq!(
        statuses,
        vec![
            TaskStatus::Finished,
            TaskStatus::Finished,
            TaskStatus::Leased,
            TaskStatus::Leased
        ]
    );

    // Finish the rest (mixed with a retry): the chunk leaves pending entirely.
    let results = complete_ids(&api, &ids[1..], lease.token, b"r");
    assert!(results.iter().all(Result::is_ok));
    assert!(!pending_exists(&api, seq));
    assert!(!is_leased(&api, seq) && !in_dedup(&api, seq));
    assert_eq!(finished(&api, seq).unwrap().done().count(), 4);

    // Retry after the lease is gone.
    assert!(
        complete_ids(&api, &ids, lease.token, b"r")
            .iter()
            .all(Result::is_ok)
    );
}

#[tokio::test]
async fn cancel_returns_only_open_indexes() {
    let (api, _dir) = setup(
        "cancel-open",
        Opts {
            chunk_size: Some(4),
            ..Default::default()
        },
    );
    let payloads: Vec<Vec<u8>> = (1..=4u8).map(|i| vec![i]).collect();
    let ids = ids(submit_batch(api.clone(), &payloads, task_type()).unwrap());
    let first = lease_one(&api, "a").await;
    assert!(
        complete_ids(&api, &ids[..2], first.token, b"r")
            .iter()
            .all(Result::is_ok)
    );

    // Any task id of the lease cancels the whole chunk lease, once.
    cancel(api.clone(), task_type(), ids[3].clone(), first.token).unwrap();
    let again = cancel(api.clone(), task_type(), ids[2].clone(), first.token).unwrap_err();
    assert_eq!(again.kind(), ErrorKind::NotFound);

    let second = lease_one(&api, "b").await;
    assert_eq!(second.task_ids(), ids[2..].to_vec());
    let payloads: Vec<Vec<u8>> = second.tasks().map(|(_, bytes)| bytes.to_vec()).collect();
    assert_eq!(payloads, vec![vec![3], vec![4]]);
    assert!(
        complete_ids(&api, &ids[2..], second.token, b"r")
            .iter()
            .all(Result::is_ok)
    );
    assert!(!pending_exists(&api, seq_of(&ids[0])));
}

#[tokio::test]
async fn reap_returns_only_open_indexes() {
    let (api, _dir) = setup(
        "reap-open",
        Opts {
            chunk_size: Some(4),
            lease_ttl_secs: 0,
            ..Default::default()
        },
    );
    let payloads: Vec<Vec<u8>> = (1..=4u8).map(|i| vec![i]).collect();
    let ids = ids(submit_batch(api.clone(), &payloads, task_type()).unwrap());
    let first = lease_one(&api, "a").await;
    assert!(complete_ids(&api, &ids[..1], first.token, b"r")[0].is_ok());

    LeasedTaskQueue::reap_expired(&api);
    let second = lease_one(&api, "b").await;
    assert_eq!(second.task_ids(), ids[1..].to_vec());

    // The reaped holder's retry of its completed task still succeeds; its
    // results for tasks now leased to someone else are fenced off.
    assert!(complete_ids(&api, &ids[..1], first.token, b"r")[0].is_ok());
    let err = complete_ids(&api, &ids[1..2], first.token, b"r")[0]
        .clone()
        .unwrap_err();
    assert_eq!(err.as_ref(), "LeaseTokenMismatch");
}

#[tokio::test]
async fn requeue_overflow_is_recovered_by_refill() {
    // One chunk of queue capacity: a cancel finds the queue full.
    let (api, _dir) = setup(
        "overflow",
        Opts {
            chunk_size: Some(2),
            queue_capacity: 2,
            ..Default::default()
        },
    );
    let payloads: Vec<Vec<u8>> = (1..=4u8).map(|i| vec![i]).collect();
    let ids = ids(submit_batch(api.clone(), &payloads, task_type()).unwrap());
    let (a, b) = (seq_of(&ids[0]), seq_of(&ids[2]));

    let first = lease_one(&api, "w").await;
    assert_eq!(first.chunk_seq, a);
    api.refill(&task_type()).unwrap(); // b fills the queue
    wait_queued(&api, 1).await;

    cancel(api.clone(), task_type(), ids[0].clone(), first.token).unwrap();
    assert!(!in_dedup(&api, a), "overflowed chunk is un-marked");

    let second = lease_one(&api, "w").await;
    assert_eq!(second.chunk_seq, b);
    // The refill drains the recovery set: a comes back without a rescan.
    let third = lease_one(&api, "w").await;
    assert_eq!(third.chunk_seq, a);
    assert_eq!(third.task_ids(), ids[..2].to_vec());
    assert!(try_lease(&api, &task_type(), "w").unwrap().is_none());
}

#[tokio::test]
async fn lease_many_rounds_up_to_whole_chunks() {
    let (api, _dir) = setup(
        "many",
        Opts {
            chunk_size: Some(4),
            ..Default::default()
        },
    );
    let payloads: Vec<Vec<u8>> = (1..=10u8).map(|i| vec![i]).collect();
    submit_batch(api.clone(), &payloads, task_type()).unwrap();
    let leases = lease_many(api.clone(), task_type(), "w".into(), 5, WAIT)
        .await
        .unwrap();
    // 5 tasks requested -> two whole chunks (8 tasks), never a split chunk.
    assert_eq!(
        leases.iter().map(Lease::len).collect::<Vec<_>>(),
        vec![4, 4]
    );
    let rest = lease_many(api.clone(), task_type(), "w".into(), 100, WAIT)
        .await
        .unwrap();
    assert_eq!(rest.iter().map(Lease::len).collect::<Vec<_>>(), vec![2]);
}

#[tokio::test]
async fn restart_recovers_pending_and_keeps_finished() {
    let dir = TempDir::new("restart");
    let opts = Opts {
        chunk_size: Some(4),
        ..Default::default()
    };
    let (done_ids, partial_ids, last_seq) = {
        let api = open(&dir, &opts);
        let payloads: Vec<Vec<u8>> = (1..=8u8).map(|i| vec![i]).collect();
        let ids = ids(submit_batch(api.clone(), &payloads, task_type()).unwrap());
        let a = lease_one(&api, "w").await;
        let b = lease_one(&api, "w").await;
        assert!(
            complete_ids(&api, &ids[..4], a.token, b"r")
                .iter()
                .all(Result::is_ok)
        );
        assert!(complete_ids(&api, &ids[4..5], b.token, b"r")[0].is_ok());
        let last_seq = seq_of(&ids[7]);
        close(api).await;
        (ids[..4].to_vec(), ids[4..].to_vec(), last_seq)
    };

    // Reopen the same store: leases are gone, the partially done chunk comes
    // back with only its open tasks, the finished one stays finished.
    let api = open(&dir, &opts);
    let lease = lease_one(&api, "w").await;
    assert_eq!(lease.task_ids(), partial_ids[1..].to_vec());
    assert!(try_lease(&api, &task_type(), "w").unwrap().is_none());
    let records = get_tasks(&api, &task_type(), done_ids.clone()).unwrap();
    assert!(records.iter().all(|r| r.status == TaskStatus::Finished));
    let records = get_tasks(&api, &task_type(), partial_ids.clone()).unwrap();
    let statuses: Vec<TaskStatus> = records.iter().map(|r| r.status).collect();
    assert_eq!(statuses[0], TaskStatus::Finished);
    assert!(statuses[1..].iter().all(|s| *s == TaskStatus::Leased));

    // chunk_seq resumes above everything handed out before the restart.
    let new_id = submit(api.clone(), b"new", task_type()).unwrap();
    assert!(seq_of(&new_id) > last_seq);
    let ids_before: BTreeSet<String> = done_ids.iter().chain(&partial_ids).cloned().collect();
    assert!(!ids_before.contains(&new_id));

    assert!(
        complete_ids(&api, &partial_ids[1..], lease.token, b"r")
            .iter()
            .all(Result::is_ok)
    );
    let records = get_tasks(&api, &task_type(), partial_ids).unwrap();
    assert!(records.iter().all(|r| r.status == TaskStatus::Finished));
}

#[tokio::test]
async fn get_reports_each_status() {
    let (api, _dir) = setup(
        "get",
        Opts {
            chunk_size: Some(1),
            ..Default::default()
        },
    );
    let ids = ids(submit_batch(api.clone(), &[b"a", b"b", b"c"], task_type()).unwrap());
    let leased = lease_one(&api, "w").await; // a
    let finished = lease_one(&api, "w").await; // b
    complete_ids(&api, &ids[1..2], finished.token, b"B")[0]
        .clone()
        .unwrap();
    assert_eq!(leased.task_ids(), ids[..1].to_vec());

    // Same chunk id with an index past its end, another server's id, garbage.
    let past_end = TaskId::new(3, seq_of(&ids[0]), 1).to_string();
    let other_server = TaskId::new(9, seq_of(&ids[0]), 0).to_string();
    let mut query = ids.clone();
    query.extend([past_end, other_server, "nope".to_string()]);
    let records = get_tasks(&api, &task_type(), query).unwrap();
    let got: Vec<(TaskStatus, Option<Vec<u8>>)> = records
        .into_iter()
        .map(|record| (record.status, record.bytes))
        .collect();
    assert_eq!(
        got,
        vec![
            (TaskStatus::Leased, Some(b"a".to_vec())),
            (TaskStatus::Finished, Some(b"B".to_vec())),
            (TaskStatus::Pending, Some(b"c".to_vec())),
            (TaskStatus::Unknown, None),
            (TaskStatus::Unknown, None),
            (TaskStatus::Unknown, None),
        ]
    );
}

#[tokio::test]
async fn scan_pages_over_tasks() {
    let (api, _dir) = setup(
        "scan",
        Opts {
            chunk_size: Some(3),
            ..Default::default()
        },
    );
    let payloads: Vec<Vec<u8>> = (1..=10u8).map(|i| vec![i]).collect();
    let ids = ids(submit_batch(api.clone(), &payloads, task_type()).unwrap());

    let scan_all = |scope: Scope, limit: usize| {
        let mut seen = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let (records, next) =
                scan_tasks(&api, &task_type(), scope, after.as_deref(), limit).unwrap();
            assert!(records.len() <= limit);
            seen.extend(records.into_iter().map(|r| (r.task_id, r.bytes)));
            match next {
                Some(next) => after = Some(next),
                None => return seen,
            }
        }
    };
    let pending = scan_all(Scope::Pending, 4);
    assert_eq!(
        pending.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>(),
        ids
    );
    assert_eq!(pending[9].1, vec![10]);

    // Complete the first chunk fully and one task of the second.
    let first = lease_one(&api, "w").await;
    let second = lease_one(&api, "w").await;
    assert!(
        complete_ids(&api, &ids[..3], first.token, b"r")
            .iter()
            .all(Result::is_ok)
    );
    assert!(complete_ids(&api, &ids[3..4], second.token, b"r")[0].is_ok());

    let finished = scan_all(Scope::Finished, 2);
    assert_eq!(
        finished
            .iter()
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>(),
        ids[..4].to_vec()
    );
    let pending = scan_all(Scope::Pending, 1);
    assert_eq!(
        pending.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>(),
        ids[4..].to_vec()
    );
}

#[tokio::test]
async fn chunk_size_precedence() {
    let dir = TempDir::new("precedence");
    let mut cfg = Config::default();
    cfg.config_toml.tasks.insert(
        "m:both".into(),
        TaskTypeConfig {
            chunk_size: Some(5),
            max_payload: Some(2),
            ..Default::default()
        },
    );
    let mut api = ServerAPI::open(&dir.0, cfg);
    let register = |api: &mut ServerAPI, name: &str, limits: TaskLimits| {
        let id: Identifier = ("m".into(), name.into());
        api.task_registry.register(
            Arc::new(TestTask {
                id: id.clone(),
                limits,
            }),
            id,
        );
    };
    let from_mod = TaskLimits {
        chunk_size: Some(8),
        max_payload: Some(100),
        max_result: Some(3),
    };
    register(&mut api, "both", from_mod);
    register(&mut api, "mod", from_mod);
    register(&mut api, "none", TaskLimits::default());
    let api = Arc::new(api);
    ServerAPI::populate(&api);

    let settings = |name: &str| api.settings(&("m".into(), name.into()));
    assert_eq!(
        settings("both"),
        TaskSettings {
            chunk_size: 5,
            max_payload: Some(2),
            max_result: Some(3),
        }
    );
    assert_eq!(settings("mod").chunk_size, 8);
    assert_eq!(settings("none").chunk_size, 1024);
    assert_eq!(settings("none").max_payload, None);

    // Limits are enforced: payload over `max_payload`, result over `max_result`.
    let both: Identifier = ("m".into(), "both".into());
    let results = submit_batch(api.clone(), &[&b"ok"[..], b"big"], both.clone()).unwrap();
    assert!(results[0].is_ok());
    assert_eq!(results[1].as_ref().unwrap_err().as_ref(), "PayloadTooLarge");
    let lease = lease_timeout(api.clone(), both.clone(), "w".into(), WAIT)
        .await
        .unwrap();
    let task_id = lease.task_ids().remove(0);
    let err = complete(
        api.clone(),
        both.clone(),
        task_id.clone(),
        lease.token,
        b"long",
    )
    .unwrap_err();
    assert_eq!(err.as_ref(), "ResultTooLarge");
    complete(api.clone(), both, task_id, lease.token, b"ok").unwrap();
}

#[tokio::test]
async fn refill_cursor_only_moves_forward() {
    // Capacity of one chunk so each refill loads exactly one.
    let (api, _dir) = setup(
        "cursor",
        Opts {
            chunk_size: Some(1),
            queue_capacity: 1,
            ..Default::default()
        },
    );
    let ids = ids(submit_batch(api.clone(), &[b"a", b"b"], task_type()).unwrap());
    let a = lease_one(&api, "w").await;
    assert_eq!(a.task_ids(), ids[..1].to_vec());
    api.refill(&task_type()).unwrap();
    wait_queued(&api, 1).await;
    let b = lease_one(&api, "w").await;
    assert_eq!(b.task_ids(), ids[1..].to_vec());

    // Store drained of loadable chunks: refills are no-ops until a submit.
    assert_eq!(api.refill(&task_type()).unwrap(), 0);
    assert_eq!(api.refill(&task_type()).unwrap(), 0);
    let c = submit(api.clone(), b"c", task_type()).unwrap();
    let lease = lease_one(&api, "w").await;
    assert_eq!(lease.task_ids(), vec![c]);

    for lease in [a, b, lease] {
        assert!(
            complete_ids(&api, &lease.task_ids(), lease.token, b"r")
                .iter()
                .all(Result::is_ok)
        );
    }
    assert!(api.task_queue.get(&task_type()).unwrap().dedup.is_empty());
    // Every pending record is gone; finished records stay.
    let pending = api
        .db
        .iterator_cf(api.cf_pending(), IteratorMode::Start)
        .filter_map(Result::ok)
        .filter(|(key, _)| key.starts_with(task_key_prefix(&task_type()).as_bytes()))
        .count();
    assert_eq!(pending, 0);
    assert!(
        api.db
            .get_cf(
                api.cf_finished(),
                finished_task_key(&task_type(), seq_of(&ids[0]))
            )
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn submit_wakes_parked_lease() {
    let (api, _dir) = setup("wake", Opts::default());
    let waiter = tokio::spawn(lease_timeout(api.clone(), task_type(), "w".into(), WAIT));
    tokio::time::sleep(Duration::from_millis(50)).await;
    let task_id = submit(api.clone(), b"payload", task_type()).unwrap();
    let lease = waiter.await.unwrap().unwrap();
    assert_eq!(lease.task_ids(), vec![task_id]);
}

#[tokio::test]
async fn lease_times_out_on_empty_queue() {
    let (api, _dir) = setup("timeout", Opts::default());
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
