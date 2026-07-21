use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use engine::client::Client;
use engine::transport::{TransportStats, serve_with_shutdown};
use enginelib::Identifier;
use enginelib::api::ServerAPI;
use enginelib::config::TransportConfig;
use enginelib::error::ErrorKind;
use enginelib::task::{Task, Verifiable};
use tokio::sync::watch;

#[derive(Debug, Clone)]
struct EchoTask;

impl Verifiable for EchoTask {
    fn verify(&self, _bytes: &[u8]) -> bool {
        true
    }
}

impl Task for EchoTask {
    fn get_id(&self) -> Identifier {
        ("test".into(), "echo".into())
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
    fn from_toml(&self, _data: String) -> Box<dyn Task> {
        Box::new(self.clone())
    }
    fn to_toml(&self) -> String {
        String::new()
    }
}

fn unique_tmp() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir()
        .join(format!("engine_transport_bounds_{nanos}"))
        .to_string_lossy()
        .into_owned()
}

fn test_config() -> TransportConfig {
    TransportConfig {
        max_active_requests: 2,
        max_active_long_polls: 1,
        max_active_mutations: 2,
        max_active_queries: 1,
        reply_queue_count: 4,
        reply_queue_bytes: 64 * 1024,
        max_wire_bytes: 32 * 1024,
        max_reply_bytes: 16 * 1024,
        max_batch_items: 2,
        max_batch_bytes: 8 * 1024,
        lease_long_poll_ms: 500,
        zmq_sndhwm: 8,
        zmq_rcvhwm: 8,
        zmq_linger_ms: 0,
        shutdown_timeout_ms: 1000,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn long_poll_bulkhead_and_request_limits_are_enforced() {
    let endpoint = "tcp://127.0.0.1:55572";
    let task_type: Identifier = ("test".into(), "echo".into());
    let api = ServerAPI::with_path(&unique_tmp());
    api.task_registry
        .tasks
        .insert(task_type.clone(), Arc::new(EchoTask));
    let api = Arc::new(api);
    ServerAPI::populate(&api);
    ServerAPI::spawn_loaders(&api);

    let config = test_config();
    let stats = Arc::new(TransportStats::default());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let server = tokio::spawn(serve_with_shutdown(
        api,
        endpoint,
        config.clone(),
        shutdown_rx,
        stats.clone(),
    ));
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut waiting_client = Client::connect(endpoint, String::new()).expect("connect waiter");
    let waiting_type = task_type.clone();
    let waiting = tokio::spawn(async move {
        waiting_client
            .lease(waiting_type, "worker-1".into(), 1)
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // The dedicated lease-poll pool is full, but mutation capacity remains.
    let mut rejected_client = Client::connect(endpoint, String::new()).expect("connect rejected");
    let err = tokio::time::timeout(
        Duration::from_secs(1),
        rejected_client.lease(task_type.clone(), "worker-2".into(), 1),
    )
    .await
    .expect("overload response timed out")
    .expect_err("second long poll should be rejected");
    assert_eq!(err.kind(), ErrorKind::Overloaded);

    let mut submitter = Client::connect(endpoint, String::new()).expect("connect submitter");
    let ids = submitter
        .submit(task_type.clone(), vec![b"payload".to_vec()])
        .await
        .expect("mutation should not be starved by long poll");
    assert_eq!(ids.len(), 1);

    let leased = tokio::time::timeout(Duration::from_secs(2), waiting)
        .await
        .expect("waiting lease timed out")
        .expect("waiting task panicked")
        .expect("waiting lease failed");
    assert_eq!(leased.len(), 1);
    assert_eq!(leased[0].task_id, ids[0]);

    // Batch validation happens before authentication or nucleus mutation.
    let err = submitter
        .submit(task_type, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
        .await
        .expect_err("oversized batch should be rejected");
    assert_eq!(err.kind(), ErrorKind::InvalidArgument);

    shutdown_tx.send(true).expect("request shutdown");
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("server did not shut down")
        .expect("server task panicked")
        .expect("server returned error");

    assert!(stats.overloaded() >= 1);
    assert!(stats.max_active() <= config.max_active_requests);
    assert!(stats.max_active_long_polls() <= config.max_active_long_polls);
    assert!(stats.max_reply_count() <= config.reply_queue_count);
    assert!(stats.max_reply_bytes() <= config.reply_queue_bytes);
    assert_eq!(stats.reply_count(), 0);
    assert_eq!(stats.reply_bytes(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_aborts_long_polls_at_the_drain_deadline_without_leaking_permits() {
    let endpoint = "tcp://127.0.0.1:55573";
    let task_type: Identifier = ("test".into(), "echo".into());
    let api = ServerAPI::with_path(&unique_tmp());
    api.task_registry
        .tasks
        .insert(task_type.clone(), Arc::new(EchoTask));
    let api = Arc::new(api);
    ServerAPI::populate(&api);
    ServerAPI::spawn_loaders(&api);

    let mut config = test_config();
    config.lease_long_poll_ms = 5000;
    config.shutdown_timeout_ms = 100;
    let stats = Arc::new(TransportStats::default());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let server = tokio::spawn(serve_with_shutdown(
        api,
        endpoint,
        config,
        shutdown_rx,
        stats.clone(),
    ));
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut client = Client::connect(endpoint, String::new()).expect("connect waiter");
    let waiter = tokio::spawn(async move { client.lease(task_type, "worker".into(), 1).await });

    let wait_deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while stats.max_active_long_polls() == 0 {
        assert!(
            tokio::time::Instant::now() < wait_deadline,
            "long poll was not admitted"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    shutdown_tx.send(true).expect("request shutdown");
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .expect("server exceeded shutdown deadline")
        .expect("server task panicked")
        .expect("server returned error");
    waiter.abort();

    let release_deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while stats.active() != 0 {
        assert!(
            tokio::time::Instant::now() < release_deadline,
            "admission permit leaked after shutdown"
        );
        tokio::task::yield_now().await;
    }
    assert_eq!(stats.reply_count(), 0);
    assert_eq!(stats.reply_bytes(), 0);
}
