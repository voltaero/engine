//! End-to-end test of the tcp ZeroMQ channel: submit → lease → complete over the
//! wire, plus a query to confirm the persisted record transitions.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use engine::client::Client;
use enginelib::Identifier;
use enginelib::api::{ServerAPI, finished_task_key, task_key};
use enginelib::nucleus::query::{Query, QueryResult};
use enginelib::task::{Task, Verifiable};

/// Minimal registered task type so `submit`/`complete` pass verification.
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

fn unique_tmp() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir()
        .join(format!("engine_e2e_{nanos}"))
        .to_string_lossy()
        .into_owned()
}

#[tokio::test]
async fn submit_lease_complete_over_tcp() {
    let task_type: Identifier = ("test".to_string(), "echo".to_string());
    let endpoint = "tcp://127.0.0.1:55571";

    // Build an isolated engine with the echo task registered.
    let api = ServerAPI::with_path(&unique_tmp());
    api.task_registry
        .tasks
        .insert(task_type.clone(), Arc::new(EchoTask));
    let api = Arc::new(api);
    ServerAPI::populate(&api);

    // Serve in the background.
    let serve_api = api.clone();
    tokio::spawn(async move {
        let _ = engine::server::serve(serve_api, endpoint).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let mut client = Client::connect(endpoint, String::new()).expect("connect");

    // Submit.
    let payload = b"hello".to_vec();
    let ids = client
        .submit(task_type.clone(), vec![payload.clone()])
        .await
        .expect("submit");
    assert_eq!(ids.len(), 1);
    let task_id = ids[0].clone();

    // Lease it back.
    let leased = client
        .lease(task_type.clone(), "worker".to_string(), 4)
        .await
        .expect("lease");
    assert_eq!(leased.len(), 1);
    assert_eq!(leased[0].task_id, task_id);
    assert_eq!(leased[0].bytes, payload);

    // Complete it.
    let ok = client
        .complete(task_type.clone(), vec![(task_id.clone(), b"done".to_vec())])
        .await
        .expect("complete");
    assert_eq!(ok, 1);

    // Pending record gone, finished record present.
    let pending = client
        .query(Query::Get {
            key: task_key(&task_type, &task_id),
        })
        .await
        .expect("query pending");
    assert!(matches!(pending, QueryResult::Record(None)));

    let finished = client
        .query(Query::Get {
            key: finished_task_key(&task_type, &task_id),
        })
        .await
        .expect("query finished");
    match finished {
        QueryResult::Record(Some(rec)) => assert_eq!(rec.value, b"done".to_vec()),
        other => panic!("expected finished record, got {other:?}"),
    }
}
