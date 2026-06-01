use engine::{EngineService, proto};
use enginelib::{
    Identifier, Registry,
    api::ServerAPI,
    task::{StoredTask, StoredTaskBlock, Task, Verifiable},
};
use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{net::TcpListener, sync::RwLock};
use tonic::{Request, transport::Server};

const TEST_NS: &str = "stream_test";
const TEST_TASK: &str = "noop";

#[derive(Debug, Clone, Default)]
struct NoopTask;

impl Verifiable for NoopTask {
    fn verify(&self, _b: Vec<u8>) -> bool {
        true
    }
}

impl Task for NoopTask {
    fn get_id(&self) -> Identifier {
        (TEST_NS.to_string(), TEST_TASK.to_string())
    }

    fn clone_box(&self) -> Box<dyn Task> {
        Box::new(self.clone())
    }

    fn run_cpu(&mut self) {}

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

async fn pick_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

async fn build_client(port: u16) -> proto::engine_client::EngineClient<tonic::transport::Channel> {
    let url = format!("http://127.0.0.1:{port}");
    let deadline = Instant::now() + Duration::from_secs(5);

    loop {
        let endpoint = tonic::transport::Endpoint::try_from(url.clone())
            .unwrap()
            .tcp_nodelay(true);
        match endpoint.connect().await {
            Ok(channel) => return proto::engine_client::EngineClient::new(channel),
            Err(err) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let _ = err;
            }
            Err(err) => panic!("failed to connect to test server on {url}: {err}"),
        }
    }
}

async fn spawn_test_server(port: u16) -> Arc<RwLock<ServerAPI>> {
    let mut api = ServerAPI::test_default();
    let id = (TEST_NS.to_string(), TEST_TASK.to_string());
    api.task_registry.register(Arc::new(NoopTask), id.clone());
    api.ensure_task_channel(id);
    enginelib::event::register_inventory_handlers(&mut api);

    let api = Arc::new(RwLock::new(api));
    let engine = EngineService::new(api.clone());
    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port));

    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(engine.into_server())
            .serve(addr)
            .await;
    });

    api
}

#[tokio::test(flavor = "multi_thread")]
async fn acquire_and_publish_task_streams() {
    let port = pick_port().await;
    let api = spawn_test_server(port).await;
    let mut client = build_client(port).await;
    let task_id = format!("{TEST_NS}:{TEST_TASK}");

    let created = client
        .create_task_block(Request::new(proto::TaskBlock {
            tasks: (0..3)
                .map(|i| proto::Task {
                    id: String::new(),
                    task_id: task_id.clone(),
                    task_payload: vec![i],
                    payload: Vec::new(),
                })
                .collect(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(created.tasks.len(), 3);

    let mut stream = client
        .aquire_task_stream(Request::new(proto::TaskBlockRequest {
            task_id: task_id.clone(),
            block_size: 3,
        }))
        .await
        .unwrap()
        .into_inner();

    let mut acquired = Vec::new();
    while let Some(task) = stream.message().await.unwrap() {
        acquired.push(task);
    }
    assert_eq!(acquired.len(), 3);

    client
        .publish_task_stream(Request::new(tokio_stream::iter(acquired)))
        .await
        .unwrap();

    let key = (TEST_NS.to_string(), TEST_TASK.to_string());
    let api = api.read().await;
    assert_eq!(api.scan_solved(&key).count(), 3);
    assert_eq!(api.scan_queued(&key).count(), 0);
}

#[test]
fn refill_skips_active_tasks_between_recv_and_lease() {
    let mut api = ServerAPI::test_default();
    let key = (TEST_NS.to_string(), TEST_TASK.to_string());
    api.task_registry.register(Arc::new(NoopTask), key.clone());
    api.ensure_task_channel(key.clone());

    let stored = StoredTask {
        id: "active-task".to_string(),
        bytes: vec![1],
    };
    api.mark_active_id(&key, stored.id.clone());
    api.put_queued(&key, &stored).unwrap();

    let (receiver, sender) = {
        let channel = api.task_queue.tasks.get(&key).unwrap();
        (channel.0.clone(), channel.1.clone())
    };
    sender
        .try_send(StoredTaskBlock {
            tasks: vec![stored],
        })
        .unwrap();

    let received = receiver.try_recv().unwrap();
    assert_eq!(received.tasks.len(), 1);
    assert!(receiver.is_empty());

    ServerAPI::fill_queue(&api, key, 1);

    assert!(receiver.try_recv().is_err());
}
