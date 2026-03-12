use std::{error::Error, sync::Arc, time::Duration};

#[path = "../../src/bin/server.rs"]
mod server_bin;

use engine::{
    cluster_client::{registration_from_api, spawn_registration},
    proto::{self, cluster_client::ClusterClient, engine_client::EngineClient},
    proxy_config::{ProxyConfigToml, RouteRuleToml},
    routing::ProxyState,
    service::proxy::{ProxyService, spawn_reaper},
};
use enginelib::{
    Registry,
    api::{EngineAPI, postcard},
    event::register_inventory_handlers,
    events::ID,
    task::{Task, Verifiable},
};
use serde::{Deserialize, Serialize};
use tokio::{
    net::TcpListener,
    sync::{RwLock, oneshot},
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, transport::Server};

pub type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestTask {
    pub value: u32,
    pub id: (String, String),
}

impl Verifiable for TestTask {
    fn verify(&self, bytes: Vec<u8>) -> bool {
        postcard::from_bytes::<TestTask>(&bytes).is_ok()
    }
}

impl Task for TestTask {
    fn get_id(&self) -> (String, String) {
        self.id.clone()
    }

    fn clone_box(&self) -> Box<dyn Task> {
        Box::new(self.clone())
    }

    fn to_bytes(&self) -> Vec<u8> {
        postcard::to_allocvec(self).unwrap()
    }

    fn from_bytes(&self, bytes: &[u8]) -> Box<dyn Task> {
        Box::new(postcard::from_bytes::<TestTask>(bytes).unwrap())
    }

    fn from_toml(&self, _d: String) -> Box<dyn Task> {
        Box::new(self.clone())
    }

    fn to_toml(&self) -> String {
        String::new()
    }
}

pub struct TestBackend {
    pub api: Arc<RwLock<EngineAPI>>,
    shutdown: Option<oneshot::Sender<()>>,
    server_task: JoinHandle<Result<(), tonic::transport::Error>>,
    registration_shutdown: Option<CancellationToken>,
    registration_task: Option<JoinHandle<()>>,
}

impl TestBackend {
    pub async fn shutdown(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(shutdown) = self.registration_shutdown.take() {
            shutdown.cancel();
        }
        if let Some(task) = self.registration_task.take() {
            let _ = task.await;
        }
        let _ = self.server_task.await;
    }
}

pub struct TestProxy {
    pub addr: String,
    shutdown: Option<oneshot::Sender<()>>,
    server_task: JoinHandle<Result<(), tonic::transport::Error>>,
    reaper_task: JoinHandle<()>,
}

impl TestProxy {
    pub async fn shutdown(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.reaper_task.abort();
        let _ = self.server_task.await;
    }
}

pub async fn spawn_proxy() -> Result<TestProxy, BoxError> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let config = ProxyConfigToml {
        listen: addr.to_string(),
        cluster_token: "cluster-secret".into(),
        node_ttl_seconds: 2,
        heartbeat_interval_seconds: 1,
        max_acquire_hops: 3,
        admin_fanout_limit: 5000,
        rules: vec![
            RouteRuleToml {
                r#match: "ml:*".into(),
                require_tags: vec!["gpu".into()],
            },
            RouteRuleToml {
                r#match: "*:*".into(),
                require_tags: Vec::new(),
            },
        ],
    };
    let state = Arc::new(ProxyState::new(config)?);
    let reaper_task = spawn_reaper(state.clone());
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let cluster_service = ProxyService::new(state.clone());
    let engine_service = ProxyService::new(state);
    let server_task = tokio::spawn(async move {
        Server::builder()
            .add_service(proto::cluster_server::ClusterServer::new(cluster_service))
            .add_service(proto::engine_server::EngineServer::new(engine_service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    Ok(TestProxy {
        addr: format!("http://{addr}"),
        shutdown: Some(shutdown_tx),
        server_task,
        reaper_task,
    })
}

pub async fn spawn_backend(
    node_id: &str,
    tags: &[&str],
    proxy_addr: &str,
) -> Result<TestBackend, BoxError> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let mut api = EngineAPI::test_default();
    register_inventory_handlers(&mut api);
    api.cfg.config_toml.host = addr.to_string();
    api.cfg.config_toml.node_id = Some(node_id.to_string());
    api.cfg.config_toml.advertise_addr = Some(format!("http://{addr}"));
    api.cfg.config_toml.cluster_proxy_addr = Some(proxy_addr.to_string());
    api.cfg.config_toml.cluster_token = Some("cluster-secret".into());
    api.cfg.config_toml.node_tags = Some(tags.iter().map(|tag| (*tag).to_string()).collect());

    register_task(&mut api, "dist", "work");
    register_task(&mut api, "ml", "train");
    register_task(&mut api, "node", node_id);

    let api = Arc::new(RwLock::new(api));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let engine_service = server_bin::BackendEngineService::new(api.clone());
    let server_task = tokio::spawn(async move {
        Server::builder()
            .add_service(proto::engine_server::EngineServer::new(engine_service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let registration_shutdown = CancellationToken::new();
    let registration_task = {
        let api_guard = api.read().await;
        registration_from_api(&api_guard)?
    }
    .map(|registration| spawn_registration(registration, registration_shutdown.clone()));

    wait_for_node(proxy_addr, node_id).await?;

    Ok(TestBackend {
        api,
        shutdown: Some(shutdown_tx),
        server_task,
        registration_shutdown: Some(registration_shutdown),
        registration_task,
    })
}

pub fn task_bytes(value: u32, namespace: &str, task: &str) -> Vec<u8> {
    TestTask {
        value,
        id: ID(namespace, task),
    }
    .to_bytes()
}

pub async fn engine_client(
    addr: &str,
) -> Result<EngineClient<tonic::transport::Channel>, tonic::transport::Error> {
    EngineClient::connect(addr.to_string()).await
}

pub async fn cluster_client(
    addr: &str,
) -> Result<ClusterClient<tonic::transport::Channel>, tonic::transport::Error> {
    ClusterClient::connect(addr.to_string()).await
}

pub fn worker_request<T>(message: T) -> Result<Request<T>, BoxError> {
    let mut request = Request::new(message);
    request.metadata_mut().insert("uid", "worker-1".parse()?);
    request
        .metadata_mut()
        .insert("authorization", "worker-token".parse()?);
    Ok(request)
}

pub fn admin_request<T>(message: T) -> Result<Request<T>, BoxError> {
    let mut request = Request::new(message);
    request
        .metadata_mut()
        .insert("authorization", "admin-token".parse()?);
    Ok(request)
}

pub fn cluster_request<T>(message: T) -> Result<Request<T>, BoxError> {
    let mut request = Request::new(message);
    request
        .metadata_mut()
        .insert("authorization", "Bearer cluster-secret".parse()?);
    Ok(request)
}

pub async fn wait_for_node(proxy_addr: &str, node_id: &str) -> Result<(), BoxError> {
    timeout(Duration::from_secs(5), async move {
        loop {
            let mut client = match cluster_client(proxy_addr).await {
                Ok(client) => client,
                Err(_) => {
                    sleep(Duration::from_millis(50)).await;
                    continue;
                }
            };
            let response = match client.list_nodes(cluster_request(proto::Empty {})?).await {
                Ok(response) => response,
                Err(_) => {
                    sleep(Duration::from_millis(50)).await;
                    continue;
                }
            };
            if response
                .get_ref()
                .nodes
                .iter()
                .any(|node| node.node_id == node_id)
            {
                return Ok::<(), BoxError>(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await??;
    Ok(())
}

pub async fn wait_for_node_count(proxy_addr: &str, expected: usize) -> Result<(), BoxError> {
    timeout(Duration::from_secs(5), async move {
        loop {
            let mut client = match cluster_client(proxy_addr).await {
                Ok(client) => client,
                Err(_) => {
                    sleep(Duration::from_millis(50)).await;
                    continue;
                }
            };
            let response = match client.list_nodes(cluster_request(proto::Empty {})?).await {
                Ok(response) => response,
                Err(_) => {
                    sleep(Duration::from_millis(50)).await;
                    continue;
                }
            };
            if response.get_ref().nodes.len() == expected {
                return Ok::<(), BoxError>(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await??;
    Ok(())
}

fn register_task(api: &mut EngineAPI, namespace: &str, task: &str) {
    let id = ID(namespace, task);
    api.task_registry.register(
        Arc::new(TestTask {
            value: 0,
            id: id.clone(),
        }),
        id.clone(),
    );
    api.task_queue.tasks.entry(id.clone()).or_default();
    api.executing_tasks.tasks.entry(id.clone()).or_default();
    api.solved_tasks.tasks.entry(id).or_default();
}
