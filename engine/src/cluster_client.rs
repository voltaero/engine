use std::time::Duration;

use enginelib::api::EngineAPI;
use tokio::{task::JoinHandle, time::sleep};
use tonic::{Request, transport::Endpoint};
use tracing::{error, info, warn};

use crate::proto::{self, cluster_client::ClusterClient};

#[derive(Debug, Clone)]
pub struct NodeRegistration {
    pub node_id: String,
    pub advertise_addr: String,
    pub cluster_proxy_addr: String,
    pub cluster_token: String,
    pub node_tags: Vec<String>,
    pub tasks: Vec<String>,
}

pub fn registration_from_api(api: &EngineAPI) -> Result<Option<NodeRegistration>, String> {
    let cfg = &api.cfg.config_toml;
    let Some(cluster_proxy_addr) = cfg.cluster_proxy_addr.clone() else {
        return Ok(None);
    };

    let node_id = cfg
        .node_id
        .clone()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "clustered backend requires config.server.node_id".to_string())?;
    if node_id.contains('@') {
        return Err("clustered backend node_id must not contain '@'".into());
    }

    let cluster_token = cfg
        .cluster_token
        .clone()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "clustered backend requires config.server.cluster_token".to_string())?;

    let advertise_addr =
        normalize_advertise_addr(cfg.advertise_addr.as_deref(), cfg.host.as_str())?;

    validate_endpoint(&cluster_proxy_addr, "cluster_proxy_addr")?;
    validate_endpoint(&advertise_addr, "advertise_addr")?;

    let mut tasks: Vec<String> = api
        .task_registry
        .tasks
        .keys()
        .map(|(namespace, task)| format!("{namespace}:{task}"))
        .collect();
    tasks.sort();
    tasks.dedup();

    let mut node_tags = cfg.node_tags.clone().unwrap_or_default();
    node_tags.sort();
    node_tags.dedup();

    Ok(Some(NodeRegistration {
        node_id,
        advertise_addr,
        cluster_proxy_addr,
        cluster_token,
        node_tags,
        tasks,
    }))
}

pub fn normalize_advertise_addr(
    advertise_addr: Option<&str>,
    host: &str,
) -> Result<String, String> {
    let value = advertise_addr.unwrap_or(host).trim();
    if value.is_empty() {
        return Err("advertise address must not be empty".into());
    }

    if value.starts_with("http://") || value.starts_with("https://") {
        Ok(value.to_string())
    } else {
        Ok(format!("http://{value}"))
    }
}

pub fn spawn_registration(registration: NodeRegistration) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match ClusterClient::connect(registration.cluster_proxy_addr.clone()).await {
                Ok(mut client) => {
                    let register_response = match register_node(&mut client, &registration).await {
                        Ok(response) => response,
                        Err(err) => {
                            warn!(
                                "cluster registration failed for {}: {}",
                                registration.node_id, err
                            );
                            sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    };

                    info!(
                        "registered backend node {} with proxy {}",
                        registration.node_id, registration.cluster_proxy_addr
                    );

                    let session_id = register_response.session_id;
                    let interval_seconds = register_response.heartbeat_interval_seconds.max(1);

                    loop {
                        sleep(Duration::from_secs(interval_seconds)).await;
                        match heartbeat(&mut client, &registration, &session_id).await {
                            Ok(_) => {}
                            Err(err) => {
                                warn!(
                                    "cluster heartbeat failed for {}: {}",
                                    registration.node_id, err
                                );
                                break;
                            }
                        }
                    }
                }
                Err(err) => {
                    error!(
                        "failed to connect backend node {} to cluster proxy {}: {}",
                        registration.node_id, registration.cluster_proxy_addr, err
                    );
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
    })
}

async fn register_node(
    client: &mut ClusterClient<tonic::transport::Channel>,
    registration: &NodeRegistration,
) -> Result<proto::NodeRegisterResponse, tonic::Status> {
    let mut request = Request::new(proto::NodeRegisterRequest {
        node_id: registration.node_id.clone(),
        advertise_addr: registration.advertise_addr.clone(),
        tags: registration.node_tags.clone(),
        tasks: registration.tasks.clone(),
    });
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", registration.cluster_token)
            .parse()
            .map_err(|_| tonic::Status::internal("failed to encode cluster authorization"))?,
    );
    Ok(client.register_node(request).await?.into_inner())
}

async fn heartbeat(
    client: &mut ClusterClient<tonic::transport::Channel>,
    registration: &NodeRegistration,
    session_id: &str,
) -> Result<(), tonic::Status> {
    let mut request = Request::new(proto::NodeHeartbeat {
        node_id: registration.node_id.clone(),
        session_id: session_id.to_string(),
    });
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", registration.cluster_token)
            .parse()
            .map_err(|_| tonic::Status::internal("failed to encode cluster authorization"))?,
    );
    client.heartbeat(request).await?;
    Ok(())
}

fn validate_endpoint(value: &str, field_name: &str) -> Result<(), String> {
    Endpoint::from_shared(value.to_string())
        .map(|_| ())
        .map_err(|err| format!("invalid {field_name} '{value}': {err}"))
}
