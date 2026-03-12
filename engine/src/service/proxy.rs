use std::{
    cmp::min,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use crate::{
    copy_metadata, get_auth,
    proto::{self, cluster_server::Cluster, engine_client::EngineClient, engine_server::Engine},
    routing::{NodeState, ProxyState, now_unix},
    service::backend::parse_owner_node,
};
use rand::{seq::SliceRandom, thread_rng};
use tokio::{task::JoinHandle, time::sleep};
use tonic::{Code, Request, Response, Status};

pub struct ProxyService {
    state: Arc<ProxyState>,
}

impl ProxyService {
    pub fn new(state: Arc<ProxyState>) -> Self {
        Self { state }
    }
}

pub fn spawn_reaper(state: Arc<ProxyState>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(1)).await;
            state.reap_stale_nodes(now_unix()).await;
        }
    })
}

struct InFlightCreateGuard {
    node: Arc<NodeState>,
}

impl InFlightCreateGuard {
    fn new(node: Arc<NodeState>) -> Self {
        node.in_flight_create.fetch_add(1, Ordering::SeqCst);
        Self { node }
    }
}

impl Drop for InFlightCreateGuard {
    fn drop(&mut self) {
        self.node.in_flight_create.fetch_sub(1, Ordering::SeqCst);
    }
}

fn require_cluster_auth<T>(request: &Request<T>, cluster_token: &str) -> Result<(), Status> {
    let expected = format!("Bearer {cluster_token}");
    if get_auth(request) != expected {
        return Err(Status::permission_denied("Invalid cluster authorization"));
    }
    Ok(())
}

fn parse_task_key(task_id: &str) -> Result<String, Status> {
    let Some((namespace, task)) = task_id.split_once(':') else {
        return Err(Status::invalid_argument(
            "Invalid task ID format, expected 'namespace:task'",
        ));
    };
    if namespace.is_empty() || task.is_empty() {
        return Err(Status::invalid_argument(
            "Invalid task ID format, expected 'namespace:task'",
        ));
    }
    Ok(format!("{namespace}:{task}"))
}

fn build_request<T: Clone>(request: &Request<T>, message: T) -> Request<T> {
    let mut outbound = Request::new(message);
    copy_metadata(request.metadata(), outbound.metadata_mut());
    outbound
}

async fn forward_create(
    node: Arc<NodeState>,
    request: &Request<proto::Task>,
) -> Result<Response<proto::Task>, Status> {
    let mut client = EngineClient::new(node.channel.clone());
    client
        .create_task(build_request(request, request.get_ref().clone()))
        .await
}

async fn forward_acquire(
    node: Arc<NodeState>,
    request: &Request<proto::TaskRequest>,
) -> Result<Response<proto::Task>, Status> {
    let mut client = EngineClient::new(node.channel.clone());
    client
        .aquire_task(build_request(request, request.get_ref().clone()))
        .await
}

async fn forward_publish(
    node: Arc<NodeState>,
    request: &Request<proto::Task>,
) -> Result<Response<proto::Empty>, Status> {
    let mut client = EngineClient::new(node.channel.clone());
    client
        .publish_task(build_request(request, request.get_ref().clone()))
        .await
}

async fn forward_delete(
    node: Arc<NodeState>,
    request: &Request<proto::TaskSelector>,
) -> Result<Response<proto::Empty>, Status> {
    let mut client = EngineClient::new(node.channel.clone());
    client
        .delete_task(build_request(request, request.get_ref().clone()))
        .await
}

async fn forward_get_tasks(
    node: Arc<NodeState>,
    request: &Request<proto::TaskPageRequest>,
    page: u64,
    page_size: u32,
) -> Result<proto::TaskPage, Status> {
    let mut payload = request.get_ref().clone();
    payload.page = page;
    payload.page_size = page_size;
    let mut client = EngineClient::new(node.channel.clone());
    Ok(client
        .get_tasks(build_request(request, payload))
        .await?
        .into_inner())
}

async fn forward_task_registry_probe(
    node: Arc<NodeState>,
    request: &Request<proto::Empty>,
) -> Result<(), Status> {
    let mut client = EngineClient::new(node.channel.clone());
    client
        .aquire_task_reg(build_request(request, request.get_ref().clone()))
        .await?;
    Ok(())
}

async fn forward_check_auth(
    node: Arc<NodeState>,
    request: &Request<proto::Empty>,
) -> Result<Response<proto::Empty>, Status> {
    let mut client = EngineClient::new(node.channel.clone());
    client
        .check_auth(build_request(request, request.get_ref().clone()))
        .await
}

async fn forward_cgrpc(
    node: Arc<NodeState>,
    request: &Request<proto::Cgrpcmsg>,
) -> Result<Response<proto::Cgrpcmsg>, Status> {
    let mut client = EngineClient::new(node.channel.clone());
    client
        .cgrpc(build_request(request, request.get_ref().clone()))
        .await
}

async fn broadcast_publish(
    nodes: Vec<Arc<NodeState>>,
    request: &Request<proto::Task>,
) -> Result<Response<proto::Empty>, Status> {
    let mut last_not_found = None;
    for node in nodes {
        match forward_publish(node, request).await {
            Ok(response) => return Ok(response),
            Err(status) if status.code() == Code::NotFound => {
                last_not_found = Some(status);
            }
            Err(status) => return Err(status),
        }
    }

    Err(last_not_found.unwrap_or_else(|| Status::not_found("Task not found")))
}

async fn broadcast_delete(
    nodes: Vec<Arc<NodeState>>,
    request: &Request<proto::TaskSelector>,
) -> Result<Response<proto::Empty>, Status> {
    let mut last_not_found = None;
    for node in nodes {
        match forward_delete(node, request).await {
            Ok(response) => return Ok(response),
            Err(status) if status.code() == Code::NotFound => {
                last_not_found = Some(status);
            }
            Err(status) => return Err(status),
        }
    }

    Err(last_not_found.unwrap_or_else(|| Status::not_found("Task not found")))
}

fn unavailable() -> Status {
    Status::unavailable("No healthy backend nodes available")
}

fn select_create_candidate(mut candidates: Vec<Arc<NodeState>>) -> Option<Arc<NodeState>> {
    if candidates.is_empty() {
        return None;
    }
    let mut rng = thread_rng();
    candidates.shuffle(&mut rng);
    if candidates.len() == 1 {
        return candidates.into_iter().next();
    }

    let left = candidates[0].clone();
    let right = candidates[1].clone();
    let left_load = left.in_flight_create.load(Ordering::SeqCst);
    let right_load = right.in_flight_create.load(Ordering::SeqCst);
    if left_load < right_load {
        Some(left)
    } else if right_load < left_load {
        Some(right)
    } else if left.node_id <= right.node_id {
        Some(left)
    } else {
        Some(right)
    }
}

#[tonic::async_trait]
impl Engine for ProxyService {
    async fn aquire_task(
        &self,
        request: Request<proto::TaskRequest>,
    ) -> Result<Response<proto::Task>, Status> {
        let task_key = parse_task_key(&request.get_ref().task_id)?;
        let mut candidates = self.state.candidate_nodes_for_task(&task_key).await;
        if candidates.is_empty() {
            return Err(unavailable());
        }

        candidates.shuffle(&mut thread_rng());
        let hops = min(
            self.state.config.max_acquire_hops as usize,
            candidates.len(),
        );
        for node in candidates.into_iter().take(hops) {
            match forward_acquire(node, &request).await {
                Ok(response) => return Ok(response),
                Err(status) if status.code() == Code::NotFound => continue,
                Err(status) => return Err(status),
            }
        }

        Err(Status::not_found("No queued tasks available"))
    }

    async fn aquire_task_reg(
        &self,
        request: Request<proto::Empty>,
    ) -> Result<Response<proto::TaskRegistry>, Status> {
        let nodes = self.state.healthy_nodes().await;
        let Some(probe_node) = nodes.first().cloned() else {
            return Err(unavailable());
        };
        forward_task_registry_probe(probe_node, &request).await?;

        let mut tasks: Vec<String> = nodes
            .iter()
            .flat_map(|node| node.tasks.iter().cloned())
            .collect();
        tasks.sort();
        tasks.dedup();
        Ok(Response::new(proto::TaskRegistry { tasks }))
    }

    async fn publish_task(
        &self,
        request: Request<proto::Task>,
    ) -> Result<Response<proto::Empty>, Status> {
        let task_key = parse_task_key(&request.get_ref().task_id)?;
        if let Some((owner_node, _)) = parse_owner_node(&request.get_ref().id) {
            if let Some(node) = self.state.node_by_id(owner_node).await {
                return forward_publish(node, &request).await;
            }
        }

        let candidates = self.state.candidate_nodes_for_task(&task_key).await;
        if candidates.is_empty() {
            return Err(unavailable());
        }
        broadcast_publish(candidates, &request).await
    }

    async fn cgrpc(
        &self,
        request: Request<proto::Cgrpcmsg>,
    ) -> Result<Response<proto::Cgrpcmsg>, Status> {
        let Some(node) = self
            .state
            .preferred_nodes_by_tag("control")
            .await
            .into_iter()
            .next()
        else {
            return Err(unavailable());
        };
        forward_cgrpc(node, &request).await
    }

    async fn create_task(
        &self,
        request: Request<proto::Task>,
    ) -> Result<Response<proto::Task>, Status> {
        let task_key = parse_task_key(&request.get_ref().task_id)?;
        let candidates = self.state.candidate_nodes_for_task(&task_key).await;
        let Some(node) = select_create_candidate(candidates) else {
            return Err(unavailable());
        };

        let _guard = InFlightCreateGuard::new(node.clone());
        forward_create(node, &request).await
    }

    async fn delete_task(
        &self,
        request: Request<proto::TaskSelector>,
    ) -> Result<Response<proto::Empty>, Status> {
        let task_key = format!("{}:{}", request.get_ref().namespace, request.get_ref().task);
        if let Some((owner_node, _)) = parse_owner_node(&request.get_ref().id) {
            if let Some(node) = self.state.node_by_id(owner_node).await {
                return forward_delete(node, &request).await;
            }
        }

        let candidates = self.state.candidate_nodes_for_task(&task_key).await;
        if candidates.is_empty() {
            return Err(unavailable());
        }
        broadcast_delete(candidates, &request).await
    }

    async fn get_tasks(
        &self,
        request: Request<proto::TaskPageRequest>,
    ) -> Result<Response<proto::TaskPage>, Status> {
        let task_key = format!("{}:{}", request.get_ref().namespace, request.get_ref().task);
        let candidates = self.state.candidate_nodes_for_task(&task_key).await;
        if candidates.is_empty() {
            return Err(unavailable());
        }

        let requested_end = request
            .get_ref()
            .page
            .saturating_add(1)
            .saturating_mul(request.get_ref().page_size as u64);
        let fanout_limit = min(requested_end, self.state.config.admin_fanout_limit as u64) as u32;

        let mut tasks = Vec::new();
        let mut join_set = tokio::task::JoinSet::new();
        for node in candidates {
            let outbound_request = build_request(&request, request.get_ref().clone());
            join_set.spawn(async move {
                forward_get_tasks(node, &outbound_request, 0, fanout_limit).await
            });
        }

        while let Some(joined) = join_set.join_next().await {
            match joined {
                Ok(Ok(page)) => tasks.extend(page.tasks),
                Ok(Err(status)) => return Err(status),
                Err(err) => {
                    return Err(Status::internal(format!(
                        "proxy get_tasks join error: {err}"
                    )));
                }
            }
        }

        tasks.sort_by(|left, right| left.id.cmp(&right.id));
        let start = request
            .get_ref()
            .page
            .saturating_mul(request.get_ref().page_size as u64) as usize;
        let end = start.saturating_add(request.get_ref().page_size as usize);
        let page_tasks = tasks
            .into_iter()
            .skip(start)
            .take(end.saturating_sub(start))
            .collect();

        Ok(Response::new(proto::TaskPage {
            namespace: request.get_ref().namespace.clone(),
            task: request.get_ref().task.clone(),
            page: request.get_ref().page,
            page_size: request.get_ref().page_size,
            state: request.get_ref().state,
            tasks: page_tasks,
        }))
    }

    async fn check_auth(
        &self,
        request: Request<proto::Empty>,
    ) -> Result<Response<proto::Empty>, Status> {
        let Some(node) = self
            .state
            .preferred_nodes_by_tag("auth")
            .await
            .into_iter()
            .next()
        else {
            return Err(unavailable());
        };
        forward_check_auth(node, &request).await
    }
}

#[tonic::async_trait]
impl Cluster for ProxyService {
    async fn register_node(
        &self,
        request: Request<proto::NodeRegisterRequest>,
    ) -> Result<Response<proto::NodeRegisterResponse>, Status> {
        require_cluster_auth(&request, &self.state.config.cluster_token)?;

        let node = request.get_ref();
        if node.node_id.trim().is_empty() {
            return Err(Status::invalid_argument("node_id must not be empty"));
        }
        if node.node_id.contains('@') {
            return Err(Status::invalid_argument("node_id must not contain '@'"));
        }

        let session_id = druid::Druid::default().to_hex();
        let state = NodeState::new(
            node.node_id.clone(),
            node.advertise_addr.clone(),
            node.tags.clone(),
            node.tasks.clone(),
            session_id.clone(),
            now_unix(),
        )
        .map_err(Status::invalid_argument)?;
        self.state.upsert_node(state).await;

        Ok(Response::new(proto::NodeRegisterResponse {
            session_id,
            heartbeat_interval_seconds: self.state.config.heartbeat_interval_seconds,
            ttl_seconds: self.state.config.node_ttl_seconds,
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<proto::NodeHeartbeat>,
    ) -> Result<Response<proto::NodeHeartbeatAck>, Status> {
        require_cluster_auth(&request, &self.state.config.cluster_token)?;

        let heartbeat = request.get_ref();
        let Some(node) = self.state.node_by_id(&heartbeat.node_id).await else {
            return Err(Status::not_found("Unknown node"));
        };
        if node.session_id != heartbeat.session_id {
            return Err(Status::permission_denied("Invalid cluster session"));
        }

        node.last_seen_unix.store(now_unix(), Ordering::Relaxed);
        Ok(Response::new(proto::NodeHeartbeatAck {
            server_time_unix: now_unix(),
        }))
    }

    async fn list_nodes(
        &self,
        request: Request<proto::Empty>,
    ) -> Result<Response<proto::NodeList>, Status> {
        require_cluster_auth(&request, &self.state.config.cluster_token)?;

        let nodes = self
            .state
            .healthy_nodes()
            .await
            .into_iter()
            .map(|node| proto::NodeInfo {
                node_id: node.node_id.clone(),
                advertise_addr: node.advertise_addr.clone(),
                tags: node.tags.clone(),
                tasks: node.tasks.clone(),
                last_seen_unix: node.last_seen_unix(),
                healthy: true,
            })
            .collect();
        Ok(Response::new(proto::NodeList { nodes }))
    }
}
