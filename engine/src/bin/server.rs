use engine::{get_auth, get_uid};
use enginelib::plugin::LibraryMetadata;
use enginelib::{
    Identifier, RawIdentifier, Registry,
    api::ServerAPI,
    chrono::Utc,
    event::{debug, info, warn},
    events::{self, Events, ID},
    plugin::LibraryManager,
    task::{LeasedTask, StoredTask, StoredTaskBlock, Task, TaskQueue},
};
use proto::{
    TaskState,
    engine_server::{Engine, EngineServer},
};
use std::{
    collections::HashMap,
    env::consts::OS,
    io::Read,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4},
    sync::{Arc, RwLock as RS_RwLock},
};
use tokio::sync::RwLock;
use tonic::{Request, Response, Status, metadata::MetadataValue, transport::Server};

use crate::proto::ModuleInfo;

mod proto {
    tonic::include_proto!("engine");
    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("engine_descriptor");
}
#[allow(non_snake_case)]
struct EngineService {
    pub EngineAPI: Arc<RwLock<ServerAPI>>,
}
#[tonic::async_trait]
impl Engine for EngineService {
    async fn get_metadata(
        &self,
        request: tonic::Request<proto::Empty>,
    ) -> Result<Response<proto::ServerMetadata>, Status> {
        let api = self.EngineAPI.read().await;

        let modules: Vec<ModuleInfo> = api
            .lib_manager
            .libraries
            .values()
            .map(|lib| lib.metadata.clone())
            //Dont show server mods
            .filter(|lib| !lib.mod_server)
            // vec<arc<librareymetadata>> --> vec<ModuleInfo>
            .map(|f| ModuleInfo {
                mod_id: f.mod_id.clone(),
                api_version: f.api_version.clone(),
                rustc_version: f.rustc_version.clone(),
                mod_version: f.mod_version.clone(),
            })
            .collect();

        let res = proto::ServerMetadata {
            engine_api: enginelib::GIT_VERSION.to_string(),
            mods: modules,
        };
        return Ok(Response::new(res));
    }

    async fn check_auth(
        &self,
        request: tonic::Request<proto::Empty>,
    ) -> Result<Response<proto::Empty>, Status> {
        let challenge = get_auth(&request);
        let api = self.EngineAPI.read().await;
        let db = api.db.clone();
        let output = Events::CheckAdminAuth(&api, challenge, ("".into(), "".into()), db);
        if !output {
            warn!("Auth check failed - permission denied");
            return Err(tonic::Status::permission_denied("Invalid Auth"));
        };
        return Ok(tonic::Response::new(proto::Empty {}));
    }
    async fn delete_task_block(
        &self,
        request: tonic::Request<proto::TaskSelector>,
    ) -> Result<Response<proto::Empty>, Status> {
        let api = self.EngineAPI.read().await;
        let data = request.get_ref();
        let challenge = get_auth(&request);
        let db = api.db.clone();
        let key = ID(&data.namespace, &data.task);

        if !Events::CheckAdminAuth(&api, challenge, ("".into(), "".into()), db) {
            warn!("Auth check failed - permission denied");
            return Err(tonic::Status::permission_denied("Invalid Auth"));
        };

        let removed = match data.state() {
            TaskState::Queued => api
                .delete_queued(&key, &data.id)
                .map_err(|e| Status::internal(format!("sled: {e}")))?,
            TaskState::Solved => api
                .delete_solved(&key, &data.id)
                .map_err(|e| Status::internal(format!("sled: {e}")))?,
            TaskState::Leased => {
                let mut leased = api.leased_tasks.tasks.entry(key.clone()).or_default();
                let orig = leased.len();
                leased.retain(|l| l.stored_task.id != data.id);
                orig != leased.len()
            }
        };

        if !removed {
            return Err(Status::not_found(format!(
                "Task {} not found in {:?} for {}:{}",
                data.id,
                data.state(),
                data.namespace,
                data.task,
            )));
        }

        info!(
            "DeleteTask: deleted {} in {:?} for {}:{}",
            data.id,
            data.state(),
            data.namespace,
            data.task
        );
        Ok(tonic::Response::new(proto::Empty {}))
    }
    /// Retrieves a paginated list of tasks filtered by namespace, task name, and state.
    ///
    /// Authenticates the request and, if authorized, returns tasks in the specified state
    /// (`Processing`, `Queued`, or `Solved`) for the given namespace and task name. The results
    /// are sorted by task ID and paginated according to the requested page and page size.
    ///
    /// Returns a `TaskPage` containing the filtered tasks and pagination metadata, or a
    /// permission denied error if authentication fails.
    ///
    /// # Examples
    ///
    /// ```
    /// // Example usage within a tonic gRPC client context:
    /// let request = proto::TaskPageRequest {
    ///     namespace: "example_ns".to_string(),
    ///     task: "example_task".to_string(),
    ///     state: proto::TaskState::Queued as i32,
    ///     page: 0,
    ///     page_size: 10,
    /// };
    /// let response = engine_client.get_tasks(request).await?;
    /// assert!(response.get_ref().tasks.len() <= 10);
    /// ```
    async fn get_tasks(
        &self,
        request: tonic::Request<proto::TaskPageRequest>,
    ) -> std::result::Result<tonic::Response<proto::TaskPage>, tonic::Status> {
        let api = self.EngineAPI.read().await;
        let challenge = get_auth(&request);

        let db = api.db.clone();
        if !Events::CheckAdminAuth(&api, challenge, ("".into(), "".into()), db) {
            info!("GetTask denied due to Invalid Auth");
            return Err(Status::permission_denied("Invalid authentication"));
        };
        let data = request.get_ref();
        let key = ID(&data.namespace, &data.task);
        let task_id_str = format!("{}:{}", data.namespace, data.task);

        let to_proto = |id: String, bytes: Vec<u8>| proto::Task {
            id,
            task_id: task_id_str.clone(),
            task_payload: bytes,
            payload: Vec::new(),
        };

        let mut tasks: Vec<proto::Task> = match data.state() {
            TaskState::Queued => {
                let mut v: Vec<_> = api
                    .scan_queued(&key)
                    .map(|t| to_proto(t.id, t.bytes))
                    .collect();
                v.sort_by(|a, b| a.id.cmp(&b.id));
                v
            }
            TaskState::Solved => {
                let mut v: Vec<_> = api
                    .scan_solved(&key)
                    .map(|t| to_proto(t.id, t.bytes))
                    .collect();
                v.sort_by(|a, b| a.id.cmp(&b.id));
                v
            }
            TaskState::Leased => {
                let mut v: Vec<_> = api
                    .leased_tasks
                    .tasks
                    .get(&key)
                    .map(|leased| {
                        leased
                            .iter()
                            .map(|l| to_proto(l.stored_task.id.clone(), l.stored_task.bytes.clone()))
                            .collect()
                    })
                    .unwrap_or_default();
                v.sort_by(|a, b| a.id.cmp(&b.id));
                v
            }
        };

        let page_size = api.cfg.config_toml.pagination_limit.min(data.page_size) as usize;
        let start = (data.page as usize).saturating_mul(page_size);
        let end = start.saturating_add(page_size).min(tasks.len());
        let final_vec = if start >= tasks.len() {
            Vec::new()
        } else {
            tasks.drain(start..end).collect()
        };

        Ok(tonic::Response::new(proto::TaskPage {
            namespace: data.namespace.clone(),
            task: data.task.clone(),
            page: data.page,
            page_size: page_size as u32,
            state: data.state,
            tasks: final_vec,
        }))
    }
    /// Handles custom gRPC messages with admin-level authentication.
    ///
    /// Processes a CGRPC request by verifying admin credentials and dispatching the event payload to the appropriate handler. Returns the processed event payload in the response. If authentication fails, returns a permission denied error.
    ///
    /// # Returns
    /// A `Cgrpcmsg` response containing the processed event payload, or a permission denied gRPC status on failed authentication.
    ///
    /// # Examples
    ///
    /// ```
    /// // Example usage within a gRPC client context:
    /// let request = proto::Cgrpcmsg {
    ///     handler_mod_id: "mod".to_string(),
    ///     handler_id: "handler".to_string(),
    ///     event_payload: vec![1, 2, 3],
    ///     // ... other fields ...
    /// };
    /// let response = engine_service.cgrpc(tonic::Request::new(request)).await?;
    /// assert_eq!(response.get_ref().handler_mod_id, "mod");
    /// ```
    async fn cgrpc(
        &self,
        request: tonic::Request<proto::Cgrpcmsg>,
    ) -> std::result::Result<tonic::Response<proto::Cgrpcmsg>, tonic::Status> {
        info!(
            "CGRPC request received for handler: {}:{}",
            request.get_ref().handler_mod_id,
            request.get_ref().handler_id
        );
        let mut api = self.EngineAPI.write().await;
        let challenge = get_auth(&request);
        let db = api.db.clone();
        debug!("Checking admin authentication for CGRPC request");
        let output = Events::CheckAdminAuth(
            &mut api,
            challenge,
            (
                request.get_ref().handler_mod_id.clone(),
                request.get_ref().handler_id.clone(),
            ),
            db,
        );
        if !output {
            warn!("CGRPC auth check failed - permission denied");
            return Err(tonic::Status::permission_denied("Invalid CGRPC Auth"));
        };
        let out = Arc::new(std::sync::RwLock::new(Vec::new()));
        debug!("Dispatching CGRPC event to handler");
        Events::CgrpcEvent(
            &mut api,
            ID("engine_core", "grpc"),
            request.get_ref().event_payload.clone(),
            out.clone(),
        );
        let mut res = request.get_ref().clone();
        res.event_payload = match out.read() {
            Ok(g) => g.clone(),
            Err(_) => {
                warn!("CGRPC response lock poisoned, returning empty payload");
                Vec::new()
            }
        };
        info!("CGRPC request processed successfully");
        return Ok(tonic::Response::new(res));
    }
    async fn aquire_task_reg(
        &self,
        request: tonic::Request<proto::Empty>,
    ) -> Result<tonic::Response<proto::TaskRegistry>, tonic::Status> {
        let uid = get_uid(&request);
        let challenge = get_auth(&request);
        info!("Task registry request received from user: {}", uid);
        let api = self.EngineAPI.read().await;
        let db = api.db.clone();

        debug!("Validating authentication for task registry request");
        if !Events::CheckAuth(&api, uid.clone(), challenge, db) {
            info!(
                "Task registry request denied - invalid authentication for user: {}",
                uid
            );
            return Err(Status::permission_denied("Invalid authentication"));
        };
        let mut tasks: Vec<RawIdentifier> = Vec::new();
        for entry in api.task_registry.tasks.iter() {
            let k = entry.key();
            tasks.push(format!("{}:{}", k.0, k.1));
        }
        info!("Returning task registry with {} tasks", tasks.len());
        let response = proto::TaskRegistry { tasks };
        Ok(tonic::Response::new(response))
    }

    async fn aquire_task_block(
        &self,
        request: tonic::Request<proto::TaskBlockRequest>,
    ) -> Result<tonic::Response<proto::TaskBlock>, tonic::Status> {
        let challenge = get_auth(&request);
        let task_id = request.get_ref().task_id.clone();
        let uid = get_uid(&request);
        info!(
            "Task acquisition request received from user: {} for task: {}",
            uid, task_id
        );

        {
            let api = self.EngineAPI.read().await;
            let db = api.db.clone();
            if !Events::CheckAuth(&api, uid.clone(), challenge, db) {
                info!(
                    "Task acquisition denied - invalid authentication for user: {}",
                    uid
                );
                return Err(Status::permission_denied("Invalid authentication"));
            };
        }

        let (namespace, task_name) = task_id.split_once(':').ok_or_else(|| {
            Status::invalid_argument("Invalid task ID format, expected 'namespace:task'")
        })?;
        let key = ID(namespace, task_name);

        {
            let api = self.EngineAPI.read().await;
            if api.task_registry.get(&key).is_none() {
                return Err(Status::invalid_argument("Task Does not Exist"));
            }
            if Events::ServerBeforeTaskAcquire(&api, uid.clone(), task_id.clone()) {
                return Err(Status::aborted(
                    "Task acquire cancelled by server event handler",
                ));
            }
        }

        // Resolve the receiver, refill from sled if drained, then await a block.
        // recv blocks until a producer (fill_queue or create_task_block) pushes one,
        // so callers should use a gRPC deadline if they need an upper bound.
        let receiver = {
            let api = self.EngineAPI.read().await;
            api.task_queue
                .tasks
                .get(&key)
                .map(|entry| entry.0.clone())
                .ok_or_else(|| Status::not_found("Unknown task type"))?
        };

        if receiver.is_empty() {
            let api = self.EngineAPI.read().await;
            ServerAPI::fill_queue(&api, key.clone());
        }

        let block = receiver
            .recv()
            .await
            .map_err(|_| Status::unavailable("Task queue closed"))?;

        // Lease the block and fire one per-block acquire event.
        let instance_ids: Vec<String> = {
            let api = self.EngineAPI.read().await;
            let mut entry = api.leased_tasks.tasks.entry(key.clone()).or_default();
            let now = Utc::now();
            let mut ids = Vec::with_capacity(block.tasks.len());
            for task in &block.tasks {
                ids.push(task.id.clone());
                entry.push(LeasedTask {
                    stored_task: Arc::new(task.clone()),
                    user_id: uid.clone(),
                    given_at: now,
                });
            }
            drop(entry);
            Events::ServerTaskBlockAcquired(&api, uid.clone(), task_id.clone(), ids.clone());
            ids
        };
        let _ = instance_ids;

        let tasks = block
            .tasks
            .into_iter()
            .map(|t| proto::Task {
                id: t.id,
                task_id: task_id.clone(),
                task_payload: t.bytes,
                payload: Vec::new(),
            })
            .collect();

        Ok(tonic::Response::new(proto::TaskBlock { tasks }))
    }
    async fn publish_task_block(
        &self,
        request: tonic::Request<proto::TaskBlock>,
    ) -> Result<tonic::Response<proto::Empty>, tonic::Status> {
        let challenge = get_auth(&request);
        let uid = get_uid(&request);
        let api = self.EngineAPI.read().await;

        {
            let db = api.db.clone();
            if !Events::CheckAuth(&api, uid.clone(), challenge, db) {
                return Err(Status::permission_denied("Invalid authentication"));
            };
        }

        // Group incoming tasks by their (namespace:task) — the proto allows mixing
        // but in practice they'll be uniform per call. Grouping defensively also
        // keeps registry lookups + event firing once-per-group.
        let mut groups: HashMap<Identifier, Vec<proto::Task>> = HashMap::new();
        for t in request.into_inner().tasks {
            let Some((ns, name)) = t.task_id.split_once(':') else {
                info!("publish: skipping malformed task_id {}", t.task_id);
                continue;
            };
            groups
                .entry((ns.to_string(), name.to_string()))
                .or_default()
                .push(t);
        }

        for (key, tasks) in groups {
            let task_id_str = format!("{}:{}", key.0, key.1);
            let Some(reg_tsk) = api.task_registry.get(&key) else {
                info!("publish: unknown task {}:{}, skipping group", key.0, key.1);
                continue;
            };

            let mut published_ids: Vec<String> = Vec::with_capacity(tasks.len());

            for t in tasks {
                let payload_for_event =
                    Arc::new(std::sync::RwLock::new(t.task_payload.clone()));
                if Events::ServerBeforeTaskPublish(
                    &api,
                    uid.clone(),
                    task_id_str.clone(),
                    t.id.clone(),
                    payload_for_event.clone(),
                ) {
                    info!("publish: handler cancelled {}:{}", task_id_str, t.id);
                    continue;
                }

                let payload = match payload_for_event.read() {
                    Ok(p) => p.clone(),
                    Err(_) => {
                        info!("publish: payload lock poisoned for {}", t.id);
                        continue;
                    }
                };

                if !reg_tsk.clone().verify(payload.clone()) {
                    info!("publish: verify failed for {}", t.id);
                    continue;
                }

                let mut leased = api.leased_tasks.tasks.entry(key.clone()).or_default();
                let Some(idx) = leased
                    .iter()
                    .position(|l| l.stored_task.id == t.id && l.user_id == uid)
                else {
                    info!("publish: no lease for {} held by {}", t.id, uid);
                    continue;
                };
                leased.remove(idx);
                drop(leased);

                let stored = StoredTask {
                    id: t.id.clone(),
                    bytes: payload,
                };
                if let Err(e) = api.put_solved(&key, &stored) {
                    info!("publish: sled put_solved failed for {}: {}", t.id, e);
                    continue;
                }
                published_ids.push(t.id);
            }

            if !published_ids.is_empty() {
                Events::ServerTaskBlockPublished(
                    &api,
                    uid.clone(),
                    task_id_str,
                    published_ids,
                );
            }
        }

        Ok(tonic::Response::new(proto::Empty {}))
    }
    async fn create_task_block(
        &self,
        request: tonic::Request<proto::TaskBlock>,
    ) -> Result<tonic::Response<proto::TaskBlock>, tonic::Status> {
        let challenge = get_auth(&request);
        let uid = get_uid(&request);
        let api = self.EngineAPI.read().await;
        let db = api.db.clone();
        if !Events::CheckAuth(&api, uid, challenge, db) {
            //TODO: change to AdminSpecific Auth
            info!("Create Task denied due to Invalid Auth");
            return Err(Status::permission_denied("Invalid authentication"));
        };

        // Group incoming tasks by (namespace:task).
        let mut groups: HashMap<Identifier, Vec<proto::Task>> = HashMap::new();
        for t in request.into_inner().tasks {
            let Some((ns, name)) = t.task_id.split_once(':') else {
                return Err(Status::invalid_argument(format!(
                    "Invalid task ID format: {}",
                    t.task_id
                )));
            };
            if ns.is_empty() || name.is_empty() {
                return Err(Status::invalid_argument(
                    "Invalid task ID format, expected 'namespace:task'",
                ));
            }
            groups
                .entry((ns.to_string(), name.to_string()))
                .or_default()
                .push(t);
        }

        let mut created: Vec<proto::Task> = Vec::new();

        for (key, tasks) in groups {
            let task_id_str = format!("{}:{}", key.0, key.1);
            let Some(reg_tsk) = api.task_registry.get(&key) else {
                return Err(Status::invalid_argument(format!(
                    "Task does not exist: {}",
                    task_id_str
                )));
            };

            let mut block_tasks: Vec<StoredTask> = Vec::with_capacity(tasks.len());
            let mut instance_ids: Vec<String> = Vec::with_capacity(tasks.len());
            let mut payloads: Vec<Arc<std::sync::RwLock<Vec<u8>>>> =
                Vec::with_capacity(tasks.len());

            for t in tasks {
                let payload_for_event =
                    Arc::new(std::sync::RwLock::new(t.task_payload.clone()));
                if Events::ServerBeforeTaskCreate(
                    &api,
                    task_id_str.clone(),
                    payload_for_event.clone(),
                ) {
                    info!("create: handler cancelled task in {}", task_id_str);
                    continue;
                }
                let payload = match payload_for_event.read() {
                    Ok(p) => p.clone(),
                    Err(_) => {
                        info!("create: payload lock poisoned in {}", task_id_str);
                        continue;
                    }
                };
                if !reg_tsk.clone().verify(payload.clone()) {
                    info!("create: verify failed in {}", task_id_str);
                    continue;
                }
                let stored = StoredTask {
                    id: druid::Druid::default().to_hex(),
                    bytes: payload,
                };
                if let Err(e) = api.put_queued(&key, &stored) {
                    info!("create: sled put_queued failed for {}: {}", stored.id, e);
                    continue;
                }
                instance_ids.push(stored.id.clone());
                payloads.push(payload_for_event);
                created.push(proto::Task {
                    id: stored.id.clone(),
                    task_id: task_id_str.clone(),
                    task_payload: stored.bytes.clone(),
                    payload: Vec::new(),
                });
                block_tasks.push(stored);
            }

            if !block_tasks.is_empty() {
                if let Some(channel) = api.task_queue.tasks.get(&key) {
                    let _ = channel.1.try_send(StoredTaskBlock { tasks: block_tasks });
                }
                Events::ServerTaskBlockCreated(&api, task_id_str, instance_ids, payloads);
            }
        }

        Ok(tonic::Response::new(proto::TaskBlock { tasks: created }))
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut api = ServerAPI::default();
    ServerAPI::init(&mut api);
    Events::init_auth(&mut api);
    Events::StartEvent(&mut api);
    Events::ServerStart(&api);
    let addr = api
        .cfg
        .config_toml
        .host
        .parse()
        .unwrap_or(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(127, 0, 0, 1),
            50051,
        )));
    let apii = Arc::new(RwLock::new(api));
    ServerAPI::init_chron(apii.clone());
    let engine = EngineService { EngineAPI: apii };

    // Build reflection service, mapping its concrete error into Box<dyn Error>
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .build_v1()
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

    // Start server and map transport errors into Box<dyn Error> so `?` works with our return type.
    Server::builder()
        .add_service(reflection_service)
        .add_service(EngineServer::new(engine))
        .serve(addr)
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

    Ok(())
}
