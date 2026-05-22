use engine::{get_auth, get_uid};
use enginelib::plugin::LibraryMetadata;
use enginelib::{
    Identifier, RawIdentifier, Registry,
    api::EngineAPI,
    chrono::Utc,
    event::{debug, info, warn},
    events::{self, Events, ID},
    plugin::LibraryManager,
    task::{SolvedTasks, StoredExecutingTask, StoredTask, Task, TaskQueue},
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
    pub EngineAPI: Arc<RwLock<EngineAPI>>,
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
        let mut api = self.EngineAPI.write().await;
        let db = api.db.clone();
        let output = Events::CheckAdminAuth(&mut api, challenge, ("".into(), "".into()), db);
        if !output {
            warn!("Auth check failed - permission denied");
            return Err(tonic::Status::permission_denied("Invalid Auth"));
        };
        return Ok(tonic::Response::new(proto::Empty {}));
    }
    async fn delete_task(
        &self,
        request: tonic::Request<proto::TaskSelector>,
    ) -> Result<Response<proto::Empty>, Status> {
        let mut api = self.EngineAPI.write().await;
        let data = request.get_ref();
        let challenge = get_auth(&request);
        let db = api.db.clone();
        let id = ID(&data.namespace, &data.task);

        let output = Events::CheckAdminAuth(&mut api, challenge, ("".into(), "".into()), db);
        if !output {
            warn!("Auth check failed - permission denied");
            return Err(tonic::Status::permission_denied("Invalid Auth"));
        };
        // Generic helper for removing a task by id from a collection, using an id extractor closure
        fn delete_task_from_collection<T, F>(
            collection: &mut HashMap<(String, String), Vec<T>>,
            id: &(String, String),
            task_id: &str,
            state_name: &str,
            namespace: &str,
            task: &str,
            id_extractor: F,
        ) -> Result<(), Status>
        where
            F: Fn(&T) -> &str,
        {
            match collection.get_mut(id) {
                Some(query) => {
                    let orig_len = query.len();
                    query.retain(|f| id_extractor(f) != task_id);
                    if query.len() == orig_len {
                        info!(
                            "DeleteTask: Task with id {} not found in {} state for namespace: {}, task: {}",
                            task_id, state_name, namespace, task
                        );
                        return Err(Status::not_found(format!(
                            "Task with id {} not found in {} state",
                            task_id, state_name
                        )));
                    }
                    Ok(())
                }
                None => {
                    info!(
                        "DeleteTask: No tasks found in {} state for namespace: {}, task: {}",
                        state_name, namespace, task
                    );
                    Err(Status::not_found(format!(
                        "No tasks found in {} state for given namespace and task",
                        state_name
                    )))
                }
            }
        }

        // Use the helper for each state
        let result = match data.state() {
            TaskState::Processing => delete_task_from_collection(
                &mut api.executing_tasks.tasks,
                &id,
                &data.id,
                "Processing",
                &data.namespace,
                &data.task,
                |f| &f.id,
            ),
            TaskState::Solved => delete_task_from_collection(
                &mut api.solved_tasks.tasks,
                &id,
                &data.id,
                "Solved",
                &data.namespace,
                &data.task,
                |f| &f.id,
            ),
            TaskState::Queued => delete_task_from_collection(
                &mut api.task_queue.tasks,
                &id,
                &data.id,
                "Queued",
                &data.namespace,
                &data.task,
                |f| &f.id,
            ),
        };

        if let Err(e) = result {
            return Err(e);
        }

        // Sync running memory into DB
        EngineAPI::sync_db(&mut api);
        info!(
            "DeleteTask: Successfully deleted task with id {} in state {:?} for namespace: {}, task: {}",
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
        let mut api = self.EngineAPI.write().await;
        let challenge = get_auth(&request);

        let db = api.db.clone();
        if !Events::CheckAdminAuth(&mut api, challenge, ("".into(), "".into()), db) {
            info!("GetTask denied due to Invalid Auth");
            return Err(Status::permission_denied("Invalid authentication"));
        };
        let data = request.get_ref();

        let q: Vec<proto::Task> = match data.clone().state() {
            TaskState::Processing => {
                match api
                    .executing_tasks
                    .tasks
                    .get(&(data.namespace.clone(), data.task.clone()))
                {
                    Some(tasks) => {
                        let mut task_refs: Vec<_> = tasks.iter().collect();
                        task_refs.sort_by_key(|f| &f.id);
                        task_refs
                            .iter()
                            .map(|f| proto::Task {
                                id: f.id.clone(),
                                task_id: format!("{}:{}", data.namespace, data.task),
                                task_payload: f.bytes.clone(),
                                payload: Vec::new(),
                            })
                            .collect()
                    }
                    None => {
                        info!(
                            "Namespace {:?} and task {:?} not found in Processing state",
                            data.namespace, data.task
                        );
                        Vec::new()
                    }
                }
            }
            TaskState::Queued => {
                match api
                    .task_queue
                    .tasks
                    .get(&(data.namespace.clone(), data.task.clone()))
                {
                    Some(tasks) => {
                        let mut d = tasks.clone();
                        d.sort_by_key(|f| f.id.clone());
                        d.iter()
                            .map(|f| proto::Task {
                                id: f.id.clone(),
                                task_id: format!("{}:{}", data.namespace, data.task),
                                task_payload: f.bytes.clone(),
                                payload: Vec::new(),
                            })
                            .collect()
                    }
                    None => {
                        info!(
                            "Namespace {:?} and task {:?} not found in Queued state",
                            data.namespace, data.task
                        );
                        Vec::new()
                    }
                }
            }
            TaskState::Solved => {
                match api
                    .solved_tasks
                    .tasks
                    .get(&(data.namespace.clone(), data.task.clone()))
                {
                    Some(tasks) => {
                        let mut d = tasks.clone();
                        d.sort_by_key(|f| f.id.clone());
                        d.iter()
                            .map(|f| proto::Task {
                                id: f.id.clone(),
                                task_id: format!("{}:{}", data.namespace, data.task),
                                task_payload: f.bytes.clone(),
                                payload: Vec::new(),
                            })
                            .collect()
                    }
                    None => {
                        info!(
                            "Namespace {:?} and task {:?} not found in Solved state",
                            data.namespace, data.task
                        );
                        Vec::new()
                    }
                }
            }
        };
        let index = data.page * data.page_size as u64;
        let end = index + (api.cfg.config_toml.pagination_limit.min(data.page_size) as u64);
        let final_vec: Vec<_> = q
            .iter()
            .skip(index as usize)
            .take(data.page_size as usize)
            .cloned()
            .collect();
        return Ok(tonic::Response::new(proto::TaskPage {
            namespace: data.namespace.clone(),
            task: data.task.clone(),
            page: data.page,
            page_size: data.page_size,
            state: data.state,
            tasks: final_vec,
        }));
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
        let mut api = self.EngineAPI.write().await;
        let db = api.db.clone();

        debug!("Validating authentication for task registry request");
        if !Events::CheckAuth(&mut api, uid.clone(), challenge, db) {
            info!(
                "Task registry request denied - invalid authentication for user: {}",
                uid
            );
            return Err(Status::permission_denied("Invalid authentication"));
        };
        let mut tasks: Vec<RawIdentifier> = Vec::new();
        for (k, v) in &api.task_registry.tasks {
            let js: Vec<String> = vec![k.0.clone(), k.1.clone()];
            let jstr = js.join(":");
            tasks.push(jstr);
        }
        info!("Returning task registry with {} tasks", tasks.len());
        let response = proto::TaskRegistry { tasks };
        Ok(tonic::Response::new(response))
    }

    async fn aquire_task(
        &self,
        request: tonic::Request<proto::TaskRequest>,
    ) -> Result<tonic::Response<proto::Task>, tonic::Status> {
        let challenge = get_auth(&request);
        let task_id = request.get_ref().task_id.clone();
        let uid = get_uid(&request);
        info!(
            "Task acquisition request received from user: {} for task: {}",
            uid, task_id
        );

        {
            let mut api = self.EngineAPI.write().await;
            let db = api.db.clone();
            debug!("Validating authentication for task acquisition");
            if !Events::CheckAuth(&mut api, uid.clone(), challenge, db) {
                info!(
                    "Task acquisition denied - invalid authentication for user: {}",
                    uid
                );
                return Err(Status::permission_denied("Invalid authentication"));
            };
        }

        let (namespace, task_name) = task_id.split_once(':').ok_or_else(|| {
            info!("Invalid task ID format: {}", task_id);
            Status::invalid_argument("Invalid task ID format, expected 'namespace:task'")
        })?;

        debug!("Looking up task definition for {}:{}", namespace, task_name);
        let key = ID(namespace, task_name);

        {
            let api = self.EngineAPI.read().await;
            if api.task_registry.get(&key).is_none() {
                warn!(
                    "Task acquisition failed - task does not exist: {}:{}",
                    namespace, task_name
                );
                return Err(Status::invalid_argument("Task Does not Exist"));
            }
            if Events::ServerBeforeTaskAcquire(&api, uid.clone(), task_id.clone()) {
                info!(
                    "ServerBeforeTaskAcquire cancelled for user: {} task: {}",
                    uid, task_id
                );
                return Err(Status::aborted(
                    "Task acquire cancelled by server event handler",
                ));
            }
        }

        let (ttask, tasks_key_state, exec_key_state, db) = {
            let mut api = self.EngineAPI.write().await;

            let queue = api
                .task_queue
                .tasks
                .get_mut(&key)
                .ok_or_else(|| Status::not_found("No queued tasks available"))?;

            if queue.is_empty() {
                info!("No queued tasks for {}:{}", namespace, task_name);
                return Err(Status::not_found("No queued tasks available"));
            }

            let ttask = queue.remove(0);
            let task_payload = ttask.bytes.clone();

            api.executing_tasks
                .tasks
                .entry(key.clone())
                .or_default()
                .push(enginelib::task::StoredExecutingTask {
                    bytes: task_payload,
                    user_id: uid.clone(),
                    given_at: Utc::now(),
                    id: ttask.id.clone(),
                });

            let tasks_key_state = api.task_queue.tasks.get(&key).cloned().unwrap_or_default();
            let exec_key_state = api
                .executing_tasks
                .tasks
                .get(&key)
                .cloned()
                .unwrap_or_default();
            let db = api.db.clone();
            (ttask, tasks_key_state, exec_key_state, db)
        };

        let tasks_op = EngineAPI::state_op_tasks(&key, &tasks_key_state)
            .map_err(|e| Status::internal(format!("Serialization error: {}", e)))?;
        let exec_op = EngineAPI::state_op_executing(&key, &exec_key_state)
            .map_err(|e| Status::internal(format!("Serialization error: {}", e)))?;

        EngineAPI::apply_batch_ops(&db, vec![tasks_op, exec_op])
            .map_err(|e| Status::internal(format!("DB insert error: {}", e)))?;

        {
            let api = self.EngineAPI.read().await;
            Events::ServerTaskAcquired(&api, uid.clone(), task_id.clone(), ttask.id.clone());
        }

        Ok(tonic::Response::new(proto::Task {
            id: ttask.id,
            task_id,
            task_payload: ttask.bytes,
            payload: Vec::new(),
        }))
    }
    async fn publish_task(
        &self,
        request: tonic::Request<proto::Task>,
    ) -> Result<tonic::Response<proto::Empty>, tonic::Status> {
        let challenge = get_auth(&request);
        let uid = get_uid(&request);

        let task_id = request.get_ref().task_id.clone();
        let instance_id = request.get_ref().id.clone();
        let payload_for_event = Arc::new(std::sync::RwLock::new(
            request.get_ref().task_payload.clone(),
        ));

        {
            let mut api = self.EngineAPI.write().await;
            let db = api.db.clone();
            if !Events::CheckAuth(&mut api, uid.clone(), challenge, db) {
                info!("Aquire Task denied due to Invalid Auth");
                return Err(Status::permission_denied("Invalid authentication"));
            };
        }

        {
            let api = self.EngineAPI.read().await;
            if Events::ServerBeforeTaskPublish(
                &api,
                uid.clone(),
                task_id.clone(),
                instance_id.clone(),
                payload_for_event.clone(),
            ) {
                info!(
                    "ServerBeforeTaskPublish cancelled for user: {} task: {}",
                    uid, task_id
                );
                return Err(Status::aborted(
                    "Task publish cancelled by server event handler",
                ));
            }
        }

        let publish_payload = payload_for_event
            .read()
            .map(|p| p.clone())
            .map_err(|_| Status::internal("Task publish payload lock poisoned"))?;

        let (namespace, task_name) = task_id
            .split_once(':')
            .ok_or_else(|| Status::invalid_argument("Invalid Params"))?;
        let key = ID(namespace, task_name);

        let reg_tsk = {
            let api = self.EngineAPI.read().await;
            if !api.task_registry.tasks.contains_key(&key) {
                warn!(
                    "Task acquisition failed - task does not exist: {}:{}",
                    namespace, task_name
                );
                return Err(Status::invalid_argument("Task Does not Exist"));
            }

            match api.task_registry.get(&key) {
                Some(r) => r,
                None => {
                    warn!("Task registry missing for {}:{}", namespace, task_name);
                    return Err(Status::invalid_argument("Task Does not Exist"));
                }
            }
        };

        if !reg_tsk.verify(publish_payload.clone()) {
            info!("Failed to parse task");
            return Err(Status::invalid_argument("Failed to parse given task bytes"));
        }

        let (solved_id, exec_key_state, solved_key_state, db) = {
            let mut api = self.EngineAPI.write().await;

            let solved_id = {
                let exec_tasks = api
                    .executing_tasks
                    .tasks
                    .get_mut(&key)
                    .ok_or_else(|| tonic::Status::not_found("Invalid taskid or userid"))?;

                let idx = exec_tasks
                    .iter()
                    .position(|f| f.id == instance_id && f.user_id == uid)
                    .ok_or_else(|| tonic::Status::not_found("Invalid taskid or userid"))?;

                exec_tasks.remove(idx).id
            };

            api.solved_tasks.tasks.entry(key.clone()).or_default().push(
                enginelib::task::StoredTask {
                    bytes: publish_payload.clone(),
                    id: solved_id.clone(),
                },
            );

            let exec_key_state = api
                .executing_tasks
                .tasks
                .get(&key)
                .cloned()
                .unwrap_or_default();
            let solved_key_state = api
                .solved_tasks
                .tasks
                .get(&key)
                .cloned()
                .unwrap_or_default();
            let db = api.db.clone();

            (solved_id, exec_key_state, solved_key_state, db)
        };

        let exec_op = EngineAPI::state_op_executing(&key, &exec_key_state)
            .map_err(|e| Status::internal(format!("Serialization error: {}", e)))?;
        let solved_op = EngineAPI::state_op_solved(&key, &solved_key_state)
            .map_err(|e| Status::internal(format!("Serialization error: {}", e)))?;

        EngineAPI::apply_batch_ops(&db, vec![exec_op, solved_op])
            .map_err(|e| Status::internal(format!("DB insert error: {}", e)))?;

        {
            let api = self.EngineAPI.read().await;
            Events::ServerTaskPublished(&api, uid.clone(), task_id.clone(), solved_id);
        }

        info!("Task published successfully: {} by user: {}", task_id, uid);
        Ok(tonic::Response::new(proto::Empty {}))
    }
    async fn create_task(
        &self,
        request: tonic::Request<proto::Task>,
    ) -> Result<tonic::Response<proto::Task>, tonic::Status> {
        let mut api = self.EngineAPI.write().await;
        let challenge = get_auth(&request);
        let uid = get_uid(&request);
        let db = api.db.clone();
        if !Events::CheckAuth(&mut api, uid, challenge, db) {
            //TODO: change to AdminSpecific Auth
            info!("Create Task denied due to Invalid Auth");
            return Err(Status::permission_denied("Invalid authentication"));
        };
        let task = request.get_ref();
        let task_id = task.task_id.clone();
        let payload_for_event = Arc::new(std::sync::RwLock::new(task.task_payload.clone()));
        if Events::ServerBeforeTaskCreate(&api, task_id.clone(), payload_for_event.clone()) {
            info!("ServerBeforeTaskCreate cancelled for task: {}", task_id);
            return Err(Status::aborted(
                "Task create cancelled by server event handler",
            ));
        }
        let task_payload = payload_for_event
            .read()
            .map(|p| p.clone())
            .map_err(|_| Status::internal("Task create payload lock poisoned"))?;

        let parts: Vec<&str> = task_id.splitn(2, ':').collect();
        if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
            return Err(Status::invalid_argument(
                "Invalid task ID format, expected 'namespace:task'",
            ));
        }
        let id: Identifier = (parts[0].to_string(), parts[1].to_string());
        let tsk_reg = api.task_registry.get(&id);
        if let Some(tsk_reg) = tsk_reg {
            if !tsk_reg.clone().verify(task_payload.clone()) {
                warn!("Failed to parse given task bytes");
                return Err(Status::invalid_argument("Failed to parse given task bytes"));
            }
            let tbp_tsk = StoredTask {
                bytes: task_payload.clone(),
                id: druid::Druid::default().to_hex(),
            };
            api.task_queue
                .tasks
                .entry(id.clone())
                .or_default()
                .push(tbp_tsk.clone());

            let tasks_key_state = api.task_queue.tasks.get(&id).cloned().unwrap_or_default();
            let task_op = EngineAPI::state_op_tasks(&id, &tasks_key_state)
                .map_err(|e| Status::internal(format!("Serialization error: {}", e)))?;
            if let Err(e) = EngineAPI::apply_batch_ops(&api.db, vec![task_op]) {
                return Err(Status::internal(format!("DB insert error: {}", e)));
            }
            Events::ServerTaskCreated(
                &api,
                task_id.clone(),
                tbp_tsk.id.clone(),
                Arc::new(std::sync::RwLock::new(tbp_tsk.bytes.clone())),
            );
            return Ok(tonic::Response::new(proto::Task {
                id: tbp_tsk.id.clone(),
                task_id: task_id.clone(),
                payload: Vec::new(),
                task_payload: tbp_tsk.bytes.clone(),
            }));
        }
        Err(tonic::Status::aborted("Error"))
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut api = EngineAPI::default();
    EngineAPI::init(&mut api);
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
    EngineAPI::init_chron(apii.clone());
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
